-- 1. Campaigns table (dimension)
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id BIGSERIAL PRIMARY KEY,
    campaign_name TEXT NOT NULL UNIQUE
);

-- 2. Ads table (dimension)
CREATE TABLE IF NOT EXISTS ads (
    ad_id BIGSERIAL PRIMARY KEY,
    ad_name TEXT NOT NULL UNIQUE
);

-- 3. Fact: fact_campaign_performance (campaign-level daily metrics)
CREATE TABLE IF NOT EXISTS fact_campaign_performance(
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT
        REFERENCES campaigns(campaign_id)
        ON DELETE SET NULL,
    execution_date DATE,
    spend NUMERIC,
    impressions BIGINT,
    clicks BIGINT,
    registrations BIGINT,
    ctr DOUBLE PRECISION,
    cr DOUBLE PRECISION,
    cpc DOUBLE PRECISION,
    processing_timestamp TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, execution_date)
);

-- 4. Fact: fact_campaign_ad_performance (campaign-ad-level daily metrics)
CREATE TABLE IF NOT EXISTS fact_campaign_ad_performance (
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT
        REFERENCES campaigns(campaign_id)
        ON DELETE SET NULL,
    ad_id BIGINT
        REFERENCES ads(ad_id)
        ON DELETE SET NULL,
    execution_date DATE,
    spend NUMERIC,
    impressions BIGINT,
    clicks BIGINT,
    registrations BIGINT,
    ctr DOUBLE PRECISION,
    cr DOUBLE PRECISION,
    cpc DOUBLE PRECISION,
    processing_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, ad_id, execution_date)
);

-- 5. Fact: fact_campaign_metrics (campaing-level lifeday metrics)
CREATE TABLE IF NOT EXISTS fact_campaign_metrics(
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT
        REFERENCES campaigns(campaign_id)
        ON DELETE SET NULL,
    execution_date DATE,
    lifeday SMALLINT CHECK (lifeday IN (1, 3, 7, 14)),
    players BIGINT,
    payers BIGINT,
    payments BIGINT,
    revenue NUMERIC,
    processing_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, execution_date, lifeday)
);

-- 6. Fact: fact_campaign_ad_metrics (campaign-ad-level lifeday metrics)
CREATE TABLE IF NOT EXISTS fact_campaign_ad_metrics (
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT
        REFERENCES campaigns(campaign_id)
        ON DELETE SET NULL,
    ad_id BIGINT
        REFERENCES ads(ad_id)
        ON DELETE SET NULL,
    execution_date DATE,
    lifeday SMALLINT CHECK (lifeday IN (1, 3, 7, 14)),
    players BIGINT,
    payers BIGINT,
    payments BIGINT,
    revenue NUMERIC,
    processing_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, ad_id, execution_date, lifeday)
);


CREATE OR REPLACE VIEW monthly_campaign_metrics AS(
WITH monthly_campaign_performance AS (
    SELECT
        campaign_id,
        DATE_TRUNC('month', execution_date)::DATE AS MONTH,
        SUM(
            CASE
                WHEN spend IS NULL THEN clicks * cpc
                ELSE spend
            END
        ) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(registrations) AS total_registrations
    FROM fact_campaign_performance
    GROUP BY 1,2
)

SELECT  c.campaign_name,
        mcp.month,
        ROUND(total_spend::numeric, 2) AS total_spend,
        ROUND(total_spend::numeric/total_registrations::numeric,2) AS cpi,
        ROUND(total_spend::numeric/l1.total_payers::bigint,2) AS cpp_d1,
        ROUND(total_spend::numeric/l3.total_payers::bigint,2) AS cpp_d3,
        ROUND(total_spend::numeric/l7.total_payers::bigint,2) AS cpp_d7,
        ROUND(total_spend::numeric/l14.total_payers::bigint,2) AS cpp_d14,

        ROUND(l1.total_players / l1.total_players,2) AS retention_d1,
        ROUND(l3.total_players / l1.total_players,2) AS retention_d3,
        ROUND(l7.total_players / l1.total_players,2) AS retention_d7,
        ROUND(l14.total_players / l1.total_players,2) AS retention_d14,

        ROUND(l1.total_revenue::numeric / mcp.total_spend::numeric,2) AS roas_d1,
        ROUND(l3.total_revenue::numeric / mcp.total_spend::numeric,2) AS roas_d3,
        ROUND(l7.total_revenue::numeric / mcp.total_spend::numeric,2) AS roas_d7,
        ROUND(l14.total_revenue::numeric / mcp.total_spend::numeric,2) AS roas_d14

FROM monthly_campaign_performance mcp

LEFT JOIN campaigns c ON c.campaign_id = mcp.campaign_id

LEFT JOIN(
  SELECT  campaign_id,
          DATE_TRUNC('month', execution_date)::DATE AS month,
          SUM(payers) AS total_payers,
          SUM(revenue) AS total_revenue,
          SUM(players) AS total_players
FROM fact_campaign_metrics
WHERE lifeday = 1
GROUP BY 1,2
) l1 ON l1.campaign_id = mcp.campaign_id AND l1.month = mcp.month

LEFT JOIN(
  SELECT  campaign_id,
          DATE_TRUNC('month', execution_date)::DATE AS month,
          SUM(payers) AS total_payers,
          SUM(revenue) AS total_revenue,
          SUM(players) AS total_players
FROM fact_campaign_metrics
WHERE lifeday = 3
GROUP BY 1,2
) l3 ON l3.campaign_id = mcp.campaign_id AND l3.month = mcp.month

LEFT JOIN(
  SELECT  campaign_id,
          DATE_TRUNC('month', execution_date)::DATE AS month,
          SUM(payers) AS total_payers,
          SUM(revenue) AS total_revenue,
          SUM(players) AS total_players
FROM fact_campaign_metrics
WHERE lifeday = 7
GROUP BY 1,2
) l7 ON l7.campaign_id = mcp.campaign_id AND l7.month = mcp.month

LEFT JOIN(
  SELECT  campaign_id,
          DATE_TRUNC('month', execution_date)::DATE AS month,
          SUM(payers) AS total_payers,
          SUM(revenue) AS total_revenue,
          SUM(players) AS total_players
FROM fact_campaign_metrics
WHERE lifeday = 14
GROUP BY 1,2
) l14 ON l14.campaign_id = mcp.campaign_id AND l14.month = mcp.month

ORDER BY 1,2 ASC
);