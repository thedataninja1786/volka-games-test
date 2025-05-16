import json


class APIConfigs:
    base_url = "https://api.sdetest.volka.team"
    lifeday_periods = [1,3,7,14]
    campaigns_endpoint = "campaigns-report"
    window = 7

class SchemaConfigs:
    # columns that are being filled from the API
    column_data = {
        "fact_campaign_ad_performance":[
        "campaign_id",
        "ad_id",
        "execution_date",
        "spend",
        "impressions",
        "clicks",
        "registrations",
        "ctr",
        "cr",
        "cpc"
    ],

    "fact_campaign_ad_metrics":[
        "campaign_id",
        "ad_id",
        "execution_date",
        "lifeday",
        "players",
        "payers",
        "payments",
        "revenue"
    ],

    "fact_campaign_performance":[
        "campaign_id",
        "execution_date",
        "spend",
        "impressions",
        "clicks",
        "registrations",
        "ctr",
        "cr",
        "cpc",
    ],

    "fact_campaign_metrics":[
        "campaign_id",
        "execution_date",
        "lifeday",
        "players",
        "payers",
        "payments",
        "revenue",
    ]
}