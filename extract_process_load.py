import os
from typing import List
from datetime import timedelta
import argparse
import pandas as pd
from dotenv import load_dotenv

from extract_data import Extractor
from load_data import DataLoader
from process_data import DataProcessor
from configs.api import SchemaConfigs


# Load environment variables
load_dotenv()
user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port")
dbname = os.getenv("dbname")


def get_date_range(source_date: str, window: int, shift: int = 0, freq: str = "7D") -> List[str]:
    # run in 7-day intervals to avoid performance issues
    source_date = pd.to_datetime(source_date)
    start_date = source_date - timedelta(days=window + shift)
    end_date = source_date - timedelta(days=shift)
    return list(pd.date_range(start_date, end_date, freq=freq).strftime("%Y-%m-%d"))


def process_and_load_campaign_ad_data(start_date: str, end_date: str, loader: DataLoader) -> None:
    print(f"Processing campaign-ad data: {start_date} - {end_date}")
    extractor = Extractor()
    data = extractor.get_data(period_from=start_date, period_to=end_date, lod="a")

    processor = DataProcessor(loader=loader)
    perf_data, metrics_data = processor.process_campaign_ad_data(data)

    loader.write_data(
        table_name="fact_campaign_ad_performance",
        data_rows=perf_data,
        column_names=SchemaConfigs.column_data["fact_campaign_ad_performance"],
        write_method="upsert",
        upsert_on=["campaign_id", "ad_id", "execution_date"],
    )

    loader.write_data(
        table_name="fact_campaign_ad_metrics",
        data_rows=metrics_data,
        column_names=SchemaConfigs.column_data["fact_campaign_ad_metrics"],
        write_method="upsert",
        upsert_on=["campaign_id", "ad_id", "execution_date", "lifeday"],
    )


def process_and_load_campaign_data(start_date: str, end_date: str, loader: DataLoader) -> None:
    print(f"Processing campaign data: {start_date} - {end_date}")
    extractor = Extractor()
    data = extractor.get_data(period_from=start_date, period_to=end_date, lod="c")

    processor = DataProcessor(loader=loader)
    perf_data, metrics_data = processor.process_campaign_data(data)

    loader.write_data(
        table_name="fact_campaign_performance",
        data_rows=perf_data,
        column_names=SchemaConfigs.column_data["fact_campaign_performance"],
        write_method="upsert",
        upsert_on=["campaign_id", "execution_date"],
    )

    loader.write_data(
        table_name="fact_campaign_metrics",
        data_rows=metrics_data,
        column_names=SchemaConfigs.column_data["fact_campaign_metrics"],
        write_method="upsert",
        upsert_on=["campaign_id", "execution_date", "lifeday"],
    )


def run_marketing_etl(source_date: str, window: int, shift: int = 0) -> None:
    loader = DataLoader(user=user, password=password, host=host, port=port, dbname=dbname)
    dates = get_date_range(source_date, window, shift)

    for i in range(1, len(dates)):
        start_date, end_date = dates[i - 1], dates[i]
        print(f"Running ETL for period: {start_date} to {end_date}")
        try:
            process_and_load_campaign_ad_data(start_date, end_date, loader)
            process_and_load_campaign_data(start_date, end_date, loader)
        except Exception as e:
            print(f"Error processing period {start_date} - {end_date}: {e}")
        print("-" * 120)


def main():
    parser = argparse.ArgumentParser(description="Run campaign ETL job")
    parser.add_argument("--source_date", type=str, default=pd.Timestamp.today().strftime("%Y-%m-%d"),
                        help="Reference date for ETL (default: today)")
    parser.add_argument("--window", type=int, default=365, help="Window size in days (default: 365)")
    parser.add_argument("--shift", type=int, default=0, help="Shift ETL window back by N days (default: 0)")
    args = parser.parse_args()

    run_marketing_etl(source_date=args.source_date, window=args.window, shift=args.shift)


if __name__ == "__main__":
    main()
