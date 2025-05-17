from typing import List, Dict, Any, Tuple, Callable
from etl.load_data import DataLoader
from collections import defaultdict


class DataProcessor:
    def __init__(self, loader: DataLoader):
        self.loader = loader

    def get_campaign_ids(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Processes a list of records to extract unique campaign names, upserts them using the
        loader, and returns a dictionary mapping campaign names to their corresponding IDs.
        """

        campaigns_ids = defaultdict(int)
        campaigns = set(
            [
                record["campaign"]
                for record in data
                if "campaign" in record and record["campaign"]
            ]
        )
        for campaign in campaigns:
            campaigns_ids[campaign] = self.loader.upsert_campaign(campaign)
        return campaigns_ids

    def get_ad_ids(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Processes a list of records to extract unique ad names, upserts them
        using the loader, and returns a dictionary mapping each ad to its corresponding ID.
        """

        ad_ids = defaultdict(int)
        ads = set([record["ad"] for record in data if "ad" in record and record["ad"]])
        for ad in ads:
            ad_ids[ad] = self.loader.upsert_ad(ad)
        return ad_ids

    def process_campaign_ad_data(
        self, data: List[Dict[str, Any]]
    ) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
        """
        Processes campaign-ad-level data to extract performance and metrics information,
        returning two lists of tuples containing the processed data.
        """

        if not data:
            print(self.__class__.__name__, "-", self.process_campaign_ad_data.__name__)
            print("No data provided for processing.")
            return [], []

        campaign_ad_performance_data, campaign_ad_metrics_data = [], []
        campaign_ids = self.get_campaign_ids(data=data)
        ad_ids = self.get_ad_ids(data=data)

        for record in data:
            try:
                # campaign and ad fields are allowed to be NULL
                campaign_id = (
                    campaign_ids[record["campaign"]]
                    if record["campaign"] in campaign_ids
                    else None
                )
                ad_id = ad_ids[record["ad"]] if record["ad"] in ad_ids else None
                execution_date = record.get("date")

                campaign_ad_performance_record = {
                    "campaign_id": campaign_id,
                    "ad_id": ad_id,
                    "execution_date": execution_date,
                    "spend": record.get("cost"),
                    "impressions": record.get("impressions"),
                    "clicks": record.get("clicks"),
                    "registrations": record.get("registrations"),
                    "ctr": record.get("ctr"),
                    "cr": record.get("cr"),
                    "cpc": record.get("cpc"),
                }

                campaign_ad_tuple = tuple(
                    v for v in campaign_ad_performance_record.values()
                )
                campaign_ad_performance_data.append(campaign_ad_tuple)

                # list of jsons
                metrics = record.get("metrics", [{}])[0]

                campaign_ad_metrics_record = {
                    "campaign_id": campaign_id,
                    "ad_id": ad_id,
                    "execution_date": execution_date,
                    "lifeday": metrics.get("lifeday"),
                    "players": metrics.get("players"),
                    "payers": metrics.get("payers"),
                    "payments": metrics.get("payments"),
                    "revenue": metrics.get("revenue"),
                }
                campaign_ad_metrics_tuple = tuple(
                    v for v in campaign_ad_metrics_record.values()
                )
                campaign_ad_metrics_data.append(campaign_ad_metrics_tuple)
            except Exception as e:
                print(f"Error processing record: {record}. Error: {e}")
                # TODO add the erronous records to a dead letter sink

        return campaign_ad_performance_data, campaign_ad_metrics_data

    def process_campaign_data(
        self, data: List[Dict[str, Any]]
    ) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
        """
        Processes campaign-level data to extract performance and metrics information,
        returning two lists of tuples containing the processed data.
        """

        if not data:
            print(self.__class__.__name__, "-", self.process_campaign_data.__name__)
            print("No data provided for processing.")
            return [], []

        campaign_performance_data, campaign_performance_metrics = [], []
        campaign_ids = self.get_campaign_ids(data=data)

        for record in data:
            try:
                # campaign and ad fields are allowed to be NULL
                campaign_id = (
                    campaign_ids[record["campaign"]]
                    if record["campaign"] in campaign_ids
                    else None
                )
                execution_date = record.get("date")

                campaign_performance_record = {
                    "campaign_id": campaign_id,
                    "execution_date": execution_date,
                    "spend": record.get("cost"),
                    "impressions": record.get("impressions"),
                    "clicks": record.get("clicks"),
                    "registrations": record.get("registrations"),
                    "ctr": record.get("ctr"),
                    "cr": record.get("cr"),
                    "cpc": record.get("cpc"),
                }

                campaign_tuple = tuple(v for v in campaign_performance_record.values())
                campaign_performance_data.append(campaign_tuple)

                # list of jsons
                metrics = record.get("metrics", [{}])[0]

                campaign_metrics_record = {
                    "campaign_id": campaign_id,
                    "execution_date": execution_date,
                    "lifeday": metrics.get("lifeday"),
                    "players": metrics.get("players"),
                    "payers": metrics.get("payers"),
                    "payments": metrics.get("payments"),
                    "revenue": metrics.get("revenue"),
                }
                campaign_metrics_tuple = tuple(
                    v for v in campaign_metrics_record.values()
                )
                campaign_performance_metrics.append(campaign_metrics_tuple)
            except Exception as e:
                print(f"Error processing record: {record}. Error: {e}")
                # TODO add the erronous records to a dead letter sink

        return campaign_performance_data, campaign_performance_metrics
