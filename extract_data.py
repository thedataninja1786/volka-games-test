from typing import Optional, Dict, Union, List, Any
import json
import requests
from configs.api import APIConfigs as Config
import time
import boto3
import os
from dotenv import load_dotenv


load_dotenv()


def get_env_variable(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Environment variable '{var_name}' is not set or empty.")
    return value


class Extractor:
    def __init__(self) -> None:
        self.base_url = Config.base_url
        self.endpoint = Config.campaigns_endpoint
        self._api_key = None
        self.lifedays = Config.lifeday_periods

    def get_api_key(self) -> None:
        secret_name = get_env_variable("secret_name")
        aws_access_key_id = get_env_variable("aws_access_key_id")
        aws_secret_access_key = get_env_variable("aws_secret_access_key")
        region_name = get_env_variable("region_name")

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        client = session.client("secretsmanager")

        try:
            response = client.get_secret_value(SecretId=secret_name)
            secret_string = json.loads(response["SecretString"])
            self._api_key = secret_string["x-api-key"]

        except Exception as e:
            print(f"An error occurred: {e}")
            raise

    def get_headers(self) -> Dict[str, str]:
        if self._api_key is None:
            self.get_api_key()
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": self._api_key,
        }

    def get_url(self) -> str:
        """Constructs the full URL by combining the base URL and endpoint."""

        return f"{self.base_url}/{self.endpoint}"

    def set_params(
        self,
        period_from: str,
        period_to: str,
        lifedays: Optional[int],
        lod: str = "c",
    ) -> Dict[str, Union[str, int]]:
        """Sets the parameters of the request."""

        return {
            "lod": lod,
            "period_from": period_from,
            "period_to": period_to,
            "lifedays": lifedays,
        }

    def _request(self, params) -> List[Dict[str, Any]]:
        """Makes a GET request with retries and exponential backoff."""

        url = self.get_url()
        headers = self.get_headers()
        max_retries = 5
        backoff_factor = 1

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url=url, headers=headers, params=params, timeout=10
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor * (2**attempt)
                    print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}.")
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Request failed after {max_retries} attempts: {e}")
                    return []

    def get_data(
        self, period_from: str, period_to: str, lod: str = "a"
    ) -> List[Dict[str, Any]]:
        """Fetches and aggregates data for the specified period and level of detail (lod) across all lifedays."""

        data = []
        # TODO switch to asynchronous for each lifeday
        for lifeday in self.lifedays:
            time.sleep(0.5)  # avoid throttling
            params = self.set_params(period_from, period_to, lifeday, lod)
            response = self._request(params)
            data.extend(response)
        return data
