from typing import Optional, Dict, Union, List, Any
import json
import requests
from configs.api import APIConfigs as Config
import time

class Extractor:
    def __init__(self, api_key_file: str = "api_key.json") -> None:
        self.base_url = Config.base_url
        self.endpoint = Config.campaigns_endpoint
        self.api_key_file = api_key_file
        self._api_key = None
        self.lifedays = Config.lifeday_periods

    def get_api_key(self) -> None:
        """Reads the API key and assigns it to the `_api_key` attribute."""

        try:
            with open(self.api_key_file, "r") as f:
                key = json.load(f)
                self._api_key = key["x-api-key"]
        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to load API key: {e}")

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
                response = requests.get(url=url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor * (2 ** attempt)
                    print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}.")
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Request failed after {max_retries} attempts: {e}")
                    return []

    def get_data(self, period_from: str, period_to: str, lod: str = "a") -> List[Dict[str, Any]]:
        """ Fetches and aggregates data for the specified period and level of detail (lod) across all lifedays."""
        
        data = []
        # TODO switch to asynchronous for each lifeday
        for lifeday in self.lifedays:
            time.sleep(0.5)  # avoid throttling
            params = self.set_params(period_from, period_to, lifeday, lod)
            response = self._request(params)
            data.extend(response)
        return data
