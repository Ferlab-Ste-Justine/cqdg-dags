import requests
from typing import Any

def http_post(url: str, json: Any = None) -> requests.Response:
    with requests.post(url, json=json) as response:
        response.raise_for_status()
        return response