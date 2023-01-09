from typing import Tuple
import requests
import os


def download(param: Tuple[str, str]) -> str:
    url, target = param
    os.makedirs(os.path.dirname(target), exist_ok=True)
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/51.0.2704.103 Safari/537.36"})
    with open(target, 'wb') as handle:
        handle.write(response.content)
    return url
