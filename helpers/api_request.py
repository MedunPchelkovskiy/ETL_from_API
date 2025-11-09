import requests
from bs4 import BeautifulSoup


def api_request_func(url):
    headers = {
        "User-Agent": (
            "Mozilla / 5.0(X11; Linux x86_64)"
            "AppleWebKit / 537.36(KHTML, like Gecko)"
            "Chrome / 77.0.3865.90 Safari / 537.36"
        )
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Request failed with status code: {response.status_code}")
    else:
        soup = BeautifulSoup(response.content, "html.parser")

    return soup
