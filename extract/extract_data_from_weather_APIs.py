import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


def extract_data_from_meteoblue_api(city: str) -> pd.DataFrame:
    url = "https://www.meteoblue.com/en/weather/today/veliko-tarnovo_bulgaria_725993"
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


        current_time = soup.find("div", class_="current-description").text.strip()
        current_temperature = soup.find("div", class_="h1 current-temp").text.strip()

    data = {
        "city": [city.capitalize()],
        "curr_time": [current_time],
        "temperature": re.findall(r'\d+', current_temperature)[0]
    }

    df = pd.DataFrame(data)

    return df


def extract_data_from_sinoptik_api():
    pass


def extract_data_from_foreca_api():
    pass
