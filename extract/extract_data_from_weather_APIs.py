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


def extract_data_from_sinoptik_api() -> pd.DataFrame:
    url = "https://www.sinoptik.bg/veliko-turnovo-bulgaria-100725993"
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

        current_city = soup.find("h1", class_="currentCity").text.strip()
        current_day = soup.find("span", class_="wfNonCurrentDate").text.strip()
        current_time = soup.find("span", class_="time").text.strip()
        current_temperature = soup.find("span", class_="wfCurrentTemp").text.strip()
        current_weather = soup.find("div", class_="wfCurrentWindContainer").text.strip()
        current_wind_speed = soup.find("span", class_="wfCurrentWind").text.strip()

    data = {
        "city": [current_city.capitalize()],
        "curr_day": [current_day.capitalize()],
        "curr_time": [current_time],
        # "temperature": [current_temperature],
        "temperature": re.findall(r'\d+', current_temperature)[0],
        "weather": [current_weather],

        # "wind": [current_wind_speed],
        "wind_m_s": re.findall(r'\d+', current_wind_speed)[0],
    }

    df = pd.DataFrame(data)

    return df


def extract_data_from_accuweather_api():
    url = "https://www.accuweather.com/bg/bg/veliko-tarnovo/46650/current-weather/46650"
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

        current_city = soup.find("h1", class_="header-loc").text.strip()
        current_day = soup.find("div", class_="content-module subnav-pagination").text.strip()
        current_time = soup.find("p", class_="sub").text.strip()
        current_temperature = soup.find("div", class_="display-temp").text.strip()
        current_weather = soup.find("div", class_="phrase").text.strip()
        current_wind_speed = soup.find("div", class_="detail-item spaced-content").text.strip()

    data = {
        "city": [current_city.capitalize()],
        "curr_day": [current_day.capitalize()],
        "curr_time": [current_time],
        # "temperature": [current_temperature],
        "temperature": re.findall(r'\d+', current_temperature)[0],
        "weather": [current_weather],
        # "wind": [current_wind_speed],
        "wind_m_s": re.findall(r'\d+', current_wind_speed)[0],
    }

    df = pd.DataFrame(data)

    return df
