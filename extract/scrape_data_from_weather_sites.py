import re

import pandas as pd

from helpers.api_request import api_request_func


def scrape_data_from_meteoblue_site(url: str) -> pd.DataFrame:
    soup = api_request_func(url)

    current_time = soup.find("div", class_="current-description").text.strip()
    current_temperature = soup.find("div", class_="h1 current-temp").text.strip()
    city_div = soup.find("a", class_="home")
    curr_city = city_div.find("span", {"itemprop": "name"}).text.strip()

    data = {
        "city": [curr_city],
        "curr_time": [current_time],
        "temperature": re.findall(r'\d+', current_temperature)[0]
    }

    df = pd.DataFrame(data)

    return df


def scrape_data_from_sinoptik_site(url) -> pd.DataFrame:
    soup = api_request_func(url)

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
        "temperature": re.findall(r'\d+', current_temperature)[0],
        "weather": [current_weather],
        "wind_m_s": re.findall(r'\d+', current_wind_speed)[0],
    }

    df = pd.DataFrame(data)

    return df


def scrape_data_from_accuweather_site(url):
    soup = api_request_func(url)

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
        "wind_km_h": re.findall(r'\d+', current_wind_speed)[0],
    }

    df = pd.DataFrame(data)

    return df
