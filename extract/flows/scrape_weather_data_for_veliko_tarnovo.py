from extract.workers.scrape_data_from_weather_sites import scrape_data_from_meteoblue_site, \
                                                   scrape_data_from_sinoptik_site, \
                                                   scrape_data_from_accuweather_site
from load.raw_data.workers.load_scrape_data_to_local_postgres import load_scrape_data_to_postgres_local
from transform.scraped_weather_data import accuweather_transformation
from transform.scraped_weather_data import meteoblue_transformation
from transform.scraped_weather_data import sinoptik_transformation

if __name__ == "__main__":
    meteoblue_data = scrape_data_from_meteoblue_site("https://www.meteoblue.com/en/weather/today/veliko-tarnovo_bulgaria_725993")
    sinoptik_data = scrape_data_from_sinoptik_site("https://www.sinoptik.bg/veliko-turnovo-bulgaria-100725993")
    accuweather_data = scrape_data_from_accuweather_site("https://www.accuweather.com/bg/bg/veliko-tarnovo/46650/current-weather/46650")


    transformed_accuweather_data = accuweather_transformation(accuweather_data)
    transformed_sinoptik_data = sinoptik_transformation(sinoptik_data)
    transformed_meteoblue_data = meteoblue_transformation(meteoblue_data)


    load_scrape_data_to_postgres_local(transformed_meteoblue_data, "meteoblue_data")
    load_scrape_data_to_postgres_local(transformed_sinoptik_data, "sinoptik_data")
    load_scrape_data_to_postgres_local(transformed_accuweather_data, "accuweather_data")

    print(meteoblue_data)
    print(sinoptik_data)
    print(transformed_accuweather_data)
