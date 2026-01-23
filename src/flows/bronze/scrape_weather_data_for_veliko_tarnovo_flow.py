from src.workers.bronze.scrape_data_from_weather_sites_workers import scrape_data_from_meteoblue_site, \
                                                   scrape_data_from_sinoptik_site, \
                                                   scrape_data_from_accuweather_site
from src.workers.bronze.load_scrape_data_to_local_postgres_workers import load_scrape_data_to_postgres_local
from src.tasks.silver.scraped_weather_data_transformation_tasks import accuweather_transformation
from src.tasks.silver.scraped_weather_data_transformation_tasks import meteoblue_transformation
from src.tasks.silver.scraped_weather_data_transformation_tasks import sinoptik_transformation

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
