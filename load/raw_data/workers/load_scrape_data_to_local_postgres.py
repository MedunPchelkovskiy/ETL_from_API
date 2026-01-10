from decouple import config
from sqlalchemy import create_engine


def load_scrape_data_to_postgres_local(df, table_name):
    # Database credentials (replace with actual values)
    username = config("DB_USER")
    password = config("DB_PASSWORD")
    host = config("DB_HOST")
    port = config("DB_PORT")
    database = config("DB_NAME_FOR_SCRAPED_WEATHER_DATA")

    # Create connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

    # Create an engine to manage the connection to the PostgreSQL database
    engine = create_engine(connection_string)

    # Load the DataFrame into the PostgresSQL table (Replace existing table if exists)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Data loaded into {table_name} table in PostgresSQL successfully!")
