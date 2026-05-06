CREATE TABLE silver_weather_forecast_data (

    -- идентификация
    api_name TEXT NOT NULL,
    place_name TEXT NOT NULL,

    -- ingest метаданни
    ingest_date DATE NOT NULL,
    ingest_hour INT NOT NULL CHECK (ingest_hour BETWEEN 0 AND 23),

    -- оригинален timestamp от API
    original_ts TIMESTAMPTZ,

    -- forecast време
    forecast_date_utc DATE NOT NULL,
    forecast_hour_utc INT CHECK (
        forecast_hour_utc BETWEEN 0 AND 23
        OR forecast_hour_utc IS NULL
    ),

    -- температури
    temp_max NUMERIC,
    temp_min NUMERIC,
    temp_avg NUMERIC,

    -- валежи
    precipitation NUMERIC,
    rain NUMERIC,
    snow NUMERIC,
    precip_prob NUMERIC CHECK (precip_prob BETWEEN 0 AND 100),

    -- атмосферни
    humidity NUMERIC CHECK (humidity BETWEEN 0 AND 100),
    wind_speed NUMERIC,
    wind_deg NUMERIC,
    clouds NUMERIC,

    -- weather metadata
    weather_code INT,
    weather_description TEXT,
    weather_icon TEXT,

    -- астрономия
    sunrise TIMESTAMPTZ,
    sunset TIMESTAMPTZ
);


CREATE UNIQUE INDEX uq_forecast_versioned
ON silver_weather_forecast_data (
    api_name,
    place_name,
    forecast_date_utc,
    COALESCE(forecast_hour_utc, 42),
    ingest_date,
    ingest_hour
);
