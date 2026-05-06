CREATE TABLE "gold_daily_summarized_data" (
    "id" BIGSERIAL PRIMARY KEY,
    "place_name" TEXT NOT NULL,
    "ingest_date" DATE NOT NULL,
    "ingest_hour" INTEGER NOT NULL,
    "forecast_date_utc" DATE NOT NULL,
    "generated_at" TIMESTAMP WITH TIME ZONE NOT NULL,
	"temp_min" NUMERIC,
    "temp_max" NUMERIC,
    "temp_avg" NUMERIC,
    "rain_min" NUMERIC,
    "rain_max" NUMERIC,
    "rain_avg" NUMERIC,
    "snow_min" NUMERIC,
    "snow_max" NUMERIC,
    "snow_avg" NUMERIC,
    "wind_speed_min" NUMERIC,
    "wind_speed_max" NUMERIC,
    "wind_speed_avg" NUMERIC,
    "cloud_cover_min" NUMERIC,
    "cloud_cover_max" NUMERIC,
    "cloud_cover_avg" NUMERIC,
    "humidity_min" NUMERIC,
    "humidity_max" NUMERIC,
    "humidity_avg" NUMERIC
);


CREATE UNIQUE INDEX unique_place_forecast
ON gold_daily_summarized_data (place_name, forecast_date_utc);