# Weather Data Pipeline — Bulgarian Ski Resorts

An automated ETL pipeline that collects hourly weather data from 6 
external APIs, stores it reliably in the cloud, and aggregates it 
across multiple time horizons — running unattended, with automatic 
fallback if the cloud is unavailable.

---

## The Problem This Solves

Most free-tier weather APIs only return current conditions — no 
historical data. If you need reliable historical weather trends 
(for ski resort operations, agricultural planning, environmental 
monitoring, or similar), you either pay for expensive premium API 
access, or you build your own collection system.

This project is that system: it continuously captures hourly 
snapshots from multiple sources and builds a historical dataset 
over time, with zero manual intervention.

The same architecture applies to any scenario where you need to 
automatically collect, store, and aggregate data from external 
APIs on a schedule — not just weather data.

**What it handles automatically:**
- Hourly data collection from 6 independent sources
- Reliable cloud storage with local backup if the cloud is down
- Daily, weekly, monthly, and seasonal data aggregation
- Failure handling — if one API fails, the pipeline keeps running
- Structured logging so every run is traceable

---

## Tech Stack

Python 3.12 · Prefect 3 · Azure Data Lake · PostgreSQL · Pandas · 
Docker · Prometheus + Loki + Grafana (in progress)

---

## Architecture

Bronze → Silver → Gold (medallion architecture)

```
Bronze   Raw JSON from 6 weather APIs, stored as-is in 
         Azure Data Lake and local PostgreSQL

Silver   Parsed, validated, and normalized into Parquet

Gold     Aggregated datasets per time horizon — 
         daily, weekly, monthly, seasonal, yearly
```

Each layer runs as an independent Prefect deployment, coordinated 
by a master orchestrator flow. If Azure is unreachable, the 
pipeline automatically falls back to local PostgreSQL — no data 
is lost, no manual recovery needed.

**Data sources:** AccuWeather, Meteoblue, Tomorrow.io, 
OpenWeatherMap, WeatherAPI, Open-Meteo

**Locations (dev environment):** Bansko, Pamporovo, Borovets

---

## Project Structure

```
src/
├── flows/          Prefect flows per layer (bronze / silver / gold)
├── tasks/          Task definitions
├── workers/        Execution logic
├── helpers/        Shared utilities per layer
├── validation/     Input/output data validation
└── tests/          Integration tests

sql/                Table definitions for PostgreSQL
prefect.yaml        Deployment and schedule configuration
```

---

## Key Design Decisions

- **Medallion architecture** keeps raw, cleaned, and aggregated 
  data clearly separated — easier to debug, easier to reprocess 
  a single layer without touching the others.
- **PostgreSQL fallback** means the pipeline never silently loses 
  data if cloud storage has an outage.
- **Independent aggregation flows** per time horizon (daily, 
  weekly, monthly, seasonal, yearly) so each can run on its own 
  schedule and be modified without affecting the others.
- **Structured logging** with flow run and task run IDs on every 
  log line, making individual pipeline runs traceable end to end.

---

## Status

Core pipeline is stable and running on schedule. Grafana dashboards 
and expanded test coverage are in active development.