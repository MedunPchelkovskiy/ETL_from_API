import pendulum
import prefect
from decouple import config
from prefect import flow
from prefect.states import Completed
from sqlalchemy import create_engine

from src.clients.datalake_client import fs_client
from src.core.exceptions import DataIssueError
from src.helpers.gold.extract import expected_months_map, \
    expand_season_to_months, \
    get_oldest_season_year_azure, get_oldest_season_year_postgres
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.decorators import measure_flow_duration
from src.helpers.observability_helpers.find_process_state import get_pending_work
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG, PIPELINE_STATUS_MAP, PIPELINE_ERROR_MAP, \
    QUARTER_START_MONTH, SEASON_START_MONTH
from src.helpers.observability_helpers.state_helpers import reconcile_processing_state, get_last_reconciled_date, \
    upsert_state_fn, get_current_retry_count, _get_oldest_available
from src.tasks.gold.extract_from_gold import get_monthly_gold_azure, get_monthly_gold_postgres
from src.tasks.gold.load_gold_data import load_gold_seasonally_summ_data_to_azure, \
    load_gold_seasonally_summ_data_to_postgres
from src.tasks.gold.transform_gold_data import get_seasonally_summ_data

PIPELINE_NAME = "gold_seasonal"  # TODO: Add season to build processing_level name in processing state table


@flow(name="Aggregate monthly to seasonal flow")
@measure_flow_duration(flow_name="gold_seasonal_flow")
def monthly_to_seasonally_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]
    expected_count = cfg["expected_count"]

    # ── early gate ────────────────────────────────────────────────────────────
    """
        in case of manual triggering, backfills, ad-hoc reruns, multiple schedules,
        someone changing the deployment schedule accidentally, CI/CD redeployments with immediate execution
    """

    last_reconciled = get_last_reconciled_date(PIPELINE_NAME)

    if last_reconciled is not None and now.month not in [3, 6, 9, 12]:
        logger.info("Not a season start month — skipping")
        return Completed(message="Skipped-NonSeasonal")

    logger.info(
        f"Starting {PIPELINE_NAME}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        },
    )
    end_date = now.start_of("month")  # current month excluded

    if last_reconciled is None:
        try:
            year, month = _get_oldest_available(
                pipeline_name=PIPELINE_NAME, fs_client=fs_client, engine=engine,
                season_cfg=PIPELINE_CONFIG["gold_seasonal"], monthly_cfg=PIPELINE_CONFIG["gold_monthly"],
            )
        except ValueError as e:
            logger.info(f"[{PIPELINE_NAME}] No source data found anywhere — nothing to reconcile yet | {e}")
            return Completed(message="Skipped-NoSourceDataYet")
        reconcile_start = pendulum.datetime(year=year, month=month, day=1)
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {year}-{month}")
    else:
        reconcile_start = last_reconciled
        logger.info(
            f"[{PIPELINE_NAME}] Reconciling from {reconcile_start.to_date_string()} "
            f"→ {end_date.to_date_string()}"
        )
    logger.info(f"[{PIPELINE_NAME}] DEBUG reconcile_start={reconcile_start} last_reconciled={last_reconciled}")
    reconcile_processing_state(
        pipeline_name=PIPELINE_NAME,
        start_date=reconcile_start,
        end_date=end_date,
        fs_client=fs_client,
        engine=engine,
    )
    # ── fetch pending work ────────────────────────────────────────────────────
    pending_seasons: list[pendulum.DateTime] = get_pending_work(
        processing_level=PIPELINE_NAME,
        statuses=PIPELINE_STATUS_MAP[PIPELINE_NAME],
        error_types=PIPELINE_ERROR_MAP[PIPELINE_NAME],
        max_retries=cfg["max_retries"],
    )

    if not pending_seasons:
        logger.info(f"[{PIPELINE_NAME}] No pending seasons — nothing to do")
        return Completed(message="Skipped-AlreadyProcessed")
    seasons_months = {}
    for season_start in pending_seasons:
        season_name = QUARTER_START_MONTH[season_start.month]
        season_year = season_start.year + 1 if season_name == "winter" else season_start.year
        season_label = f"{season_name}_{season_year}"

        seasons_months[season_label] = (expand_season_to_months(season_start))

    logger.info(f"[{PIPELINE_NAME}] {len(pending_seasons)} season(s) to process")

    # ── extract ───────────────────────────────────────────────────────────────
    all_seasons_dfs = {}
    missing_months = {}

    for season_label, season_months in seasons_months.items():
        season_name, season_year = season_label.split("_")
        season_year = int(season_year)
        expected = expected_months_map[season_name]

        for month in season_months:
            month_label = month.to_date_string()
            try:
                month_df = get_monthly_gold_azure(month)
                if season_label not in missing_months:  # ← само ако сезонът все още е "чист"
                    all_seasons_dfs.setdefault(season_label, []).append((month, month_df))
            except Exception as e:
                logger.warning(f"[{PIPELINE_NAME}] Azure failed for {month_label}, falling back to Postgres | {e}")
                try:
                    month_df = get_monthly_gold_postgres(month)
                    if season_label not in missing_months:
                        all_seasons_dfs.setdefault(season_label, []).append((month, month_df))
                except Exception as e2:
                    logger.error(f"[{PIPELINE_NAME}] Postgres fallback also failed for {month_label} | {e2}")
                    missing_months.setdefault(season_label, set()).add(month.month)
                    all_seasons_dfs.pop(season_label, None)  # премахва вече натрупаните частични данни
                    continue

        # ── missing months gate ─────────────────────────────────────────────────

        if season_label in missing_months:
            period_start_month = SEASON_START_MONTH[season_name]
            period_start_year = season_year - 1 if season_name == "winter" else season_year
            period_start = pendulum.datetime(period_start_year, period_start_month, 1)
            current_retries = get_current_retry_count(PIPELINE_NAME, period_start, period_name=season_label)

            if current_retries >= cfg["max_retries"]:
                status = "abandoned"
                error_message = (
                    f"Abandoned after {current_retries} retries — "
                    f"no source data available. Missing months: {missing_months[season_label]}"
                )
                logger.error(
                    f"[{PIPELINE_NAME}] Season {season_name} abandoned after "
                    f"{current_retries} retries — manual review required"
                )
            else:
                status = "pending"
                error_message = f"Missing months: {missing_months[season_label]}"
                logger.warning(
                    f"[{PIPELINE_NAME}] Season {season_name} — "
                    f"{len(missing_months[season_label])}/{len(expected)} missing months "
                    f"| retry {current_retries + 1}/{cfg['max_retries']}"
                )
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status=status,
                expected_count=expected_count,
                actual_count=len(season_months) - len(missing_months[season_label]),
                error_type="missing_partitions",
                error_message=error_message,
            )

            continue

    # ── transform ─────────────────────────────────────────────────
    season_data_aggregated = {}
    transform_failed_seasons = []
    for season_label, data in all_seasons_dfs.items():
        season, year = season_label.split("_")
        year = int(year)
        period_start_month = SEASON_START_MONTH[season]
        period_start_year = year - 1 if season == "winter" else year
        period_start = pendulum.datetime(period_start_year, period_start_month, 1)
        try:
            season_summ = get_seasonally_summ_data(season, year, data)
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="processing",
                expected_count=expected_count,
                actual_count=len(data),
            )

        except DataIssueError as e:
            # data / business issue
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="pending",
                expected_count=expected_count,
                error_message=str(e),
            )
            continue

        except Exception as e:
            # bug / unexpected
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="failed",
                expected_count=expected_count,
                error_type="missing_partitions",
                error_message=str(e)
            )
            transform_failed_seasons.append(season_label)
            continue
        season_data_aggregated[season_label] = season_summ

    # ── load ─────────────────────────────────────────────────
    failed_seasons = []
    for season_label, df in season_data_aggregated.items():
        season_name, season_year = season_label.split("_")
        season_year = int(season_year)
        period_start_month = SEASON_START_MONTH[season_name]
        period_start_year = season_year - 1 if season_name == "winter" else season_year
        period_start = pendulum.datetime(period_start_year, period_start_month, 1)

        azure_ok = False
        postgres_ok = False
        try:
            load_gold_seasonally_summ_data_to_azure(PIPELINE_NAME, season_label, df)
            azure_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Azure upload failed for {season_label}")

        try:
            load_gold_seasonally_summ_data_to_postgres(PIPELINE_NAME, df, season_label)
            postgres_ok = True

        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Postgres load failed for {season_label}")

        if azure_ok or postgres_ok:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="success",
                expected_count=expected_count,
                actual_count=expected_count,
            )
        else:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="failed",
                expected_count=expected_count,
                actual_count=0,
                error_type="missing_partitions",
                error_message="Both Azure and Postgres load failed",
            )
            failed_seasons.append(season_label)
    # ── final report ─────────────────────────────────────────────────────────

    if missing_months:
        logger.warning(f"[{PIPELINE_NAME}] Skipped missing data: {missing_months}")
    all_failed = list(set(list(missing_months.keys()) + failed_seasons + transform_failed_seasons))
    logger.info(
        f"[{PIPELINE_NAME}] Finished | "
        f"processed={len(season_data_aggregated)} | "
        f"missed={len(missing_months)} | "
        f"failed={len(failed_seasons) + len(transform_failed_seasons)}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": pendulum.now("UTC").to_iso8601_string(),
        },
    )
    if all_failed:
        return Completed(message=f"Partial-Failure: {len(all_failed)} season(s) failed: {all_failed}")

    return Completed(message=f"Success: {len(season_data_aggregated)} season(s) processed")


if __name__ == "__main__":
    monthly_to_seasonally_aggregation()
