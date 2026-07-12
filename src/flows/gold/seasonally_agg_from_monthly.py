import pendulum
import prefect
from decouple import config
from prefect import flow
from prefect.states import Completed
from sqlalchemy import create_engine

from src.helpers.observability_helpers.decorators import measure_flow_duration
from src.clients.datalake_client import fs_client
from src.core.exceptions import DataIssueError
from src.helpers.gold.extract import expected_months_map, \
    get_oldest_monthly_date_azure, get_oldest_monthly_date_postgres, group_months_by_season
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.find_process_state import get_pending_work
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG, PIPELINE_STATUS_MAP, PIPELINE_ERROR_MAP
from src.helpers.observability_helpers.state_helpers import reconcile_processing_state, get_last_reconciled_date, \
    upsert_state_fn, get_current_retry_count
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
    end_date = now.start_of("month")  # текущият месец excluded
    max_missing = 0

    if last_reconciled is None:
        try:
            year, month = get_oldest_monthly_date_azure(fs_client, config("BASE_DIR_MONTHLY_SUMM_GOLD"))
        except Exception as e:
            logger.warning(f"[{PIPELINE_NAME}] Azure failed, falling back to Postgres | {e}")
            year, month = get_oldest_monthly_date_postgres(config("DB_CONN_RAW"))
        reconcile_start = pendulum.datetime(year=year, month=month, day=1)
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {year}-{month}")
    else:
        reconcile_start = last_reconciled
        logger.info(
            f"[{PIPELINE_NAME}] Reconciling from {reconcile_start.to_date_string()} "
            f"→ {end_date.to_date_string()}"
        )

    reconcile_processing_state(
        pipeline_name=PIPELINE_NAME,
        start_date=reconcile_start,
        end_date=end_date,
        fs_client=fs_client,
        engine=engine,
    )
    # ── fetch pending work ────────────────────────────────────────────────────
    pending_months: list[pendulum.DateTime] = get_pending_work(
        processing_level=PIPELINE_NAME,
        statuses=PIPELINE_STATUS_MAP[PIPELINE_NAME],
        error_types=PIPELINE_ERROR_MAP[PIPELINE_NAME],
        max_retries=cfg["max_retries"],
    )

    if not pending_months:
        logger.info(f"[{PIPELINE_NAME}] No pending months — nothing to do")
        return Completed(message="Skipped-AlreadyProcessed")

    grouped = group_months_by_season(pending_months)

    logger.info(f"[{PIPELINE_NAME}] {len(pending_months)} month(s) to process")

    # ── extract ───────────────────────────────────────────────────────────────
    all_seasons_dfs = {}
    missing_months = {}

    for season_label, season_months in grouped.items():
        season_name, season_year = season_label.split("_")
        season_year = int(season_year)
        expected = expected_months_map[season_name]
        missing = set(expected) - {dt.month for dt in season_months}

        if len(missing) > max_missing:
            missing_months[season_label] = missing

            continue

        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=season_months[0],
            period_name=season_label,
            status="processing",
            expected_count=len(expected),
        )
        for month in season_months:
            month_label = month.to_date_string()
            try:
                month_df = get_monthly_gold_azure(month)
                all_seasons_dfs.setdefault(season_label, []).append((month, month_df))
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=month,
                    status="success",
                    expected_count=len(expected),
                )
            except Exception as e:
                logger.warning(
                    f"[{PIPELINE_NAME}] Azure failed for {month_label}, falling back to Postgres | {e}"
                )
                try:
                    month_df = get_monthly_gold_postgres(month)
                    all_seasons_dfs.setdefault(season_label, []).append((month, month_df))
                    upsert_state_fn(
                        processing_level=PIPELINE_NAME,
                        partition_date=month,
                        status="success",
                        expected_count=len(expected),
                    )
                except Exception as e2:
                    logger.error(
                        f"[{PIPELINE_NAME}] Postgres fallback also failed for {month_label} | {e2}"
                    )
                    missing_months.setdefault(season_label, set()).add(month.month)
                    upsert_state_fn(
                        processing_level=PIPELINE_NAME,
                        partition_date=month,
                        status="pending",
                        expected_count=len(expected),
                    )
                    continue

        # ── missing months gate ─────────────────────────────────────────────────

        runtime_missing = missing_months.get(season_label, set())
        total_missing = missing | runtime_missing

        if len(total_missing) > max_missing:
            period_start_month = int(min(expected))
            if season_name == "winter":
                year = season_year - 1
            else:
                year = season_year
            period_start_month = pendulum.datetime(year, period_start_month, 1)
            current_retries = get_current_retry_count(PIPELINE_NAME, period_start_month)

            if current_retries >= cfg["max_retries"]:
                status = "abandoned"
                error_message = (
                    f"Abandoned after {current_retries} retries — "
                    f"no source data available. Missing months: {total_missing}"
                )
                logger.error(
                    f"[{PIPELINE_NAME}] Season {season_name} abandoned after "
                    f"{current_retries} retries — manual review required"
                )
            else:
                status = "pending"
                error_message = f"Missing months: {total_missing}"
                logger.warning(
                    f"[{PIPELINE_NAME}] Season {season_name} — "
                    f"{len(total_missing)}/{len(expected)} missing months "
                    f"| retry {current_retries + 1}/{cfg['max_retries']}"
                )
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start_month,
                period_name=season_label,
                status=status,
                expected_count=len(expected),
                actual_count=len(season_months),
                error_type="missing_partitions",
                error_message=error_message,
            )

            # upsert_state_fn(
            #     processing_level=PIPELINE_NAME,
            #     partition_date=period_start_month,
            #     status=status,
            #     expected_count=len(expected),
            #     actual_count=len(season_months),
            #     error_type="missing_partitions",
            #     error_message=error_message,
            # )

            continue

    # ── transform ─────────────────────────────────────────────────
    season_data_aggregated = {}
    for season_label, data in all_seasons_dfs.items():
        season, year = season_label.split("_")
        year = int(year)
        period_start_month = data[0][0]
        try:
            season_summ = get_seasonally_summ_data(season, year, data)
            # upsert_state_fn(
            #     processing_level=PIPELINE_NAME,
            #     partition_date=period_start_month,
            #     period_name=season_label,
            #     status="success",
            #     expected_count=len(expected),
            #     actual_count=len(expected),
            # )

            # upsert_state_fn(
            #     processing_level=PIPELINE_NAME,
            #     partition_date=period_start_month,
            #     status="success",
            #     expected_count=len(expected_months_map[season]),
            # )

        except DataIssueError as e:
            # data / business issue
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start_month,
                status="pending",
                expected_count=len(expected_months_map[season]),
                error_message=str(e),
            )
            continue

        except Exception as e:
            # bug / unexpected
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start_month,
                status="failed",
                error_message=str(e)
            )
            raise
        season_data_aggregated[season_label] = season_summ

    # ── load ─────────────────────────────────────────────────
    failed_seasons = []
    for season_label, df in season_data_aggregated.items():
        season_name, season_year = season_label.split("_")
        season_year = int(season_year)
        expected = expected_months_map[season_name]
        period_start_month = int(min(expected))
        if season_name == "winter":
            period_start = pendulum.datetime(season_year - 1, period_start_month, 1)
        else:
            period_start = pendulum.datetime(season_year, period_start_month, 1)

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
                expected_count=len(expected),
                actual_count=len(expected),
            )
        else:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=period_start,
                period_name=season_label,
                status="failed",
                expected_count=len(expected),
                actual_count=0,
                error_type="missing_partitions",
                error_message="Both Azure and Postgres load failed",
            )
            failed_seasons.append(season_label)
    # ── final report ──────────────────────────────────────────────────────────
    logger.info(
        f"[{PIPELINE_NAME}] Finished | "
        f"processed={len(season_data_aggregated)} | "
        f"missed={len(missing_months)} | "
        f"failed={len(failed_seasons)}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": pendulum.now("UTC").to_iso8601_string(),
        },
    )

    if missing_months:
        logger.warning(f"[{PIPELINE_NAME}] Skipped missing data: {missing_months}")

    if failed_seasons:
        raise RuntimeError(
            f"[{PIPELINE_NAME}] Both Azure and Postgres load failed for: {failed_seasons}"
        )


if __name__ == "__main__":
    monthly_to_seasonally_aggregation()
