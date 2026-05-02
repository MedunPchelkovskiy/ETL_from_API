EXPECTED_COUNTS = {
    "daily_agg": 24,
    "weekly_agg": 7,
    "monthly_agg": 30,  # или динамично
}


def get_expected_count(pipeline_step: str, context: dict = None) -> int:
    base = EXPECTED_COUNTS[pipeline_step]

    # future-proof: ако имаш специални случаи
    if pipeline_step == "monthly_agg" and context:
        return context.get("days_in_month", base)

    return base
