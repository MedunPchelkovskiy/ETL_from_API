import datetime
from typing import Any

import pendulum


def ensure_pendulum(dt: Any) -> pendulum.DateTime | None:
    if dt is None:
        return None
    if isinstance(dt, pendulum.DateTime):
        return dt
    if isinstance(dt, str):
        return pendulum.parse(dt)
    if isinstance(dt, datetime):
        return pendulum.instance(dt)
    raise TypeError(f"Unsupported type for week_start: {type(dt)}")