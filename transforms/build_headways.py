"""
Derive actual headways from raw.travel_times and materialize them into
analytics.headways.

A headway is the time gap between consecutive departures at the same stop,
on the same route, in the same direction. travel_times has multiple rows
per actual departure (one per downstream destination stop reached from that
origin), so we first collapse to distinct (trip, stop, direction, date,
departure_datetime) rows before computing gaps -- otherwise the same
departure would be counted many times over.

Partitioned by service_date as well as route/stop/direction, so the first
departure of a day is never diffed against the previous day's last one.

This is a MATERIALIZED VIEW: computed once and stored, so Tableau queries
are fast. Re-run this script (or REFRESH MATERIALIZED VIEW directly) after
loading new travel_times data.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

from sqlalchemy import text
from db import get_engine, log_run_start, log_run_finish

VIEW_NAME = "analytics.headways"

CREATE_VIEW_SQL = f"""
DROP MATERIALIZED VIEW IF EXISTS {VIEW_NAME};

CREATE MATERIALIZED VIEW {VIEW_NAME} AS
WITH distinct_departures AS (
    SELECT DISTINCT
        trip_id,
        route_id,
        from_stop_id,
        from_stop_name,
        direction_id,
        direction,
        service_date::date AS service_date,
        from_stop_departure_datetime::timestamptz AS departure_datetime
    FROM raw.travel_times
    WHERE from_stop_id IS NOT NULL
      AND from_stop_departure_datetime IS NOT NULL
)
SELECT
    trip_id,
    route_id,
    from_stop_id,
    from_stop_name,
    direction_id,
    direction,
    service_date,
    departure_datetime,
    LAG(departure_datetime) OVER (
        PARTITION BY route_id, from_stop_id, direction_id, service_date
        ORDER BY departure_datetime
    ) AS previous_departure_datetime,
    EXTRACT(
        EPOCH FROM (
            departure_datetime
            - LAG(departure_datetime) OVER (
                PARTITION BY route_id, from_stop_id, direction_id, service_date
                ORDER BY departure_datetime
            )
        )
    )::int AS headway_sec
FROM distinct_departures;
"""


def build_headways(engine) -> int:
    with engine.begin() as conn:
        conn.execute(text(CREATE_VIEW_SQL))
        result = conn.execute(text(f"SELECT COUNT(*) FROM {VIEW_NAME}"))
        row_count = result.scalar()
    print(f"Built {VIEW_NAME}: {row_count:,} rows")
    return row_count


if __name__ == "__main__":
    engine = get_engine()
    run_id = log_run_start(engine, "build_headways")
    try:
        rows = build_headways(engine)
        log_run_finish(engine, run_id, "success", rows_loaded=rows)
    except Exception as e:
        log_run_finish(engine, run_id, "failed", error_message=str(e))
        raise
