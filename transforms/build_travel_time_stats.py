@'
"""
Compute travel-time variability stats from raw.travel_times and materialize
them into analytics.travel_time_stats.

For each (route_id, from_stop_id, to_stop_id, time_period) group:
  - median, p90, p95 travel time
  - standard deviation
  - buffer_time_index = p95 - median, i.e. how much extra time a rider
    should budget beyond the "typical" trip to be reliably on time

time_period buckets the trip's departure hour into AM peak / midday /
PM peak / evening / night, since the same stop pair behaves very
differently at 8am vs 11pm and lumping them together would blur the signal.

Filters applied (found via data investigation, see project notes):
  - Excludes rows where from_stop_id = to_stop_id. These are not real
    "travel between two stops" -- mostly near-zero measurement noise from
    loops/short-turns, but a small number are multi-hour trip-matching
    errors in the source data that badly distort percentiles for the
    handful of low-volume groups they land in.
  - Excludes NONREV-prefixed trip_ids: non-revenue vehicle moves (e.g. to
    or from the yard), not passenger service, and shouldn't count toward
    a rider-facing reliability metric.

This is a MATERIALIZED VIEW: computed once and stored for fast Tableau
queries. Re-run this script after loading new travel_times data.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

from sqlalchemy import text
from db import get_engine, log_run_start, log_run_finish

VIEW_NAME = "analytics.travel_time_stats"

CREATE_VIEW_SQL = f"""
DROP MATERIALIZED VIEW IF EXISTS {VIEW_NAME};

CREATE MATERIALIZED VIEW {VIEW_NAME} AS
WITH tagged AS (
    SELECT
        route_id,
        from_stop_id,
        from_stop_name,
        to_stop_id,
        to_stop_name,
        travel_time_sec,
        EXTRACT(HOUR FROM from_stop_departure_datetime::timestamptz) AS departure_hour,
        CASE
            WHEN EXTRACT(HOUR FROM from_stop_departure_datetime::timestamptz) BETWEEN 6 AND 8 THEN 'AM Peak'
            WHEN EXTRACT(HOUR FROM from_stop_departure_datetime::timestamptz) BETWEEN 9 AND 14 THEN 'Midday'
            WHEN EXTRACT(HOUR FROM from_stop_departure_datetime::timestamptz) BETWEEN 15 AND 18 THEN 'PM Peak'
            WHEN EXTRACT(HOUR FROM from_stop_departure_datetime::timestamptz) BETWEEN 19 AND 22 THEN 'Evening'
            ELSE 'Night'
        END AS time_period
    FROM raw.travel_times
    WHERE travel_time_sec > 0
      AND from_stop_id IS NOT NULL
      AND to_stop_id IS NOT NULL
      AND from_stop_departure_datetime IS NOT NULL
      AND from_stop_id <> to_stop_id
      AND trip_id NOT LIKE 'NONREV-%'
)
SELECT
    route_id,
    from_stop_id,
    from_stop_name,
    to_stop_id,
    to_stop_name,
    time_period,
    COUNT(*) AS trip_count,
    ROUND(AVG(travel_time_sec)) AS mean_travel_time_sec,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY travel_time_sec) AS median_travel_time_sec,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY travel_time_sec) AS p90_travel_time_sec,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY travel_time_sec) AS p95_travel_time_sec,
    ROUND(STDDEV(travel_time_sec)::numeric, 1) AS stddev_travel_time_sec,
    ROUND(
        (percentile_cont(0.95) WITHIN GROUP (ORDER BY travel_time_sec)
         - percentile_cont(0.5) WITHIN GROUP (ORDER BY travel_time_sec))::numeric,
        1
    ) AS buffer_time_index_sec
FROM tagged
GROUP BY route_id, from_stop_id, from_stop_name, to_stop_id, to_stop_name, time_period;
"""


def build_travel_time_stats(engine) -> int:
    with engine.begin() as conn:
        conn.execute(text(CREATE_VIEW_SQL))
        result = conn.execute(text(f"SELECT COUNT(*) FROM {VIEW_NAME}"))
        row_count = result.scalar()
    print(f"Built {VIEW_NAME}: {row_count:,} rows")
    return row_count


if __name__ == "__main__":
    engine = get_engine()
    run_id = log_run_start(engine, "build_travel_time_stats")
    try:
        rows = build_travel_time_stats(engine)
        log_run_finish(engine, run_id, "success", rows_loaded=rows)
    except Exception as e:
        log_run_finish(engine, run_id, "failed", error_message=str(e))
        raise
'@ | Out-File -Encoding utf8 transforms\build_travel_time_stats.py