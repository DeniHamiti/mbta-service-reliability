"""
Referential integrity check: do the route_id / stop_id values referenced in
raw.travel_times actually exist in the GTFS reference tables (raw.gtfs_routes,
raw.gtfs_stops)?

Two distinct issue types are reported separately, since they mean different
things:
  - "missing_value": the column is NULL/blank in the source data
  - "referential": the column has a real value, but it doesn't match
    anything in the GTFS reference table (could be a genuine data problem,
    or a GTFS feed-version mismatch)

Findings are written to staging.data_quality_issues. Each run first clears
this check's previous rows, so re-running doesn't pile up stale duplicates.
"""
import sys
import os

# validation/ needs the db helpers from ingest/ -- add that folder to the path.
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

import pandas as pd
from sqlalchemy import text
from db import get_engine, log_quality_issue

CHECK_NAME = "referential_integrity"


def clear_previous_results(engine) -> None:
    """Delete this check's previous rows so re-running doesn't accumulate duplicates."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM staging.data_quality_issues WHERE check_name = :check_name"),
            {"check_name": CHECK_NAME},
        )


def check_route_ids(engine) -> pd.DataFrame:
    """Find non-null route_ids in travel_times that don't exist in gtfs_routes."""
    query = text("""
        SELECT tt.route_id, COUNT(*) AS row_count
        FROM raw.travel_times tt
        LEFT JOIN raw.gtfs_routes r ON tt.route_id = r.route_id
        WHERE r.route_id IS NULL AND tt.route_id IS NOT NULL
        GROUP BY tt.route_id
        ORDER BY row_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def check_stop_ids(engine) -> pd.DataFrame:
    """
    Find non-null from_stop_id / to_stop_id values in travel_times that
    don't exist in gtfs_stops. NULLs are handled separately by
    check_missing_stop_ids(), not here.
    """
    query = text("""
        SELECT combined.stop_id AS stop_id,
               COUNT(*) AS row_count,
               array_agg(DISTINCT combined.side) AS sides
        FROM (
            SELECT from_stop_id AS stop_id, 'from' AS side FROM raw.travel_times
            UNION ALL
            SELECT to_stop_id AS stop_id, 'to' AS side FROM raw.travel_times
        ) combined
        LEFT JOIN raw.gtfs_stops s ON combined.stop_id = s.stop_id
        WHERE s.stop_id IS NULL AND combined.stop_id IS NOT NULL
        GROUP BY combined.stop_id
        ORDER BY row_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def check_missing_stop_ids(engine) -> pd.DataFrame:
    """Count NULL from_stop_id / to_stop_id values, per side, in travel_times."""
    query = text("""
        SELECT side, COUNT(*) AS row_count
        FROM (
            SELECT 'from' AS side FROM raw.travel_times WHERE from_stop_id IS NULL
            UNION ALL
            SELECT 'to' AS side FROM raw.travel_times WHERE to_stop_id IS NULL
        ) missing
        GROUP BY side
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def run_check() -> None:
    engine = get_engine()

    print(f"Running check: {CHECK_NAME}")
    clear_previous_results(engine)

    bad_routes = check_route_ids(engine)
    print(f"  route_ids not found in gtfs_routes: {len(bad_routes)}")
    for _, row in bad_routes.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="referential",
            record_key=str(row["route_id"]),
            details={"missing_in": "raw.gtfs_routes", "affected_rows": int(row["row_count"])},
        )

    bad_stops = check_stop_ids(engine)
    print(f"  non-null stop_ids not found in gtfs_stops: {len(bad_stops)}")
    for _, row in bad_stops.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="referential",
            record_key=str(row["stop_id"]),
            details={"missing_in": "raw.gtfs_stops", "affected_rows": int(row["row_count"]), "sides": list(row["sides"])},
        )

    missing_stops = check_missing_stop_ids(engine)
    print(f"  NULL stop_id rows: {missing_stops['row_count'].sum() if len(missing_stops) else 0}")
    for _, row in missing_stops.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="missing_value",
            record_key=None,
            details={"column": f"{row['side']}_stop_id", "affected_rows": int(row["row_count"])},
        )

    total_issues = len(bad_routes) + len(bad_stops) + len(missing_stops)
    print(f"Check complete: {total_issues} distinct issues logged to staging.data_quality_issues")


if __name__ == "__main__":
    run_check()
