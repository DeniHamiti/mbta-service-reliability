"""
Anomaly check on raw.travel_times.travel_time_sec:
  1. Impossible values: negative or zero travel time.
  2. Statistical outliers: values far outside the normal range FOR THAT
     ROUTE, using the IQR (interquartile range) method. Outliers are
     computed per route_id rather than globally, since a normal Green Line
     travel time and a normal Red Line travel time are very different
     scales -- a global threshold would either flag half the Green Line
     or miss real Red Line outliers.

IQR method: outliers are values below Q1 - 1.5*IQR or above Q3 + 1.5*IQR,
where IQR = Q3 - Q1. This is a standard, distribution-agnostic way to find
outliers without assuming a normal distribution.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

import pandas as pd
from sqlalchemy import text
from db import get_engine, log_quality_issue

CHECK_NAME = "anomalies"


def clear_previous_results(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM staging.data_quality_issues WHERE check_name = :check_name"),
            {"check_name": CHECK_NAME},
        )


def find_impossible_values(engine) -> pd.DataFrame:
    """Negative or zero travel times -- physically impossible."""
    query = text("""
        SELECT route_id, COUNT(*) AS row_count, MIN(travel_time_sec) AS min_value
        FROM raw.travel_times
        WHERE travel_time_sec <= 0
        GROUP BY route_id
        ORDER BY row_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def find_statistical_outliers(engine) -> pd.DataFrame:
    """
    IQR-based outliers, computed per route_id. Only considers travel_time_sec
    > 0, since impossible values are already handled separately above.
    """
    query = text("""
        WITH stats AS (
            SELECT
                route_id,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY travel_time_sec) AS q1,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY travel_time_sec) AS q3
            FROM raw.travel_times
            WHERE travel_time_sec > 0
            GROUP BY route_id
        ),
        bounds AS (
            SELECT
                route_id,
                q1,
                q3,
                (q3 - q1) AS iqr,
                q1 - 1.5 * (q3 - q1) AS lower_bound,
                q3 + 1.5 * (q3 - q1) AS upper_bound
            FROM stats
        )
        SELECT
            tt.route_id,
            COUNT(*) AS row_count,
            MIN(tt.travel_time_sec) AS min_value,
            MAX(tt.travel_time_sec) AS max_value,
            b.lower_bound,
            b.upper_bound
        FROM raw.travel_times tt
        JOIN bounds b ON tt.route_id = b.route_id
        WHERE tt.travel_time_sec > 0
          AND (tt.travel_time_sec < b.lower_bound OR tt.travel_time_sec > b.upper_bound)
        GROUP BY tt.route_id, b.lower_bound, b.upper_bound
        ORDER BY row_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def run_check() -> None:
    engine = get_engine()
    print(f"Running check: {CHECK_NAME}")
    clear_previous_results(engine)

    impossible = find_impossible_values(engine)
    print(f"  routes with negative/zero travel_time_sec: {len(impossible)} (total rows: {impossible['row_count'].sum() if len(impossible) else 0})")
    for _, row in impossible.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="anomaly",
            record_key=str(row["route_id"]),
            details={"anomaly_type": "impossible_value", "affected_rows": int(row["row_count"]), "min_value": int(row["min_value"])},
        )

    outliers = find_statistical_outliers(engine)
    print(f"  routes with IQR outliers: {len(outliers)} (total rows: {outliers['row_count'].sum() if len(outliers) else 0})")
    for _, row in outliers.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="anomaly",
            record_key=str(row["route_id"]),
            details={
                "anomaly_type": "iqr_outlier",
                "affected_rows": int(row["row_count"]),
                "min_value": int(row["min_value"]),
                "max_value": int(row["max_value"]),
                "expected_lower_bound": round(float(row["lower_bound"]), 1),
                "expected_upper_bound": round(float(row["upper_bound"]), 1),
            },
        )

    total = len(impossible) + len(outliers)
    print(f"Check complete: {total} distinct issues logged to staging.data_quality_issues")


if __name__ == "__main__":
    run_check()
