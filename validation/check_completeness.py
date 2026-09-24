"""
Completeness/gap check: are all expected calendar days actually present in
raw.travel_times.service_date? A missing day likely means either MBTA's own
data collection had an outage, or something went wrong in the export/load.

This generates the full list of days between the min and max service_date
actually present in the table, then finds which of those days have zero
rows.

Note: service_date is stored as TEXT in raw.travel_times (pandas inferred
it from the CSV as a plain string column), so it's explicitly cast to
::date here for date arithmetic to work.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

import pandas as pd
from sqlalchemy import text
from db import get_engine, log_quality_issue

CHECK_NAME = "completeness"


def clear_previous_results(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM staging.data_quality_issues WHERE check_name = :check_name"),
            {"check_name": CHECK_NAME},
        )


def find_missing_days(engine) -> pd.DataFrame:
    query = text("""
        WITH date_range AS (
            SELECT generate_series(
                MIN(service_date::date),
                MAX(service_date::date),
                '1 day'::interval
            )::date AS day
            FROM raw.travel_times
        ),
        present_days AS (
            SELECT DISTINCT service_date::date AS service_date FROM raw.travel_times
        )
        SELECT dr.day AS missing_date
        FROM date_range dr
        LEFT JOIN present_days pd ON dr.day = pd.service_date
        WHERE pd.service_date IS NULL
        ORDER BY dr.day
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def run_check() -> None:
    engine = get_engine()
    print(f"Running check: {CHECK_NAME}")
    clear_previous_results(engine)

    missing_days = find_missing_days(engine)
    print(f"  missing calendar days: {len(missing_days)}")

    for _, row in missing_days.iterrows():
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="gap",
            record_key=str(row["missing_date"]),
            details={"missing_date": str(row["missing_date"])},
        )

    print(f"Check complete: {len(missing_days)} distinct issues logged to staging.data_quality_issues")


if __name__ == "__main__":
    run_check()
