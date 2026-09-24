"""
Duplicate check: does the same (trip_id, from_stop_id, to_stop_id,
service_date) combination appear more than once in raw.travel_times?

That combination should be unique -- a single trip passes between a given
pair of stops once. More than one row for the same combination means
either a genuine duplicate in the source export, or an overlap in how the
month's file(s) were compiled.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

import pandas as pd
from sqlalchemy import text
from db import get_engine, log_quality_issue

CHECK_NAME = "duplicates"


def clear_previous_results(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM staging.data_quality_issues WHERE check_name = :check_name"),
            {"check_name": CHECK_NAME},
        )


def find_duplicates(engine) -> pd.DataFrame:
    query = text("""
        SELECT trip_id, from_stop_id, to_stop_id, service_date, COUNT(*) AS occurrences
        FROM raw.travel_times
        GROUP BY trip_id, from_stop_id, to_stop_id, service_date
        HAVING COUNT(*) > 1
        ORDER BY occurrences DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def run_check() -> None:
    engine = get_engine()
    print(f"Running check: {CHECK_NAME}")
    clear_previous_results(engine)

    dupes = find_duplicates(engine)
    print(f"  duplicate (trip_id, from_stop_id, to_stop_id, service_date) combinations: {len(dupes)}")

    for _, row in dupes.iterrows():
        record_key = f"{row['trip_id']}|{row['from_stop_id']}|{row['to_stop_id']}|{row['service_date']}"
        log_quality_issue(
            engine,
            check_name=CHECK_NAME,
            table_name="raw.travel_times",
            issue_type="duplicate",
            record_key=record_key,
            details={"occurrences": int(row["occurrences"])},
        )

    print(f"Check complete: {len(dupes)} distinct issues logged to staging.data_quality_issues")


if __name__ == "__main__":
    run_check()
