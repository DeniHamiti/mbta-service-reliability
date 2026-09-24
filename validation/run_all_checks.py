"""
Run all data quality validation checks in sequence, and print a summary
of what's currently in staging.data_quality_issues afterward.

Each individual check (referential integrity, duplicates, anomalies,
completeness) clears and re-logs its own results when it runs, so running
this script is always idempotent -- re-running it gives you a fresh,
current picture rather than accumulating stale findings.

This is also the single entry point a future run_pipeline.py would call
for the validation stage.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingest"))

import pandas as pd
from sqlalchemy import text
from db import get_engine, log_run_start, log_run_finish

import check_referential_integrity
import check_duplicates
import check_anomalies
import check_completeness

CHECKS = [
    ("referential_integrity", check_referential_integrity.run_check),
    ("duplicates", check_duplicates.run_check),
    ("anomalies", check_anomalies.run_check),
    ("completeness", check_completeness.run_check),
]


def print_summary(engine) -> None:
    query = text("""
        SELECT check_name, issue_type, COUNT(*) AS issue_count
        FROM staging.data_quality_issues
        GROUP BY check_name, issue_type
        ORDER BY check_name, issue_type
    """)
    with engine.connect() as conn:
        summary = pd.read_sql(query, conn)

    print("\n=== Data Quality Summary ===")
    if summary.empty:
        print("No issues currently logged.")
    else:
        for _, row in summary.iterrows():
            print(f"  {row['check_name']:<25} {row['issue_type']:<15} {row['issue_count']:>8} issue(s)")
    print("============================\n")


def run_all() -> None:
    engine = get_engine()
    run_id = log_run_start(engine, "validation")

    try:
        for name, check_func in CHECKS:
            print(f"\n--- {name} ---")
            check_func()

        print_summary(engine)
        log_run_finish(engine, run_id, "success")
    except Exception as e:
        log_run_finish(engine, run_id, "failed", error_message=str(e))
        raise


if __name__ == "__main__":
    run_all()
