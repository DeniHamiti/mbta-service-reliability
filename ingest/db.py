"""
Shared database connection + pipeline-run logging helpers for all ingest scripts.
Reads connection details from .env (see .env.example for the expected keys).
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_engine():
    """Build and return a SQLAlchemy engine connected to the project's Postgres."""
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    missing = [k for k, v in {
        "POSTGRES_USER": user, "POSTGRES_PASSWORD": password,
        "POSTGRES_HOST": host, "POSTGRES_PORT": port, "POSTGRES_DB": db,
    }.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required .env variables: {', '.join(missing)}")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def log_run_start(engine, source: str) -> int:
    """Insert a 'running' row into staging.pipeline_runs, return its id."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO staging.pipeline_runs (source, status)
                VALUES (:source, 'running')
                RETURNING id
            """),
            {"source": source},
        )
        run_id = result.scalar()
    print(f"[pipeline_runs] started run id={run_id} source={source}")
    return run_id


def log_run_finish(engine, run_id: int, status: str, rows_loaded: int = None, error_message: str = None) -> None:
    """Update a staging.pipeline_runs row once the run finishes (success or failed)."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE staging.pipeline_runs
                SET status = :status,
                    finished_at = now(),
                    rows_loaded = :rows_loaded,
                    error_message = :error_message
                WHERE id = :run_id
            """),
            {
                "status": status,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
                "run_id": run_id,
            },
        )
    print(f"[pipeline_runs] finished run id={run_id} status={status} rows_loaded={rows_loaded}")


def log_quality_issue(engine, check_name: str, table_name: str, issue_type: str,
                       record_key: str = None, details: dict = None) -> None:
    """Insert one row into staging.data_quality_issues."""
    import json
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO staging.data_quality_issues
                    (check_name, table_name, record_key, issue_type, details)
                VALUES
                    (:check_name, :table_name, :record_key, :issue_type, :details)
            """),
            {
                "check_name": check_name,
                "table_name": table_name,
                "record_key": record_key,
                "issue_type": issue_type,
                "details": json.dumps(details) if details is not None else None,
            },
        )
