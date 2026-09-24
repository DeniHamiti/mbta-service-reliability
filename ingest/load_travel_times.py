"""
Load MBTA Rapid Transit Travel Times (Open Data Portal) into raw.travel_times.

Unlike GTFS, these files are downloaded manually from the Open Data Portal
(no stable, scriptable download URL) and placed in data/raw/TravelTimes_2026/.

Scope: one month to start (January 2026), both Heavy Rail (HR) and Light
Rail (LR) files, combined into a single raw.travel_times table since they
share an identical schema -- route_id already distinguishes HR vs LR lines.

These files are large (millions of rows each), so we read and write them
in CHUNKS rather than loading the whole file into memory at once -- this
keeps memory usage roughly constant regardless of file size.

Loaded unfiltered into `raw`, same principle as the GTFS load: no scoping
or cleaning here, that belongs downstream in staging.
"""
import os

import pandas as pd

from db import get_engine, log_run_start, log_run_finish

SOURCE_DIR = "data/raw/TravelTimes_2026"
FILES = [
    os.path.join(SOURCE_DIR, "2026-01_HRTravelTimes.csv"),
    os.path.join(SOURCE_DIR, "2026-01_LRTravelTimes.csv"),
]
TABLE_NAME = "travel_times"
CHUNK_SIZE = 100_000  # rows per chunk -- keeps memory usage low and roughly constant


def load_travel_times(filepaths: list, table_name: str, chunk_size: int = CHUNK_SIZE) -> int:
    """
    Read each CSV in filepaths in chunks and append each chunk into
    raw.<table_name> as it's read, rather than loading the whole file
    into memory first. Returns the total row count loaded.

    The table is dropped/recreated once at the start (first chunk of the
    first file), then every subsequent chunk is appended.
    """
    engine = get_engine()
    total_rows = 0
    table_initialized = False

    for filepath in filepaths:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Expected file not found: {filepath}")

        print(f"Loading {filepath} in chunks of {chunk_size:,} rows...")
        for chunk_num, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size, low_memory=False), start=1):
            if not table_initialized:
                # First chunk overall: drop/recreate the table
                chunk.to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
                table_initialized = True
            else:
                # Every subsequent chunk: append to the now-existing table
                chunk.to_sql(table_name, engine, schema="raw", if_exists="append", index=False)

            total_rows += len(chunk)
            print(f"  chunk {chunk_num}: +{len(chunk):,} rows (running total: {total_rows:,})")

    print(f"Loaded {total_rows:,} rows into raw.{table_name}")
    return total_rows


if __name__ == "__main__":
    engine = get_engine()
    run_id = log_run_start(engine, "travel_times")
    try:
        rows_loaded = load_travel_times(FILES, TABLE_NAME)
        log_run_finish(engine, run_id, "success", rows_loaded=rows_loaded)
    except Exception as e:
        log_run_finish(engine, run_id, "failed", error_message=str(e))
        raise
