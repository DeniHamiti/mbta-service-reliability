"""
Download, unzip, and load the MBTA GTFS static feed into raw.gtfs_* tables.

Everything is loaded as-is/unfiltered into `raw` -- no route/stop filtering
happens here. Any scoping (e.g. rapid transit only) belongs downstream in
staging, so raw stays a faithful, complete copy of the source feed.

MBTA updates this feed roughly monthly, so we skip re-downloading if we
already have a copy less than MAX_AGE_DAYS old.

Every run is logged to staging.pipeline_runs (source='gtfs'), so there's a
record of when this last ran, how many rows came in, and whether it failed.
"""
import glob
import os
import time
import zipfile

import pandas as pd
import requests

from db import get_engine, log_run_start, log_run_finish

GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
DEST_DIR = "data/raw/gtfs"
DEST_PATH = os.path.join(DEST_DIR, "MBTA_GTFS.zip")
EXTRACT_DIR = os.path.join(DEST_DIR, "extracted")

MAX_AGE_DAYS = 30  # MBTA updates GTFS roughly monthly


def download_gtfs(url: str, dest_path: str, max_age_days: int = MAX_AGE_DAYS) -> None:
    """
    Download the GTFS zip from `url` and save it to `dest_path`,
    unless a copy already exists that's newer than `max_age_days`.
    """
    if os.path.exists(dest_path):
        age_seconds = time.time() - os.path.getmtime(dest_path)
        age_days = age_seconds / 86400
        if age_days < max_age_days:
            print(
                f"Skipping download: {dest_path} is {age_days:.1f} days old "
                f"(< {max_age_days} day threshold)"
            )
            return
        else:
            print(f"Existing file is {age_days:.1f} days old, re-downloading...")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    response = requests.get(url)
    response.raise_for_status()  # raise an error if the download failed

    with open(dest_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded GTFS feed to {dest_path} ({len(response.content):,} bytes)")


def unzip_gtfs(zip_path: str, extract_dir: str) -> None:
    """Extract the GTFS zip into extract_dir."""
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    print(f"Extracted GTFS feed to {extract_dir}")


def load_all_gtfs_files(extract_dir: str) -> dict:
    """
    Load every .txt file in extract_dir into a matching raw.gtfs_<filename>
    table, fully unfiltered. Each run replaces the table's contents, since
    this is reference data that gets re-downloaded/reloaded wholesale.
    """
    engine = get_engine()
    results = {}

    txt_files = sorted(glob.glob(os.path.join(extract_dir, "*.txt")))
    if not txt_files:
        print(f"No .txt files found in {extract_dir} -- did unzip_gtfs run?")
        return results

    for filepath in txt_files:
        table_name = "gtfs_" + os.path.splitext(os.path.basename(filepath))[0]
        df = pd.read_csv(filepath, low_memory=False)
        df.to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
        results[table_name] = len(df)
        print(f"Loaded {len(df):,} rows into raw.{table_name}")

    return results


if __name__ == "__main__":
    engine = get_engine()
    run_id = log_run_start(engine, "gtfs")
    try:
        download_gtfs(GTFS_URL, DEST_PATH)
        unzip_gtfs(DEST_PATH, EXTRACT_DIR)
        results = load_all_gtfs_files(EXTRACT_DIR)
        log_run_finish(engine, run_id, "success", rows_loaded=sum(results.values()))
    except Exception as e:
        log_run_finish(engine, run_id, "failed", error_message=str(e))
        raise
