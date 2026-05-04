import os
import logging
from datetime import datetime
from pathlib import Path
import subprocess
from airflow.sdk import DAG, task

# ---- Config ----
URL_PREFIX = "https://www.ncei.noaa.gov/oa/synoptic-summary-of-the-day/v2/archive/"
OUTPUT_YEAR_TEMPLATE = "{{ logical_date.strftime('%Y') }}"
URL_TEMPLATE =     URL_PREFIX + "ssod_v2.0.0_d" + OUTPUT_YEAR_TEMPLATE + "_c20260323.tar.gz"
TAR_URL = "https://www.ncei.noaa.gov/oa/synoptic-summary-of-the-day/v2/archive/ssod_v2.0.0_d2026_c20260323.tar.gz"
ARCHIVE_PATH = Path("/tmp/ssod_v2.0.0_d2022_c20260323.tar.gz")
TARGET_MEMBER = "SSOD_USW00094728_2026.csv"  # file inside tar.gz
OUTPUT_PATH = Path("/tmp/SSOD_USW00094728_2026.csv")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
AIRFLOW_HOME_DATA =  os.environ.get("AIRFLOW_HOME_DATA", "/opt/airflow/data")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")


def do_stuff(csv_dir, src_file, path_dir, ti):
    if not src_file.endswith("csv"):
        logging.error("Can only accept csv format")
        return
    csv_file = f"{csv_dir}/{src_file}"
@task
def download_tar_gz() -> str:
    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["curl", "-fL", "-sS", URL_TEMPLATE, "-o", str({ARCHIVE_PATH})],
        check=True,
    )
    return str({ARCHIVE_PATH})

@task
def extract_single_file(archive_path: str) -> str:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Extract only one member and write to OUTPUT_PATH
    # Equivalent shell:
    # tar -xzf /tmp/archive.tar.gz -O path/inside/archive/data.csv > /tmp/extracted_data.csv
    with open(OUTPUT_PATH, "wb") as out:
        subprocess.run(
            ["tar", "-xzf", archive_path, "-O", TARGET_MEMBER],
            check=True,
            stdout=out,
        )
    return str(OUTPUT_PATH)

with DAG(
    dag_id="download_and_extract_one_file_cli",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    extract_single_file(download_tar_gz())
