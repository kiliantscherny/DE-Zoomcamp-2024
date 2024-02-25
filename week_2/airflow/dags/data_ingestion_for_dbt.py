# 1. Download the CSV data from each dataset URL and save it to a local file in the AIRFLOW_HOME directory
# 2. Format it to Parquet format
# 3. Upload the Parquet file to GCS
# 4. Create an external table in BigQuery

import os
import logging

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

# from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "DBTIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 7, 1),
    max_active_runs=3,  # Limits concurrent runs to 3
)

# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
# new_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz"


# Starting with the YELLOW taxis

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
# Depending on the execution date of the task, it will get the correct file from the URL
URL_TEMPLATE = (
    URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)
OUTPUT_FILE_NAME = "output_{{ execution_date.strftime('%Y-%m') }}"
OUTPUT_FILE_TEMPLATE_CSVGZ = AIRFLOW_HOME + "/" + OUTPUT_FILE_NAME + ".csv.gz"
OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + "/" + OUTPUT_FILE_NAME + ".csv"
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"


# Takes an input of your source file and converts it to parquet format
def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


# 2 tasks WGET and ingest

with local_workflow:

    # This operator is a BashOperator that downloads the CSV file from the URL
    wget_task = BashOperator(
        task_id="wget_task",
        # We save it to AIRFLOW_HOME because the default location is a /tmp/ folder that gets deleted after the task finishes
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE_CSVGZ}",
    )

    # This operator is a BashOperator that decompresses the .csv.gz file to a .csv file
    decompress_task = BashOperator(
        task_id="decompress_task",
        bash_command=f"gunzip -f {OUTPUT_FILE_TEMPLATE_CSVGZ} > {OUTPUT_FILE_TEMPLATE_CSV}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE_CSV,
        },
    )

    # # This operator is a PythonOperator that uploads the parquet file to GCS
    # # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    # local_to_gcs_task = PythonOperator(
    #     task_id="local_to_gcs_task",
    #     python_callable=upload_to_gcs,
    #     op_kwargs={
    #         "bucket": BUCKET,
    #         "object_name": f"raw/{parquet_file}",
    #         "local_file": f"{path_to_local_home}/{parquet_file}",
    #     },
    # )

wget_task >> decompress_task >> format_to_parquet_task
# >> format_to_parquet_task
