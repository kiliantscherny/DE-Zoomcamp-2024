# 1. Download the CSV data from each dataset URL and save it to a local file in the AIRFLOW_HOME directory
# 2. Unzip the CSV files
# 3. Format it to Parquet format
# 4. Upload the Parquet file to GCS
# 5. Delete the local files
# 6. Create an external table in BigQuery

import os
import logging

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)

import pyarrow.csv as pv
import pyarrow.parquet as pq

from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "YellowTaxiDataIngestion",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 8, 1),
    max_active_runs=3,  # Limits concurrent runs to 3
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Taxi Data"]
)

# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
# new_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz"


# Starting with the YELLOW taxis

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
# Depending on the execution date of the task, it will get the correct file from the URL
URL_TEMPLATE = (
    URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)
OUTPUT_FILE_NAME = "yellow_taxi_{{ execution_date.strftime('%Y-%m') }}"
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


# Creates a client for your GCS storage
# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


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

    # This operator is a PythonOperator that uploads the parquet file to GCS
    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_NAME}.parquet",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_NAME}.parquet",
        },
    )

    # This operator is a BigQueryCreateExternalTableOperator that creates an external table in BigQuery
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "yellow_taxi_data",
                "tableId": TABLE_NAME_TEMPLATE,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{OUTPUT_FILE_NAME}.parquet"],
            },
        },
    )

    # Bash Operator that removes all the files created in the process
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/{OUTPUT_FILE_NAME}.*",
    )

(
    wget_task
    >> decompress_task
    >> format_to_parquet_task
    >> local_to_gcs_task
    >> bigquery_external_table_task
    >> cleanup_task
)
