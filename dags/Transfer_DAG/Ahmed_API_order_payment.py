import logging
from datetime import datetime
import os
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# -------------------- Config --------------------
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET = "ready-labs-postgres-to-gcs"
GCS_PREFIX = "ready-labs-postgres-to-gcs/ahmednabil/api/order_payment"
API_URL = "https://payments-table-834721874829.europe-west1.run.app"
BIGQUERY_DATASET = "ready-de26.project_landing"
BQ_TABLE = "order_payments_ahmednbail"
LOCAL_TMP_DIR = "/tmp"
LOCAL_FILENAME = "order_payments.csv"
LOCAL_FILE_PATH = os.path.join(LOCAL_TMP_DIR, LOCAL_FILENAME)
# -------------------- Python callables --------------------
def fetch_api_data_and_save() -> str:
    """
    Fetch CSV from the API and save it to /tmp.
    Returns the local file path.
    """
    try:
        logging.info(f"Fetching CSV from API: {API_URL}")
        resp = requests.get(API_URL, timeout=60)
        resp.raise_for_status()
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        with open(LOCAL_FILE_PATH, "w", encoding="utf-8", newline="") as f:
            f.write(resp.text)
        logging.info(f"CSV saved to {LOCAL_FILE_PATH}")
        return LOCAL_FILE_PATH
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while saving CSV: {e}")
        raise
def upload_to_gcs(local_file_path: str, ds: str) -> str:
    """
    Upload the local file to GCS under a date-partitioned path.
    Returns the GCS object path (without bucket).
    """
    gcs_object = f"{GCS_PREFIX}/dt={ds[:4]}/{ds[5:7]}/{ds[8:]}/{LOCAL_FILENAME}"
    logging.info(f"Uploading '{local_file_path}' to gs://{GCS_BUCKET}/{gcs_object}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_object)
    blob.upload_from_filename(local_file_path)
    logging.info("Upload completed.")
    return gcs_object
# -------------------- DAG --------------------
with DAG(
    dag_id="ahmednabil_order_payments_api_dag",
    schedule_interval=None,
    start_date=datetime(2025, 8, 25),
    catchup=False,
    tags=["api", "ahmednabil", "order_payments", "gcs-to-bq"],
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_order_payments_from_api",
        python_callable=fetch_api_data_and_save,
    )
    upload_file = PythonOperator(
        task_id="upload_order_payments_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "local_file_path": "{{ ti.xcom_pull(task_ids='fetch_order_payments_from_api') }}",
            "ds": "{{ ds }}",
        },
    )
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_order_payments_gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='upload_order_payments_to_gcs') }}"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BQ_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )
fetch_data >> upload_file >> load_gcs_to_bq