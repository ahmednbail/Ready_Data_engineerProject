from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import json
from datetime import datetime


# ========= CONFIG ========= #
API_URL = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"   
BUCKET_NAME = "your-gcs-bucket-name"
GCP_CONN_ID = "google_cloud_default"  # Composer sets this up by default
# ========================== #


def fetch_api_data(**context):
    """Fetch data from the API and return it."""
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()


def upload_to_gcs(**context):
    """Upload API data to GCS as JSON file."""
    data = context['ti'].xcom_pull(task_ids="fetch_data")

    # Add execution date to filename for partitioning
    execution_date = context["ds"]  # YYYY-MM-DD
    destination_blob = f"api_data/posts_{execution_date}.json"

    # Convert to JSON string
    json_data = json.dumps(data, indent=2)

    # Upload to GCS
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=destination_blob,
        data=json_data,
        mime_type="application/json",
    )
    print(f"✅ Uploaded data to gs://{BUCKET_NAME}/{destination_blob}")


with DAG(
    dag_id="api_to_gcs_cloud_composer",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",   # Adjust schedule
    catchup=False,
    tags=["composer", "api", "gcs"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_api_data,
    )

    upload_data = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    fetch_data >> upload_data
