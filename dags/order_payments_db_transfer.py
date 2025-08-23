from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)

POSTGRES_CONN_ID = "********"
GCS_BUCKET = "*********"
GCS_FILENAME = "******"
BIGQUERY_DATASET = "***********"
BIGQUERY_TABLE = "********"

default_args = {
    "owner": "Ahmed Nabil",
    "depends_on_past": False,
    "retries": 1,
}


dag = DAG(
    dag_id="order_payments_db_transfer",
    default_args=default_args,
    description="Transfer order_payments data from postgres to gcs and then to bigquery",
    schedule_interval=None,
    catchup=False,
    tags=["order_payments", "db", "transfer"],
)

transfer_postgres_to_gcs=PostgresToGCSOperator(
    task_id=f"{BIGQUERY_TABLE}_postgres_to_gcs",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT * FROM order_payments",
    bucket=GCS_BUCKET,
    filename=GCS_FILENAME,
    export_format="json",
    dag=dag,
)
load_gcs_to_bigquery=GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILENAME],
    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)


transfer_postgres_to_gcs >> load_gcs_to_bigquery