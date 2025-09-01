from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

POSTGRES_CONN_ID = "Ahmednabil_orders_products_DB"
GCS_BUCKET = "ready-labs-postgres-to-gcs"  
GCS_PATH = "ahmednabil/orders"          
GCS_FILENAME = "Ahmednabil_orders"
BIGQUERY_DATASET = "project_landing"
BIGQUERY_TABLE = "Ahmednabil_orders"

default_args = {
    "owner": "Ahmed Nabil",
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="orders_db_transfer",
    default_args=default_args,
    description="Transfer orders data from postgres to gcs and then to bigquery",
    schedule_interval=None,
    catchup=False,
    tags=["orders", "db", "transfer"],
)

# Transfer data from Postgres to GCS for the execution date
transfer_postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"{BIGQUERY_TABLE}_postgres_to_gcs",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    SELECT * FROM orders
    ORDER BY updated_at_timestamp ASC
    """,
    bucket=GCS_BUCKET,
    filename=f"{GCS_PATH}/{GCS_FILENAME}_{{{{ ds_nodash }}}}.json",
    export_format="json",
    dag=dag,
)

# Load data from GCS to BigQuery
load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[f"{GCS_PATH}/{GCS_FILENAME}_{{{{ ds_nodash }}}}.json"],
    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_APPEND",  # Append new data to existing table
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)

transfer_postgres_to_gcs >> load_gcs_to_bigquery
