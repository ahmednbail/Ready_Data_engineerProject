from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

default_args = {
     'owner': 'airflow',
     'retries': 1,
 }

with DAG(
     dag_id='run_dbt_cloud_job',
     default_args=default_args,
     schedule_interval=None, 
     start_date=days_ago(1),
     catchup=False,
     tags=['dbt'],
) as dag:

 run_dbt_job = DbtCloudRunJobOperator(
         task_id='run_dbt_model',
         dbt_cloud_conn_id='Ahmed_dbt_connection',   
         job_id=531115131135151313,   
         check_interval=10,   
         timeout=300   
     )