from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.prepare_analysis_files import prepare_analysis_files

with DAG(
    dag_id="ml_tasks_dag",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 9, 29, 0, 0),
    catchup=False,
    tags=['ml'],
) as dag:
    generate_sales_csv = PythonOperator(
        task_id="prepare_analysis_files",
        python_callable=prepare_analysis_files,
    )