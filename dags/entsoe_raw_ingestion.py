from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark.raw.generate_data import generate_data
from spark.raw.write_raw import write_raw
from spark.raw.verify_raw import verify_raw

with DAG(
    dag_id="entsoe_raw_ingestion",
    schedule="0 0 * * *",
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    task_1 = PythonOperator(task_id="generate_data", python_callable=generate_data, op_kwargs={"execution_date":"{{ ds }}"})
    task_2 = PythonOperator(task_id="write_raw", python_callable=write_raw, op_kwargs={"execution_date":"{{ ds }}"})
    task_3 = PythonOperator(task_id="verify_raw", python_callable=verify_raw, op_kwargs={"execution_date":"{{ ds }}"})

    task_1 >> task_2 >> task_3