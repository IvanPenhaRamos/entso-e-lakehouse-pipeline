from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark.raw.write_raw import write_raw
from spark.raw.verify_raw import verify_raw
from spark.silver.transform_data import transform_data
from spark.silver.verify_silver import verify_silver


with DAG(
    dag_id="entsoe_pipeline",
    schedule="0 0 * * *",
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    task_1 = PythonOperator(task_id="write_raw", python_callable=write_raw, op_kwargs={"execution_date":"{{ ds }}"})
    task_2 = PythonOperator(task_id="verify_raw", python_callable=verify_raw, op_kwargs={"execution_date":"{{ ds }}"})
    task_3 = PythonOperator(task_id="transform_data", python_callable=transform_data, op_kwargs={"execution_date":"{{ ds }}"})
    task_4 = PythonOperator(task_id="verify_silver", python_callable=verify_silver, op_kwargs={"execution_date":"{{ ds }}"})

    task_1 >> task_2 >> task_3 >> task_4