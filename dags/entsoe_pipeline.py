from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark.raw.write_raw import write_raw
from spark.raw.verify_raw import verify_raw
from spark.silver.transform_data import transform_data
from spark.silver.verify_silver import verify_silver
from spark.gold.production_per_day_per_country import production_per_day_per_country
from spark.gold.verify_gold import verify_gold


with DAG(
    dag_id="entsoe_pipeline",
    schedule="0 0 * * *",
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    task_R1 = PythonOperator(task_id="write_raw", python_callable=write_raw, op_kwargs={"execution_date":"{{ ds }}"})
    task_R2 = PythonOperator(task_id="verify_raw", python_callable=verify_raw, op_kwargs={"execution_date":"{{ ds }}"})
    task_S1 = PythonOperator(task_id="transform_data", python_callable=transform_data, op_kwargs={"execution_date":"{{ ds }}"})
    task_S2 = PythonOperator(task_id="verify_silver", python_callable=verify_silver, op_kwargs={"execution_date":"{{ ds }}"})
    task_G1 = PythonOperator(task_id="production_per_day_per_country", python_callable=production_per_day_per_country, op_kwargs={"execution_date":"{{ ds }}"})
    task_G2 = PythonOperator(task_id="verify_gold", python_callable=verify_gold, op_kwargs={"execution_date":"{{ ds }}"})

    task_R1 >> task_R2 >> task_S1 >> task_S2 >> task_G1 >> task_G2