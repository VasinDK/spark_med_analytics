from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_python():
    print("Привет! Это код, выполненный внутри PythonOperator!")

with DAG(
    'my_first_airflow_dag',
    default_args=default_args,
    description='Простой тестовый DAG',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task_2 = PythonOperator(
        task_id='run_python_func',
        python_callable=hello_python,
    )
    IMAGE_TAG = Variable.get("dbt_worker_image_tag", default_var="latest")
      # CLICKHOUSE_USER: ${{ secrets.CLICKHOUSE_USER }}
      # CLICKHOUSE_PASSWORD: ${{ secrets.CLICKHOUSE_PASSWORD }}
      # CLICKHOUSE_DATABASE: ${{ secrets.CLICKHOUSE_DATABASE }}
      # CLICKHOUSE_HOST: ${{ secrets.CLICKHOUSE_HOST }}
      # CLICKHOUSE_PORT: ${{ secrets.CLICKHOUSE_PORT }}
      # ICE_ACCESS_KEY_ID: ${{ secrets.ICE_ACCESS_KEY_ID }}
      # ICE_SECRET_ACCESS_KEY: ${{ secrets.ICE_SECRET_ACCESS_KEY }}
    task_3 = DockerOperator(
        task_id='dbt_run',
        image=f'cr.yandex/crp00000000000000000/dbt_worker:{IMAGE_TAG}',  # Доработать
        command='dbt run --profiles-dir /usr/app --project-dir /usr/app',
        network_mode='airflow_default', 
        auto_remove='success',
    )
    task_1 >> task_2 >> task_3