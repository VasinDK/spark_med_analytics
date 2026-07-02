from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    'my_first_airflow_dag',         # Уникальный ID графа в интерфейсе
    default_args=default_args,
    description='Простой тестовый DAG',
    schedule=None,         # Запуск только вручную (или '@daily' для ежедневного)
    start_date=datetime(2026, 1, 1), # Дата, с которой граф начинает гипотетически существовать
    catchup=False,                  # Не догонять прошлые даты при старте
) as dag:

    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task_2 = PythonOperator(
        task_id='run_python_func',
        python_callable=hello_python,
    )

    task_1 >> task_2