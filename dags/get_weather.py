from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
import random
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 16),
    'retries': 1
}

def random_square_print():
    num = random.randint(1, 100)
    sq_num = num ** 2
    print(f'Number = {num}, squared number = {sq_num}')

def print_weather(**kwargs):
    response = kwargs['ti'].xcom_pull(key=None, task_ids='get_weather')
    data = json.loads(response)
    print(f"Weather in Novosibirsk: temperature {data['temperature']}; wind {data['wind']}; description {data['description']}")


dag = DAG(dag_id='get_weather', default_args=default_args, schedule_interval=None)

task1 = BashOperator(
    task_id = 'print_random_num_bash',
    bash_command = 'echo $((RANDOM % 100))',
    dag=dag
)

task2 = PythonOperator(
    task_id = 'print_random_square_num',
    python_callable=random_square_print,
    dag=dag
)

task3 = SimpleHttpOperator(
    task_id='get_weather',
    method='GET',
    http_conn_id='goweather_api',
    endpoint='/weather/Novosibirsk',
    headers={},
    dag=dag
)

task4 = PythonOperator(
    task_id='print_weather',
    python_callable=print_weather,
    provide_context=True,
    dag=dag
)

task1 >> task2 >> task3 >> task4