from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print("Hello World")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('hello_world_dag',
          default_args=default_args,
          description='A simple Hello World DAG',
          schedule_interval=timedelta(days=1),
          catchup=False)

hello_world_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag,
)

hello_world_task
