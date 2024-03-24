from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable


API_KEY = Variable.get('secret_openweather_key')
URL = f'/data/2.5/weather?q=Moscow,ru&exclude=current&appid={API_KEY}&units=metric'


def choosing_description_weather(ti):
    current_temp = ti.xcom_pull(task_ids='get_temperature')
    if current_temp > 15:
        return 'warm_branch'
    return 'cold_branch'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 12),
    'retries': 1
}

dag = DAG(
    dag_id='get_temp_from_openweather',
    default_args=default_args,
    schedule_interval=None
)

get_response = HttpOperator(
    task_id='get_temperature',
    method='GET',
    http_conn_id='openweather',
    endpoint=URL,
    response_filter=lambda response: response.json()["main"]["temp"],
    headers={},
    dag=dag
)

choosing_description = BranchPythonOperator(
    task_id='choosing_result',
    python_callable=choosing_description_weather,
    dag=dag
)

warm_branch_task = PythonOperator(
    task_id='warm_branch',
    python_callable=lambda ti: print(f'тепло: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    dag=dag
)

cold_branch_task = PythonOperator(
    task_id='cold_branch',
    python_callable=lambda ti: print(f'холодно: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    dag=dag
)


get_response >> choosing_description
choosing_description >> warm_branch_task
choosing_description >> cold_branch_task
