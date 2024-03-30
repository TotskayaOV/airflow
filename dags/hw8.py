from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
import pandas as pd
from forex_python.converter import CurrencyRates
import os
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_booking_df():
    df = pd.read_csv("data/booking.csv", parse_dates=['booking_date'])
    return df

def get_client_df():
    df = pd.read_csv("data/client.csv")
    return df
def get_hotel_df():
    df = pd.read_csv("data/hotel.csv")
    return df

def transform_data(**kwargs):
    booking = kwargs['ti'].xcom_pull(task_ids='get_booking_df')
    client = kwargs['ti'].xcom_pull(task_ids='get_client_df')
    hotel = kwargs['ti'].xcom_pull(task_ids='data/hotel.csv')

    booking.dropna(inplace=True)
    booking.booking_date = booking.booking_date.str.replace('/', '-')
    c = CurrencyRates()
    eur_to_gbp_rate = c.get_rate('EUR', "GBP")
    eur_mask = booking['currency'] == 'EUR'
    booking.loc[eur_mask, 'booking_cost'] = (booking.loc[eur_mask, 'booking_cost'] * eur_to_gbp_rate).round(1)
    booking.loc[eur_mask, 'currency'] = 'GBP'
    hotel.dropna(inplace=True)
    client.dropna(inplace=True)
    client.age = client.age.astype('int')
    data = pd.merge(booking, hotel, on='hotel_id').merge(client, on='client_id')
    data.rename(columns={'name_x': 'name_hotel', 'name_y': 'name_client', 'address': 'address_hotel'}, inplace=True)
    data = data[['booking_date', 'client_id', 'name_client', 'type', 'age', 'hotel_id', 'name_hotel', 'room_type',
                 'booking_cost', 'currency']]
    if not os.path.exists('data/data.csv'):
        data.to_csv('data/data.csv', index=False)
    else:
        os.remove('data/data.csv')
        data.to_csv('data/data.csv', index=False)

def remove_data():
    if os.path.exists('data/data.csv'):
        os.remove('data/data.csv')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 23)
}

with DAG('transform_data', default_args=default_args, schedule_interval=None) as dag:
    get_booking = PythonOperator(
        task_id='get_booking_df',
        python_callable=get_booking_df
    )
    get_client = PythonOperator(
        task_id='get_client_df',
        python_callable=get_client_df
    )
    get_hotel = PythonOperator(
        task_id='get_hotel_df',
        python_callable=get_hotel_df
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    create_table_operator = PostgresOperator(
        task_id='create_data_gb_table',
        sql="""
            CREATE TABLE IF NOT EXISTS data_gb(
                booking_date DATE,
                client_id INT,
                name_client VARCHAR(50),
                type VARCHAR(20),
                age INT,
                hotel_id INT,
                name_hotel VARCHAR(50),
                room_type VARCHAR(20),
                booking_cost FLOAT,
                currency VARCHAR(5)
                );
        """,
        postgres_conn_id='postgres_airflow',
        database='airflow'
    )
    load_to_postgresql = PostgresOperator(
        task_id='load_to_postgres',
        postgres_conn_id='postgres_airflow',
        sql="""
        COPY data_gb FROM 'airflow/data/data.csv' WITH CSV HEADER;
        """,
    )
    remove_data_csv = PythonOperator(
        task_id='remove_data',
        python_callable=remove_data
    )

    send_message = TelegramOperator(
        task_id='send_message',
        telegram_conn_id='telegram_bot',
        text=f'Данные обработаны и сохранены в базу данных airflow '
             f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    )

    get_booking >> transform
    get_hotel >> transform
    get_client >> transform
    transform >> create_table_operator >> load_to_postgresql >> send_message >> remove_data_csv