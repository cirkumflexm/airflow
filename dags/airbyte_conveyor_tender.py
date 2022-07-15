from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airbyte_conveyor_selection import postgres_dst_selection, conveyor_data_bd

with DAG(
        'conveyor_tender',
        default_args={
            'email': ['mansur.insur@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
        },
        description='Postgres to Postgres',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['airbyte'],
) as dag:
    create_tender_table = PostgresOperator(
        # Создание таблицы для данных после обработки
        task_id="create_tender_table",
        postgres_conn_id="postgres_dst",
        sql="sql/dst_postgres_cr_table_tender.sql",
    )

    enum_procedure_type = 'tender'
    table_name_src = 'tenders'
    table_dst = 'tender_dst'

    conveyor_tender = PythonOperator(
        task_id='conveyor_tender',
        python_callable=conveyor_data_bd,
    )

    postgres_tender = PythonOperator(
        task_id='postgres_tender',
        python_callable=postgres_dst_selection,
    )

    create_tender_table >> conveyor_tender >> postgres_tender
