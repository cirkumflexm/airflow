from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql


def preparation_var_dst_sql(data):
    data_sql = map(sql.Literal, data)
    result_data = sql.SQL('({})').format(sql.SQL(', ').join(data_sql))
    return result_data


def postgres_airbyte():
    src = PostgresHook(postgres_conn_id='postgres_airbyte')
    src_conn = src.get_conn()
    src_cursor = src_conn.cursor()
    src_cursor.execute("SELECT * FROM selection;")
    src_data = src_cursor.fetchall()
    src_conn.close()

    data_dst = [preparation_var_dst_sql(row) for row in src_data]
    query_dst = sql.SQL("INSERT INTO selection (id, url, name, amount, status, end_date, start_date, "
                        "_airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, _airbyte_selection_hashid)"
                        " VALUES {}").format(sql.SQL(', ').join(data_dst))
    print(query_dst)
    dst = PostgresHook(postgres_conn_id='postgres_dst')
    dst_conn = dst.get_conn()
    dst_cursor = dst_conn.cursor()
    dst_cursor.execute(query_dst)
    dst_conn.commit()
    dst_conn.close()


with DAG(
        'tutorial_airbyte',
        default_args={
            'email': ['mansur.insur@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Postgres to Postgres',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['airbyte'],
) as dag:
    create_selection_table = PostgresOperator(
        task_id="create_selection_table",
        postgres_conn_id="postgres_dst",
        sql="sql/dst_postgres_cr_table_selection_json.sql",
    )

    postgres_airbyte = PythonOperator(
        task_id='postgres_airbyte',
        python_callable=postgres_airbyte,
    )

    create_selection_table >> postgres_airbyte
