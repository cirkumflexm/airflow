from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from pydantic import parse_obj_as, BaseModel, validator


class Lot(BaseModel):
    name: str
    amount: float

    @validator("name")
    def check_str(cls, str_value):
        if "'" in str_value or '"' in str_value:
            str_value = str_value.replace('"', ' ').replace("'", ' ').strip()
        return str_value


class Selection(BaseModel):
    id: int
    name: str
    start_date: str
    end_date: str
    url: str
    address: str
    lots: List[Lot]
    enum_procedure_type: str

    @validator("name", "address")
    def check_str(cls, str_value):
        if "'" in str_value or '"' in str_value:
            str_value = str_value.replace('"', ' ').replace("'", ' ').strip()
        return str_value


with DAG(
        'airbyte_transformation_selection',
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
    create_selection_table = PostgresOperator(
        # Создание таблицы для данных после обработки
        task_id="create_selection_table",
        postgres_conn_id="postgres_dst",
        sql="sql/dst_postgres_cr_table_selection.sql",
    )


    def create_query_src_table(table: str, limit: int, offset: int) -> str:
        select = f"SELECT id, url, name, amount, status, end_date, start_date" \
                 f" FROM {table} LIMIT {limit} OFFSET {offset} ;"
        return select


    def create_query_src_table_details(id_tender: int) -> str:
        select = "SELECT id, lots, type, email, files, phone, region, country, contact_person, delivery_address" \
                 f" FROM selection_details WHERE id = {id_tender}"
        return select

    def make_query(src_cursor, limit_row: int, offset_row: int):
        query_src_table = create_query_src_table('selection', limit_row, offset_row)
        src_cursor.execute(query_src_table)
        src_data_rows = src_cursor.fetchall()
        row_count = src_cursor.rowcount
        return row_count, src_data_rows

    def transformation_one_row(src_cursor) -> dict:
        row_count = 1
        limit_row = 1000
        offset_row = 0
        while row_count:
            row_count, src_data_rows = make_query(src_cursor, limit_row, offset_row)
            offset_row += 1000
            for data_row in src_data_rows:
                query_src_table_details = create_query_src_table_details(data_row[0])
                src_cursor.execute(query_src_table_details)
                src_data_rows_details = src_cursor.fetchone()
                transformed_data = {'id': data_row[0], 'name': data_row[2], 'start_date': data_row[6],
                                    'end_date': data_row[5], 'url': data_row[1], 'address': src_data_rows_details[9],
                                    'enum_procedure_type': src_data_rows_details[2], 'lots': src_data_rows_details[1]}
                yield transformed_data

    def get_src_data():
        src = PostgresHook(postgres_conn_id='postgres_airbyte')
        src_conn = src.get_conn()
        src_cursor = src_conn.cursor()
        src_rows = transformation_one_row(src_cursor)
        for one_row in src_rows:
            yield one_row
        src_conn.close()

    def get_sql_query_many_dst(src_data: list, pk_new_row) -> str:
        first_part = f'INSERT INTO selection_dst_lot (selection_pk, name, amount) VALUES \n'
        query_data = []
        for row_data in src_data:
            query_data.append(f"({pk_new_row}, '{row_data['name']}', {row_data['amount']})")
        query_data_str = ",\n".join(query_data)
        sql_query_dst = '{} {};'.format(first_part, query_data_str)
        return sql_query_dst

    def get_sql_query_one_dst(data: dict):
        sql_query = sql.SQL('INSERT INTO selection_dst ({}) VALUES ({}) RETURNING pk;')\
            .format(
                    sql.SQL(", ").join(map(sql.Identifier, data)),
                    sql.SQL(", ").join(map(sql.Literal, data.values()))
                    )
        return sql_query

    def write_db_dst(row_data):
        dst = PostgresHook(postgres_conn_id='postgres_dst')
        dst_conn = dst.get_conn()
        dst_cursor = dst_conn.cursor()
        lots = row_data.pop('lots')
        query_dst = get_sql_query_one_dst(row_data)
        dst_cursor.execute(query_dst)
        tender_id = dst_cursor.fetchone()[0]
        if lots:
            query_slot_dst = get_sql_query_many_dst(lots, tender_id)
            #dst_cursor.execute(query_slot_dst)


        dst_conn.commit()
        dst_conn.close()

    def transformation_data():
        get_src_data()
        for src_rows in get_src_data():
            write_db_dst(src_rows)


    transformation = PythonOperator(
        task_id='transformation',
        python_callable=transformation_data,
    )

    create_selection_table >> transformation
