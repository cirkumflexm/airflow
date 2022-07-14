"""

import json
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        'airbyte_conveyor_selection_json',
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
        task_id="create_table_dst_json",
        postgres_conn_id="postgres_dst",
        sql="sql/dst_postgres_cr_table_selection_json.sql",
    )


    def get_data_bd():
        src = PostgresHook(postgres_conn_id='postgres_airbyte')
        src_conn = src.get_conn()
        src_cursor = src_conn.cursor()
        src_cursor.execute("SELECT _airbyte_ab_id, _airbyte_data, _airbyte_emitted_at FROM _airbyte_raw_selection;")
        src_data_raw_selection = src_cursor.fetchall()
        src_cursor.execute(
            "SELECT _airbyte_ab_id, _airbyte_data, _airbyte_emitted_at FROM _airbyte_raw_selection_details;")
        src_data_raw_selection_details = src_cursor.fetchall()
        src_conn.close()
        return src_data_raw_selection, src_data_raw_selection_details


    def conveyor(selection_list, selection_details_list) -> list:
        selection_dict = {}
        for item in selection_list:
            selection_data = item[1]
            selection_dict[selection_data.get("id")] = selection_data

        data_list = []
        for item in selection_details_list:
            selection_details_data = item[1]

            lots = parse_obj_as(List[Lot], selection_details_data.get("lots"))

            data = selection_dict.get(selection_details_data.get("id"))
            selection_data = Selection(
                **data,
                address=selection_details_data.get("delivery_address"),
                lots=lots,
                enum_procedure_type="selection"
            )
            data_list.append(selection_data.dict())
            print(data_list)
        return data_list


    def conveyor_data_bd():
        selection_list, selection_details_list = get_data_bd()
        return conveyor(selection_list, selection_details_list)


    conveyor_op = PythonOperator(
        task_id='conveyor_op',
        python_callable=conveyor_data_bd,
    )

    def get_sql_query_dst(src_data: list) -> str:
        first_part = 'INSERT INTO selection_dst_json (json) VALUES \n'
        query_data = []
        for row_data in src_data:
            query_data.append("('{}')".format(json.dumps(row_data, ensure_ascii=False)))
        query_data_str = ",\n".join(query_data)
        sql_query_dst = '{} {};'.format(first_part, query_data_str)
        return sql_query_dst

    def postgres_dst(ti=None):
        src_data = ti.xcom_pull(task_ids='conveyor_op')
        dst = PostgresHook(postgres_conn_id='postgres_dst')
        dst_conn = dst.get_conn()
        dst_cursor = dst_conn.cursor()
        query_dst = get_sql_query_dst(src_data)
        dst_cursor.execute(query_dst)
        dst_conn.commit()
        dst_conn.close()


    postgres_airbyte = PythonOperator(
        task_id='postgres_airbyte',
        python_callable=postgres_dst,
    )

    create_selection_table >> conveyor_op >> postgres_airbyte
"""