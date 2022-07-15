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
        'airbyte_conveyor_selection',
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

    enum_procedure_type = 'selection'
    table_name_src = 'selection'
    table_dst = 'selection_dst'

    def get_data_bd(select, select_details):
        src = PostgresHook(postgres_conn_id='postgres_airbyte')
        src_conn = src.get_conn()
        src_cursor = src_conn.cursor()
        src_cursor.execute(select)
        src_data_raw_selection = src_cursor.fetchall()
        src_cursor.execute(select_details)
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
                enum_procedure_type=enum_procedure_type
            )
            data_list.append(selection_data.dict())
        return data_list


    def conveyor_data_bd():
        select = f"SELECT _airbyte_ab_id, _airbyte_data, _airbyte_emitted_at FROM _airbyte_raw_{table_name_src};"
        select_details = f"SELECT _airbyte_ab_id, _airbyte_data, _airbyte_emitted_at FROM _airbyte_raw_{table_name_src}_details;"
        selection_list, selection_details_list = get_data_bd(select, select_details)
        return conveyor(selection_list, selection_details_list)


    conveyor_op = PythonOperator(
        task_id='conveyor_op',
        python_callable=conveyor_data_bd,
    )


    def get_sql_query_many_dst(src_data: list, first_part: str, pk_new_row) -> str:
        query_data = []
        for row_data in src_data:
            query_data.append(f"({pk_new_row}, '{row_data['name']}', {row_data['amount']})")
        query_data_str = ",\n".join(query_data)
        sql_query_dst = '{} {};'.format(first_part, query_data_str)
        return sql_query_dst


    def get_sql_query_one_dst(row_data: dict, first_part: str) -> str:
        query_data = (f"({row_data['id']}, '{row_data['name']}', '{row_data['start_date']}', "
                      f"'{row_data['end_date']}', '{row_data['url']}', '{row_data['address']}',"
                      f" '{row_data['enum_procedure_type']}')")
        sql_query_dst = '{} {} RETURNING pk;'.format(first_part, query_data)
        return sql_query_dst


    insert_query = f'INSERT INTO {table_dst} (id, name, start_date, end_date, url, address, enum_procedure_type) VALUES \n'
    insert_slot_query = f'INSERT INTO {table_dst}_lots (selection_pk, name, amount) VALUES \n'

    def postgres_dst_selection(ti=None):
        dst = PostgresHook(postgres_conn_id='postgres_dst')
        dst_conn = dst.get_conn()
        dst_cursor = dst_conn.cursor()
        src_data = ti.xcom_pull(task_ids='conveyor_op')
        for row_data in src_data:
            query_dst = get_sql_query_one_dst(row_data, insert_query)
            dst_cursor.execute(query_dst)
            pk_new_row = dst_cursor.fetchone()[0]
            if row_data['lots']:
                query_slot_dst = get_sql_query_many_dst(row_data['lots'], insert_slot_query, pk_new_row)
            dst_cursor.execute(query_slot_dst)
        dst_conn.commit()
        dst_conn.close()


    postgres_airbyte = PythonOperator(
        task_id='postgres_airbyte',
        python_callable=postgres_dst_selection,
    )

    create_selection_table >> conveyor_op >> postgres_airbyte
