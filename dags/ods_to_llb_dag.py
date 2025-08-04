from plugins.operators.ods_to_llb import OdsToLlbOperator
import json, os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

with open('pipeline_params/ods_to_llb.json') as f:
    pipeline_params = json.load(f)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
}

with DAG(
    dag_id='ods_to_llb_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ods', 'llb'],
) as dag:
    start = EmptyOperator(task_id='start') 
    end = EmptyOperator(task_id='end')

    with TaskGroup(group_id="ods_to_llb_tasks") as ods_to_llb_tasks:
        key_task_groups = []
        for key, value in pipeline_params.items():
            with TaskGroup(group_id=f"group_{key}") as key_task_group:
                # Add debug log for SQL file path
                if value.get('sql_skip', 0):
                    sql_path = f'dags/sql/ods_to_llb/extract/{value["des_table_name"]}.sql'
                    print(f"[DEBUG] Will look for SQL template at: {sql_path} for key: {key}")
                ods_to_llb = OdsToLlbOperator(
                    task_id=f'ods_to_llb_{key}',
                    ods_conn_id=value['ods_conn_id'],
                    staging_conn_id=value['staging_conn_id'],
                    src_schema_name=value['src_schema_name'],
                    src_table_name=value['src_table_name'],
                    des_schema_name=value['des_schema_name'],
                    des_table_name=value['des_table_name'],
                    columns=value['columns'],
                    sql_skip=value.get('sql_skip', 0),
                    cursor_field=value.get('cursor_field'),
                    chunk_size=value.get('chunk_size', 100000),
                    sql=value.get('sql', ""),  # Đảm bảo mỗi task có sql riêng nếu có
                )
                update_des_table = SQLExecuteQueryOperator(
                    task_id=f'update_des_table_{key}',
                    conn_id=value['staging_conn_id'],
                    sql=f'sql/ods_to_llb/update_des_table/{key}.sql',
                )
                ods_to_llb >> update_des_table
            key_task_groups.append(key_task_group)

    start >> ods_to_llb_tasks