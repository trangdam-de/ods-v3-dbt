from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

from plugins.operators.khl_to_ods_backdate import KHLToStagingDailyOperator
from plugins.operators.sql_execute_query import CustomSQLExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with open("pipeline_params/khl.json", "r") as file:
    pipeline_params = json.load(file)

with DAG(
        'khl_backdate_dag__dtl_full',
        default_args=default_args,
        schedule_interval="0 2 * * *",
        catchup=True,
        max_active_runs = 1,
) as dag:
    
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    with TaskGroup(group_id='connection_check_tasks') as connection_check_tasks:
        src_connect_check = SqlSensor(
            task_id='src_connect_check',
            conn_id='khl_conn_id',
            sql='SELECT 1 FROM dual',
            timeout=30,
            poke_interval=5,
            mode='reschedule',
        )

        des_connect_check = SqlSensor(
            task_id='des_connect_check',
            conn_id='staging_conn_id',
            sql='SELECT 1',
            timeout=30,
            poke_interval=5,
            mode='reschedule',
        )
        
    now = datetime(2024, 12, 28)
    last_monday = now - timedelta(days=now.weekday())    
    start_time = last_monday.replace(hour=14).strftime('%Y-%m-%d %H:%M:%S')
    end_time = last_monday.replace(hour=17).strftime('%Y-%m-%d %H:%M:%S')
    # start_date = datetime(2024, 11, 1)
    # end_date = datetime(2024, 11, 2)

    # current_date = start_date
    # while current_date <= end_date:
    
    # Function để kiểm tra xem bảng có nên xử lý hay không
    def should_process_table(table_name, **context):
        tables_to_process = ["khl_portal_item_dtl", "khl_portal_item_hdr"]
        run_type = context["dag_run"].run_type
        # Nếu là scheduled hoặc không có tables chỉ định, chạy tất cả bảng
        if not tables_to_process:
            return True
        # Nếu trigger thủ công và có chỉ định tables, chỉ chạy các bảng được chọn
        return table_name in tables_to_process
        
    with TaskGroup(group_id=f"one_to_one_tasks") as one_to_one_tasks:
        for key, value in pipeline_params.items():
            with TaskGroup(group_id=key) as one_to_one_task:
                    # Kiểm tra xem có nên xử lý bảng này không
                    check_table = ShortCircuitOperator(
                        task_id=f"check_{key}",
                        python_callable=should_process_table,
                        op_kwargs={"table_name": key},
                    )
                    
                    source_to_staging = KHLToStagingDailyOperator(
                        task_id=f"source_to_staging_{key}",
                        khl_conn_id= value.get('khl_conn_id'),
                        staging_conn_id = value.get('staging_conn_id'),
                        src_schema_name = value.get('src_schema_name'),
                        src_table_name  = value.get('src_table_name'),
                        cursor_field    = value.get('cursor_field'),
                        des_schema_name = value.get('des_schema_name'),
                        des_table_name  = value.get('des_table_name'),
                        columns         = value.get('columns'),
                        middle_storage_conn_id = "minio_conn_id",
                        bucket = "daily",
                        # pool            ="khl_pool",
                        # start_time      = start_date,
                        # end_time        = end_date,
                        # inlets          =Dataset(f"oracle://khl/{value['src_schema_name']}/{value['src_table_name']}"),
                        outlets         =Dataset(f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}")
                    )
                    
                    update_des_table = CustomSQLExecuteQueryOperator(
                        task_id         =f"update_des_table_{key}",
                        conn_id         =value.get('staging_conn_id'),
                        sql             =f"/sql/khl/update_des_table/{key}.sql",
                        autocommit      =True,
                        des_schema_name = value.get('des_schema_name'),
                        params          ={
                                        "des_schema_name": value.get('des_schema_name'),
                                        "des_table_name" : value.get('des_table_name'),
                                        },
                        # pool            ="doisoat_pool",
                        # inlets          =Dataset(f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}"),
                        outlets         =Dataset(f"postgres://ods_database/doisoatvnpost/{value['des_schema_name']}/{value['des_table_name']}")
                    )
                    
                    check_table >> source_to_staging >> update_des_table 
 
    start_task >>\
        connection_check_tasks >>\
            one_to_one_tasks 
