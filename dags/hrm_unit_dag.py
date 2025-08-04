from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

from plugins.operators.sql_execute_query import CustomSQLExecuteQueryOperator
from plugins.operators.hrm_unit_to_ods import HRMToStagingDailyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27),
}

with open("pipeline_params/hrm_unit.json", "r") as file:
    pipeline_params = json.load(file)    

with DAG(
        'hrm_unit_dag',
        default_args=default_args,
        schedule_interval="40 0 * * *",
        catchup=False,
) as dag:
    
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task', trigger_rule=TriggerRule.ALL_DONE)

    with TaskGroup(group_id='connection_check_tasks') as connection_check_tasks:
        des_connect_check = SqlSensor(
            task_id='des_connect_check',
            conn_id='staging_conn_id',
            sql='SELECT 1',
            timeout=30,
            poke_interval=5,
            mode='reschedule',
        )
        
    from_date = datetime(1900, 4, 27).strftime("%d/%m/%Y")
    to_date = datetime.now().strftime("%d/%m/%Y")
    
    
    with TaskGroup(group_id="one_to_one_tasks") as one_to_one_tasks:
        for key, value in pipeline_params.items():
            with TaskGroup(group_id=key) as one_to_one_task:
            
                source_to_staging = HRMToStagingDailyOperator(
                    task_id         = f"source_to_staging_{key}",
                    from_date       = from_date,
                    to_date         = to_date,
                    staging_conn_id = value.get('staging_conn_id'),
                    des_schema_name = value.get('des_schema_name'),
                    des_table_name  = value.get('des_table_name'),
                    src_columns     = value.get('src_columns'),
                    des_columns     = value.get('des_columns'),
                    
                    # pool          ="bccp_pool",
                    # inlets        =Dataset(f"oracle://bccp/{value['src_schema_name']}/{value['src_table_name']}"),
                    outlets         =Dataset(f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}")
                )
                
                update_des_table = CustomSQLExecuteQueryOperator(
                    task_id         =f"update_des_table_{key}",
                    conn_id         =value.get('staging_conn_id'),
                    sql             =f"/sql/hrm/update_des_table/{key}.sql",
                    autocommit      =True,
                    des_schema_name = value.get('des_schema_name'),
                    params          ={
                                    "des_schema": value.get('des_schema_name'),
                                    "des_table" : value.get('des_table_name'),
                                    },
                    pool            ="doisoat_pool",
                    inlets          =Dataset(f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}"),
                    outlets         =Dataset(f"postgres://ods_database/doisoatvnpost/{value['des_schema_name']}/{value['des_table_name']}")
                )
                
                source_to_staging >> update_des_table

    start_task >> connection_check_tasks >> one_to_one_tasks  >> end_task
