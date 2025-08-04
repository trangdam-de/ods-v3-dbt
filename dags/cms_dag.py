import json
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import ShortCircuitOperator
from plugins.operators.cms_to_ods import CMSToStagingDailyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with open("pipeline_params/cms.json", "r") as file:
    pipeline_params = json.load(file)

with DAG(
        'cms_dag_truncate',
        default_args=default_args,
        schedule_interval="10 0 * * *",
        catchup=False,
        max_active_runs = 1,
        # max_active_tasks=1
) as dag:
    start_task = DummyOperator(task_id='start_task')

    with TaskGroup(group_id='connection_check_tasks') as connection_check_tasks:
        src_connect_check = SqlSensor(
            task_id='src_connect_check',
            conn_id='cms_conn_id',
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

    # Function d? ki?m tra xem b?ng có nên x? lý hay không
    def should_process_table(table_name, **context):
        tables_to_process = ["customers","contracts","customer_relationship"]
        run_type = context["dag_run"].run_type
        if  not tables_to_process:
            return True
    
        return table_name in tables_to_process

    with TaskGroup(group_id="one_to_one_tasks") as one_to_one_tasks:
        for key, value in pipeline_params.items():
            # if value['skip'] > 0:
            #     continue

            with TaskGroup(group_id=key) as one_to_one_task:
                # Ki?m tra xem có nên x? lý b?ng này không
                check_table = ShortCircuitOperator(
                    task_id=f"check_{key}",
                    python_callable=should_process_table,
                    op_kwargs={"table_name": key},
                )

                source_to_staging = CMSToStagingDailyOperator(
                    task_id=f"source_to_staging_{key}",
                    cms_conn_id=value['cms_conn_id'],
                    staging_conn_id=value['staging_conn_id'],
                    src_schema_name=value['src_schema_name'],
                    src_table_name=value['src_table_name'],
                    des_schema_name=value['des_schema_name'],
                    des_table_name=value['des_table_name'],
                    columns=value['columns'],
                    cursor_field=value['cursor_field'],
                    sql_skip=value['sql_skip'],
                    sql=f"/sql/cms/extract/{key}.sql",
                    params={
                        'src_schema_name': value['src_schema_name'],
                        'src_table_name': value['src_table_name'],
                        'cursor_field': value['cursor_field'],
                    },
                    pool="cms_pool",
                    inlets=Dataset(f"oracle://cms/{value['src_schema_name']}/{value['src_table_name']}"),
                    outlets=Dataset(
                        f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}")
                )

                update_des_table = SQLExecuteQueryOperator(
                    task_id=f"update_des_table_{key}",
                    sql=f"/sql/cms/update_des_table/{key}.sql",
                    conn_id=value['staging_conn_id'],
                    params={
                        'des_schema_name': value['des_schema_name'],
                        'des_table_name': value['des_table_name']
                    },
                    autocommit=True,
                    pool="cms_pool",
                    inlets=Dataset(
                        f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}"),
                    outlets=Dataset(
                        f"postgres://ods_database/doisoatvnpost/{value['des_schema_name']}/{value['des_table_name']}")
                )

                check_table >> source_to_staging >> update_des_table

    start_task >> connection_check_tasks >> one_to_one_tasks
