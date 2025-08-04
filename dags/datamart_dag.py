from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27)
}

with DAG(
        'datamart_dag',
        default_args=default_args,
        schedule_interval=None,
) as dag:
    start_task = DummyOperator(task_id='start_task')

    update_datamart = SQLExecuteQueryOperator(
        task_id="update_datamart",
        sql="/sql/pns/update_des_table/update_datamart.sql",
        conn_id='datamart_conn_id',
        autocommit=True,
        pool="pns_pool",
        # inlets=Dataset(
        # f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}"),
        # outlets=Dataset(
        #     f"postgres://ods_database/doisoatvnpost/{value['des_schema_name']}/{value['des_table_name']}")
    )

    end_task = DummyOperator(task_id='end_task', trigger_rule=TriggerRule.ALL_DONE)

    start_task >> update_datamart >> end_task
