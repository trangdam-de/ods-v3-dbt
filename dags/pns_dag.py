import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper1.get_xcom_value import get_xcom_value
from plugins.operators.ftp_downloader import SFTPToMinIOOperator
from plugins.operators.pns_to_ods_optimize import PNSToStagingDailyOperator
from airflow.models import Variable

#status_date = None

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
}

with open("pipeline_params/pns.json", "r") as file:
    pipeline_params = json.load(file)

with DAG(
        'pns_dag',
        default_args=default_args,
        schedule_interval="0 23 * * *",
        catchup=True,
        max_active_runs=1,
) as dag:
    start_task = DummyOperator(task_id='start_task')
    #status_date = "{{ dag_run.conf.get('acceptance_date', ds ~ ' 00:00:00') }}"
    # file_date = (datetime.now() - timedelta(days=1)).strftime('%d%m%Y')
    #file_dates = ['02032025']

    with TaskGroup(group_id="file_download_tasks") as file_download_tasks:
        for key, value in pipeline_params.items():
            type = value['type']
            if type != 'detail':
                continue
            # sftp_remote_path = (f"{value['sftp_remote_path']}{file_date}.xlsx" if type == 'detail' else value['sftp_remote_path'])
            file_download = SFTPToMinIOOperator(
                task_id=f'download_{key}',
                sftp_conn_id='sftp_server_conn_id',
                sftp_remote_path=value['sftp_remote_path'],
                minio_conn_id='minio_ftp_conn_id',
                minio_bucket_name='pns',
                minio_key=value['minio_key'],
                type=value['type']
            )
    def should_process_table(table_name, **context):
        tables_to_process = ["item_collection_detail", "item_delivery_detail"]
        run_type = context["dag_run"].run_type
        if not tables_to_process:
            return True
        return table_name in tables_to_process

    def get_status_date(**context):
        #global status_date
        if context["dag_run"].run_type == "scheduled":
            status_date = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y%m%d')
            #status_date = (current_date + timedelta(days=-2)).strftime('%Y%m%d')
        else:
            start_time = context["dag_run"].conf.get("start_time")
            status_date = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')
        return status_date

    def get_acceptance_date(**context):
        if context["dag_run"].run_type == "scheduled":
            acceptance_date = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
        else: 
            start_time = context["dag_run"].conf.get("start_time")
            acceptance_date = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        return acceptance_date

    def execute_sql(key, **context):
        
        value = pipeline_params[key]
        des_schema_name = value['des_schema_name']
        des_table_name = value['des_table_name']

        if des_table_name == "item_collection_detail":
            acceptance_date = context['ti'].xcom_pull(task_ids = f"one_to_one_tasks.{key}.get_acceptance_date", key = 'return_value')
            sql = f"""
                    DELETE FROM {des_schema_name}.{des_table_name} des
                    WHERE DATE(des.acceptance_date) = DATE('{acceptance_date}');

                    INSERT INTO {des_schema_name}.{des_table_name}
                    SELECT * FROM staging.{des_schema_name}_{des_table_name} as src
                    WHERE DATE(src.acceptance_date) = DATE('{acceptance_date}');
                   """
        elif des_table_name == "item_delivery_detail":
            status_date = context['ti'].xcom_pull(task_ids = f"one_to_one_tasks.{key}.get_status_date", key = 'return_value')
            sql = f"""
                    DELETE FROM {des_schema_name}.{des_table_name} des
                    WHERE des.status_date = '{status_date}';

                    INSERT INTO {des_schema_name}.{des_table_name}
                    SELECT * FROM staging.{des_schema_name}_{des_table_name} as src
                    WHERE src.status_date = '{status_date}';
                   """
        
        try:
            pg_hook = PostgresHook(postgres_conn_id = value['staging_conn_id'])
        except Exception as conn_error:
            print(f"Can't connect staging_conn_id {str(conn_error)}")

        pg_hook.run(sql)
        return f"Successfully executed SQL for {key}"

    with TaskGroup(group_id="one_to_one_tasks") as one_to_one_tasks:
        for key, value in pipeline_params.items():
            type = value['type']
            if type != 'detail':
                continue
            with TaskGroup(group_id=key) as one_to_one_task:
                check_table = ShortCircuitOperator(
                    task_id = f"check_{key}",
                    python_callable = should_process_table,
                    op_kwargs = {"table_name": key},
                )
 
                source_to_staging = PNSToStagingDailyOperator(
                    task_id=f"source_to_staging_{key}",
                    staging_conn_id=value['staging_conn_id'],
                    minio_conn_id='minio_ftp_conn_id',
                    minio_bucket_name='pns',
                    des_schema_name=value['des_schema_name'],
                    des_table_name=value['des_table_name'],
                    file_prefix=value['file_prefix'],
                    #file_dates=file_dates,
                    end_skip=value['end_skip'],
                    type=value['type'],
                    minio_key=value['minio_key'],
                    header_rows=value['header_rows'],
                    columns=value['columns'],
                    pool="pns_pool",
                    inlets=Dataset(f"s3://pns/{value['minio_key']}"),
                    outlets=Dataset(
                        f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}")
                )

                if key == "item_collection_detail":
                    get_acceptance_date_ = PythonOperator(
                        task_id = "get_acceptance_date",
                        python_callable = get_acceptance_date,
                        provide_context = True
                    )
                    execute_sql_ = PythonOperator(
                        task_id = f"execute_sql_{key}",
                        python_callable = execute_sql,
                        op_kwargs = {'key': key},
                        provide_context = True
                    )
                #    update_des_table = SQLExecuteQueryOperator(
                #        task_id=f"update_des_table_{key}",
                #        sql=f"/sql/pns/update_des_table/{key}.sql",
                #        conn_id=value['staging_conn_id'],
                #        params={
                #            'des_schema_name': value['des_schema_name'],
                #            'des_table_name': value['des_table_name'],
                #            #'status_date': status_date
                #        'status_date': f"{{ ti.xcom_pull(task_ids = 'one_to_one_tasks.{key}.get_status_date', key = 'return_value') }}"
                #        #'acceptance_date': "{{ dag_run.conf.get('start_time', ds + ' 00:00:00') }}",
                #        #'status_date': "{{ dag_run.conf.get('start_time', ds) | replace('-', '') }}"
                #        },
                #        autocommit=True,
                #        pool="pns_pool",
                #        inlets=Dataset(
                #            f"postgres://ods_database/doisoatvnpost/staging/{value['des_schema_name']}_{value['des_table_name']}"),
                #        outlets=Dataset(
                #            f"postgres://ods_database/doisoatvnpost/{value['des_schema_name']}/{value['des_table_name']}")
                #    )
                    check_table >> get_acceptance_date_ >> source_to_staging >> execute_sql_
                elif key == "item_delivery_detail":    
                    
                    get_status_date_ = PythonOperator(
                        task_id = "get_status_date",
                        python_callable = get_status_date,
                        provide_context = True
                    )
                    execute_sql_ = PythonOperator(
                        task_id = f"execute_sql_{key}",
                        python_callable = execute_sql,
                        op_kwargs = {'key': key},
                        provide_context = True
                    )

                    check_table >> get_status_date_ >> source_to_staging  >> execute_sql_

    end_task = DummyOperator(task_id='end_task', trigger_rule=TriggerRule.ALL_DONE)

    start_task >> file_download_tasks >> one_to_one_tasks >> end_task
