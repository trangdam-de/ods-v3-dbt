"""
Sample Airflow DAG integrating with DBT
This demonstrates how to replace SQLExecuteQueryOperator with DbtRunOperator
"""

import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

# DBT Operator (you'll need to install airflow-dbt package)
# pip install airflow-dbt
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'casreport_dbt_dag',
    default_args=default_args,
    description='CASREPORT ETL pipeline using DBT',
    schedule_interval='30 0 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'casreport', 'etl']
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    # Connection checks (same as before)
    with TaskGroup(group_id='connection_check_tasks') as connection_check_tasks:
        src_connect_check = SqlSensor(
            task_id='src_connect_check',
            conn_id='casreport_conn_id',
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

    # DBT workflow
    with TaskGroup(group_id='dbt_transformation_tasks') as dbt_tasks:
        
        # Install DBT dependencies
        dbt_deps = DbtRunOperator(
            task_id='dbt_deps',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            command='deps',
            target='production'
        )

        # Run staging models for CASREPORT
        dbt_staging = DbtRunOperator(
            task_id='dbt_run_staging_casreport',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            command='run',
            models='staging.casreport',
            target='production'
        )

        # Test staging models
        dbt_test_staging = DbtTestOperator(
            task_id='dbt_test_staging_casreport',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            models='staging.casreport',
            target='production'
        )

        # Run marts models for CASREPORT
        dbt_marts = DbtRunOperator(
            task_id='dbt_run_marts_casreport',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            command='run',
            models='marts.casreport',
            target='production'
        )

        # Test marts models
        dbt_test_marts = DbtTestOperator(
            task_id='dbt_test_marts_casreport',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            models='marts.casreport',
            target='production'
        )

        # Generate documentation
        dbt_docs = DbtRunOperator(
            task_id='dbt_generate_docs',
            dir='/usr/app/dbt',
            profiles_dir='/root/.dbt',
            command='docs generate',
            target='production'
        )

        # Set dependencies within DBT task group
        dbt_deps >> dbt_staging >> dbt_test_staging >> dbt_marts >> dbt_test_marts >> dbt_docs

    end_task = EmptyOperator(task_id='end_task')

    # Set overall DAG dependencies
    start_task >> connection_check_tasks >> dbt_tasks >> end_task
