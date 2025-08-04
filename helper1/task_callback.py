from airflow.hooks.base import BaseHook
from helper1.task_logger import TaskRunMetadata


def end_task_callback(context, key, value):
    value_list = context['ti'].xcom_pull(key=key)
    value_list = value_list if type(value_list) is list else []
    value_list.append({
                        'task_id': context['ti'].task_id,
                        'value'  : value
                        })
    context['ti'].xcom_push(key=key, value=value_list)
  
def sensor_fail_callback(context):
    task_metadata = TaskRunMetadata(context=context, 
                                        table_name=context,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
                                        
def end_task_callback_standard(context):
    # Wrapper cho end_task_callback để tương thích với các DAG cũ
    end_task_callback(context, key='failed_tasks', value='Task failed')
