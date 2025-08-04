def get_xcom_value(**context):
  results = context['ti'].xcom_pull(key='Result')
  for task_result in results:
      print(f"Task result: {task_result}")