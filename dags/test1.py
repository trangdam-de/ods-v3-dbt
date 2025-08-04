from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.exceptions import AirflowException
from typing import Any, Dict
from airflow import DAG
from airflow.utils.dates import days_ago

class CustomSQLOperator(BaseOperator):
    template_fields = ("sql")  # Enable Jinja rendering

    def __init__(
        self,
        *,
        sql: str,
        params: Dict[str, Any] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.params = params or {}

    def execute(self, context: Context):

        # Render template
        self.log.info("Rendering SQL template with params: %s", self.params)
        rendered_sql = self.sql.format(**self.params)

        self.log.info("Executing SQL: %s", rendered_sql)
        try:
            print("sjfajf")
        except Exception as e:
            raise AirflowException(f"SQL execution failed: {e}")

# Define DAG
dag = DAG(
    "example_custom_sql",
    schedule_interval="* * * * *",
    start_date=days_ago(1),
    catchup=False
)

sql_template = """
{% if dag_run.run_type == 'scheduled' %}
    {% set result = "Admin Panel" %}
{% else %}
    {% set result = "User Dashboard" %}
{% endif %}
INSERT INTO my_table (id, name, created_at)
VALUES ({{ params.id }}, {{ dag_run.run_type }}, {{ result }});
"""

info = {"id": 1, "name": "John Doe", "created_at": "2022-01-01"}

insert_data = CustomSQLOperator(
    task_id="insert_data",
    sql=sql_template,
    params={
        "id": info["id"],
        "name": info["name"],
        "created_at": info.get("created_at")
        },
    dag=dag
)
