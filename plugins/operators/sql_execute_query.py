# Standard library imports
import logging
# Airflow imports
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from helper1.task_logger import TaskRunMetadata

LOGGER = logging.getLogger(__name__)

class CustomSQLExecuteQueryOperator(SQLExecuteQueryOperator):
    def __init__(self, des_schema_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.des_schema_name = des_schema_name

    def pre_execute(self, context):
        super().pre_execute(context)
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
    
    def execute(self, context):
        LOGGER.info("{} - execute() - START execute sql query".format(self.task_id.split(".")[-1]))
        
        try:
            rs = super().execute(context)
            self.task_metadata.write_result(result=f"Affected {rs} rows", is_success=True)
            LOGGER.info("{} - execute() - SUCCESSFULLY execute sql query".format(self.task_id.split(".")[-1]))
        except Exception as query_error:
            self.task_metadata.write_result(result=f"Fail execute sql query with error {query_error}", is_success=True)
            LOGGER.error("{} - execute() - FAIL execute sql query with error {}".format(self.task_id.split(".")[-1], query_error))
            raise
