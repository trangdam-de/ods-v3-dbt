# Standard library imports
import logging
from typing import Any, ClassVar, Sequence
from datetime import datetime, timedelta

# Third-party library imports
import pandas as pd
import dask
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helper1.log_constants import LogConstants
from plugins.hooks.custom_thick_mode import ThickModeOracleHook
from soda.scan import Scan
from helper1.task_logger import TaskRunMetadata


LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class OdsToLlbOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers: ClassVar[dict] = {"sql": "sql"}
    ui_color = "#cdaaed"

    def __init__(self,
                 ods_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 columns: dict,
                 sql_skip: int,
                 sql: str = "",
                 cursor_field: str = None,
                 chunk_size: int = 10000,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ods_conn_id = ods_conn_id
        self.staging_conn_id = staging_conn_id
        self.src_schema_name = src_schema_name
        self.src_table_name = src_table_name
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = columns
        self.sql_skip = sql_skip
        self.sql = sql
        self.cursor_field = cursor_field
        self.chunk_size = chunk_size
        self.ods_hook = None
        self.stg_hook = None
        self.stg_connection = None
        self.task_metadata = None
        self.start_time = None
        self.end_time = None
        self.task = self.task_id.split(".")[-1]

    def pre_execute(self, context):
        LOGGER.info(LogConstants.INFO_PRE_EXECUTE_START.format(task_id=self.task))
        # Nếu có log Postgres, truyền vào, nếu không thì bỏ qua
        try:
            log_conn_id = 'log_postgres_conn_id'  # Đặt tên connection log Postgres ở Airflow
            log_uri = BaseHook.get_connection(log_conn_id).get_uri()
        except Exception:
            log_uri = None
        if log_uri:
            self.task_metadata = TaskRunMetadata(context=context,
                                                 table_name=self.des_schema_name,
                                                 result_connection=log_uri)
        else:
            self.task_metadata = None
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
            self.end_time = base_date.add(days=1).strftime('%Y-%m-%d')
            self.start_time = base_date.strftime('%Y-%m-%d')
        else:
            self.start_time = context["dag_run"].conf.get("start_time")
            self.end_time = context["dag_run"].conf.get("end_time")
            if not self.start_time or not self.end_time:
                LOGGER.error(f"{self.task} - pre_execute() - ERROR: start_time/end_time missing", exc_info=True)
                raise
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
            self.ods_hook = PostgresHook(postgres_conn_id=self.ods_conn_id)
            self.stg_hook = MsSqlHook(mssql_conn_id=self.staging_conn_id)
            self.execute_truncate()
            LOGGER.info(f"{self.task} - pre_execute() - SUCCESSFULLY destination connections established")
        except Exception as conn_error:
            self.task_metadata.write_result(result="ERROR when creating destination connection", is_success=False)
            LOGGER.error(f"{self.task} - pre_execute() - ERROR when creating destination connection --> {conn_error}", exc_info=True)
            raise
        if self.sql_skip:
            if not self.sql:
                # Tự động tìm và load file template nếu chưa truyền sql
                sql_path = f'dags/sql/ods_to_llb/extract/{self.des_table_name}.sql'
                import os
                abs_sql_path = os.path.abspath(sql_path)
                LOGGER.info(f"[DEBUG] Looking for SQL template at: {abs_sql_path}")
                if os.path.exists(sql_path):
                    with open(sql_path, 'r', encoding='utf-8') as f:
                        self.sql = f.read()
                    LOGGER.info(f"Loaded SQL template from {sql_path}")
                else:
                    LOGGER.error(f"sql_skip=1 nhưng không truyền sql template cho {self.task} và không tìm thấy file {abs_sql_path}")
                    raise ValueError(f"sql_skip=1 nhưng không truyền sql template cho {self.task} và không tìm thấy file {abs_sql_path}")
            # Render Jinja2 template
            import jinja2
            template = jinja2.Template(self.sql)
            self.sql = template.render(
                params={
                    "start_time": self.start_time,
                    "end_time": self.end_time
                }
            )
            # Không nối thêm WHERE nếu file đã có WHERE
        else:
            self.sql = self.get_extract_query(list(self.columns.keys()), self.cursor_field)
        LOGGER.info(f"Final SQL for {self.task}: {self.sql}")
        LOGGER.info(LogConstants.INFO_PRE_EXECUTE_END.format(task_id=self.task))

    def execute(self, context):
        LOGGER.info(LogConstants.INFO_EXECUTE_START.format(task_id=self.task))
        LOGGER.info(f"SQL =======================> {self.sql}")
        df = self.ods_hook.get_pandas_df(self.sql)
        df.columns = df.columns.str.lower()
        for col, dtype in self.columns.items():
            if dtype == "str" and col.lower() in df.columns:
                df[col.lower()] = df[col.lower()].astype(str).str.replace('\x00', '', regex=False)
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)
        self.load_data(df)
        LOGGER.info(LogConstants.INFO_EXECUTE_END.format(task_id=self.task))

    def post_execute(self, context, result=None):
        LOGGER.info(LogConstants.INFO_POST_EXECUTE_START.format(task_id=self.task))
        if self.task_metadata:
            try:
                self.task_metadata.write_result(result=result, is_success=True)
            except Exception as e:
                LOGGER.warning(f"Logging to metadata DB failed: {e}")
        LOGGER.info(LogConstants.INFO_POST_EXECUTE_END.format(task_id=self.task))

    def get_extract_query(self, columns, cursor_field):
        LOGGER.info(f"{self.task} - get_extract_query() - START creating SQL select query")
        sql_query = f"""
            SELECT {', '.join([f'"{x}"' for x in columns])}
            FROM "{self.src_schema_name}"."{self.src_table_name}"
        """
        condition = f"""
            WHERE "{cursor_field}" >= '{self.start_time}'
            AND "{cursor_field}" < '{self.end_time}'
        """ if cursor_field else ""
        sql_query = sql_query + condition
        LOGGER.info(f"{self.task} - get_extract_query() - END creating SQL select query --> {sql_query}")
        return sql_query

    def get_condition(self, cursor_field):
        return f"""
            WHERE {cursor_field} >= '{self.start_time}'
            AND {cursor_field} < '{self.end_time}'
        """

    def load_data(self, dataframe):
        LOGGER.info(f"{self.task} - load_data() - START.")
        if dataframe.empty:
            LOGGER.info(f"{self.task} - DataFrame is empty, skip insert.")
            return
        columns = dataframe.columns.map(str.lower).tolist()
        for col, dtype in self.columns.items():
            col_lower = col.lower()
            if col_lower in dataframe.columns:
                if dtype.lower() in ["int", "int64", "bigint"]:
                    dataframe[col_lower] = pd.to_numeric(dataframe[col_lower], errors='coerce').fillna(0).astype(int)
                elif dtype.lower() in ["float", "float64"]:
                    dataframe[col_lower] = pd.to_numeric(dataframe[col_lower], errors='coerce').astype('float64')
                elif dtype.lower() in ["str", "string"]:
                    dataframe[col_lower] = dataframe[col_lower].astype(str).str.replace('\x00','',regex=False)
        dataframe = dataframe.astype(object)
        dataframe = dataframe.where(pd.notnull(dataframe), None)
        self.stg_hook.insert_rows(
            table=f"{self.des_schema_name}.{self.des_table_name}",
            rows=dataframe.values.tolist(),
            target_fields=columns,
            commit_every=self.chunk_size
        )
        LOGGER.info(LogConstants.LOAD_DATA_END.format(task_id=self.task))

    def execute_truncate(self):
        LOGGER.info(f"{self.task} - execute_truncate() START.")
        truncate_query = f"TRUNCATE TABLE {self.des_schema_name}.{self.des_table_name}"
        self.stg_hook.run(truncate_query)
        LOGGER.info(f"Truncate table {self.des_schema_name}.{self.des_table_name} completed successfully.")
        LOGGER.info(f"{self.task} - execute_truncate() END.")