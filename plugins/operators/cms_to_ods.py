# Standard library imports
import logging
from typing import Any, ClassVar, Sequence

# Third-party library imports
import dask
import pandas as pd
import psycopg
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
# Airflow imports
from airflow.utils.decorators import apply_defaults
from helper1.log_constants import LogConstants
from helper1.task_logger import TaskRunMetadata
from plugins.hooks.custom_thick_mode import ThickModeOracleHook
from psycopg import sql
from soda.scan import Scan

LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class CMSToStagingDailyOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql")
    template_ext: Sequence[str] = (".sql")
    template_fields_renderers: ClassVar[dict] = {"sql": "sql"}
    ui_color = "#cdaaed"

    def __init__(self,
                 cms_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 columns: dict,
                 sql_skip: int,
                 sql: str="",
                 cursor_field: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oracle_hook = None
        self.stg_connection = None
        self.task_metadata = None
        self.sql = sql
        self.cms_conn_id = cms_conn_id
        self.staging_conn_id = staging_conn_id
        self.src_schema_name = src_schema_name
        self.src_table_name = src_table_name
        self.cursor_field = cursor_field
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = columns
        self.sql_skip = sql_skip
        self.task = self.task_id.split(".")[-1]
        self.start_time = None
        self.end_time = None

    def pre_execute(self, context):
        LOGGER.info(LogConstants.INFO_PRE_EXECUTE_START.format(task_id=self.task))
        self.task_metadata = TaskRunMetadata(context=context,
                                             table_name=self.des_schema_name,
                                             result_connection=BaseHook.get_connection(
                                                 "staging_conn_id").get_uri())
        
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
                self.end_time = base_date.add(days=1).strftime('%Y-%m-%d %H:%M:%S')
                self.start_time = base_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            self.start_time = context["dag_run"].conf.get("start_time")
            self.end_time = context["dag_run"].conf.get("end_time")
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
        
        try:
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
            self.oracle_hook = ThickModeOracleHook(oracle_conn_id=self.cms_conn_id)
            self.execute_truncate()
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY destination connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.task_metadata.write_result(result="ERROR when creating destination connection", is_success=False)
            LOGGER.error("{} - pre_execute() - ERROR when creating destination connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)
            raise
        if self.sql_skip:
            if (self.des_table_name == 'contracts' or self.des_table_name =='customers' or self.des_table_name =='customer_relationship' ) and self.sql_skip == 1:
        # Không c?ng thêm condition
                pass
            else:
                self.sql = self.sql + self.get_condition(self.cursor_field)
        else:
            self.sql = self.get_extract_query(list(self.columns.keys()), self.cursor_field)

        LOGGER.info(LogConstants.INFO_PRE_EXECUTE_END.format(task_id=self.task))

    def execute(self,
                context):
        LOGGER.info(LogConstants.INFO_EXECUTE_START.format(task_id=self.task))
        LOGGER.info(f"SQL =======================> {self.sql}")
        df = self.oracle_hook.get_pandas_df(self.sql)
        df.columns = df.columns.str.lower()
        for col,dtype in self.columns.items():
            if dtype == "str" and col.lower() in df.columns:
                df[col.lower()] = df[col.lower()].astype(str).str.replace('\x00','',regex=False)
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)

        # self.validate_schema(dataframe=df)
        self.load_data(dataframe=df)
        LOGGER.info(LogConstants.INFO_EXECUTE_END.format(task_id=self.task))

    def post_execute(self,
                     context,
                     result=None):
        LOGGER.info(LogConstants.INFO_POST_EXECUTE_START.format(task_id=self.task))
        self.stg_connection.close()
        self.task_metadata.write_result(
            result=result,
            is_success=True
        )
        LOGGER.info(LogConstants.INFO_POST_EXECUTE_END.format(task_id=self.task))

    def get_extract_query(self, 
                          columns,
                          cursor_field):
        """ Tạo query để lấy dữ liệu
            columns: các cột dữ liệu lấy về
            cursor_field: cột dùng để xác định điều kiện lấy dữ liệu
        """
        LOGGER.info("{} - get_extract_query() - START creating SQL select query".format(self.task_id.split(".")[-1]))
        # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        sql_query = f"""
                    SELECT {", ".join(map(lambda x: f'"{x}"', columns))}
                    FROM "{self.src_schema_name}"."{self.src_table_name}"
                    """
        codition = f"""
                    WHERE "{cursor_field}" >= TO_DATE('{self.start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    AND "{cursor_field}" < TO_DATE('{self.end_time}', 'YYYY-MM-DD HH24:MI:SS')
                    """ if cursor_field else ""
        sql_query = sql_query + codition
        LOGGER.info("{} - get_extract_query() - END creating SQL select query --> {}"
                    .format(self.task_id.split(".")[-1], sql_query))
        
        return sql_query

    def get_condition(self,
                      cursor_field):
        codition = f"""
                    WHERE C.CREATED_DATE >= TO_DATE('{self.start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    AND C.CREATED_DATE < TO_DATE('{self.end_time}', 'YYYY-MM-DD HH24:MI:SS')
                    """ 
        return codition

    def validate_schema(self,
                        dataframe):
        LOGGER.info(LogConstants.VALIDATE_SCHEMA_START.format(task_id=self.task))
        scan = Scan()
        try:
            scan.set_scan_definition_name(self.des_schema_name)
            scan.set_data_source_name(f"pandas")
            scan.add_pandas_dataframe(dataset_name=self.des_table_name, pandas_df=dataframe, data_source_name="pandas")
            scan.add_sodacl_yaml_file(f"/opt/airflow/soda/check/{self.des_schema_name}/{self.des_table_name}.yml")

            scan.set_verbose(True)  # Set format log
            scan.execute()

            scan.assert_no_error_logs()
            scan.assert_no_checks_fail()
            LOGGER.info(LogConstants.VALIDATE_SCHEMA_END.format(task_id=self.task))
        except Exception:
            self.task_metadata.write_result(
                result="Failed to validate schema",
                is_success=False
            )
            if scan.has_error_logs():
                LOGGER.error(LogConstants.VALIDATE_SCHEMA_ERROR
                             .format(task_id=self.task, details=scan.get_error_logs_text()),
                             exc_info=True)
                raise
            if scan.has_check_fails():
                LOGGER.error(LogConstants.VALIDATE_SCHEMA_ERROR
                             .format(task_id=self.task, details=scan.get_checks_fail_text()),
                             exc_info=True)
                raise

    def load_data(self,
                  dataframe):
        LOGGER.info(LogConstants.LOAD_DATA_START.format(task_id=self.task))

        try:
            columns = dataframe.columns.map(str.lower).tolist()

            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({})"
            ).format(
                sql.Identifier("staging", f"{self.des_schema_name}_{self.des_table_name}"),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns))
            )

            with self.stg_connection.cursor() as cur:
                cur.executemany(
                    insert_query,
                    dataframe.itertuples(index=False, name=None)
                )
            self.stg_connection.commit()
            LOGGER.info(LogConstants.LOAD_DATA_END.format(task_id=self.task))

        except psycopg.OperationalError as conn_error:
            self.task_metadata.write_result(
                result="Failed to establish database connection",
                is_success=False
            )
            LOGGER.error(LogConstants.LOAD_DATA_ERROR
                         .format(task_id=self.task, details=conn_error),
                         exc_info=True)
            raise RuntimeError(f"Database connection failed: {conn_error}")

        except Exception as e:
            self.task_metadata.write_result(
                result="Failed to load data to ods",
                is_success=False
            )
            LOGGER.error(LogConstants.LOAD_DATA_ERROR
                         .format(task_id=self.task, details=e),
                         exc_info=True)
            raise RuntimeError(f"Data loading failed: {e}")

    def execute_truncate(self):
        LOGGER.info("{task_id} - execute_truncate() START.".format(task_id=self.task))
        try:
            truncate_query = sql.SQL("TRUNCATE TABLE {};").format(
                sql.Identifier("staging", f"{self.des_schema_name}_{self.des_table_name}")
            )

            with self.stg_connection.cursor() as cur:
                cur.execute(truncate_query)
            self.stg_connection.commit()
            LOGGER.info(f"Truncate table staging.{self.des_table_name} completed successfully.")
            LOGGER.info("{task_id} - execute_truncate() END.".format(task_id=self.task))
        except Exception as e:
            LOGGER.error(f"Failed to truncate table staging.{self.des_table_name}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to truncate table: {e}")
