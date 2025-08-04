# Standard library imports
import datetime
from datetime import date, datetime, timedelta
import logging
import os
import re
import sys
import time
import traceback
from typing import Any, ClassVar, Sequence

# Third-party library imports
import pandas as pd
import dask
import dask.dataframe as dd
import pyodbc
from psycopg import connect, sql
import psycopg
from soda.scan import Scan
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine

# Airflow imports
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Custom helper import
from helper1.task_logger import TaskRunMetadata
from helper1.optimize_compute import get_optimal_settings, get_total_rows, create_temp_dir

LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class BCCPToStagingDailyOperator(BaseOperator):
    # template_fields = ('prefix', 'src_bucket_name')
    template_fields: Sequence[str] = ("sql","parameters")
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers: ClassVar[dict] = {"sql": "sql", "parameters": "json"}
    ui_color = "#cdaaed"
    
    def __init__(self,
                 bccp_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 middle_storage_conn_id: str,
                 bucket: str,
                 order_by: list,
                 cursor_field: str = None,
                 columns: dict = "*",
                 sql: str = "",
                 parameters: dict = {},
                 start_time: str = None,
                 end_time: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mssql_hook = None
        self.stg_connection = None
        self.bccp_connection = None
        self.bccp_conn_id = bccp_conn_id
        self.staging_conn_id = staging_conn_id
        self.src_schema_name = src_schema_name
        self.src_table_name = src_table_name
        self.cursor_field = cursor_field
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = columns
        self.order_by = order_by
        self.sql = sql
        self.parameters = parameters
        self.start_time = None
        self.end_time = None
        self.middle_storage_conn_id = middle_storage_conn_id
        self.bucket = bucket

    def pre_execute(self, context):
    # Hàm chạy sau khi khởi tạo Operator
    # Hàm khởi tạo cho các biến
    ## context: context của task trong airflow
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
        
        # base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        
        if context["dag_run"].run_type == "scheduled":
            self.end_time = context['data_interval_end'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
            self.start_time = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
        else:
            self.start_time = context["dag_run"].conf.get("start_time")
            self.end_time = context["dag_run"].conf.get("end_time")
            self.bucket = "manual"
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
            # self.end_time = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
            # self.start_time = (context['logical_date'].in_tz('Asia/Ho_Chi_Minh') - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            
        try:
            self.s3_hook = S3Hook(aws_conn_id=self.middle_storage_conn_id)
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY middle storage connections established".format(self.task_id.split(".")[-1]))
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY destination connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.task_metadata.write_result(result="ERROR when creating destination connection", is_success=False)
            LOGGER.error("{} - pre_execute() - ERROR when creating destination connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)
            raise
        
        try:
            mssql_hook = MsSqlHook(mssql_conn_id=self.bccp_conn_id)
            conn = mssql_hook.get_connection(self.bccp_conn_id)
            server = conn.host
            database = conn.schema
            username = conn.login
            password = conn.password
            driver = "ODBC Driver 18 for SQL Server" 
            port = conn.port or 1433
            self.connection_string = f"""
                DRIVER={{{driver}}};
                SERVER={server},{port};
                DATABASE={database};
                UID={username};
                PWD={password};
                TrustServerCertificate=yes;
            """
            self.bccp_connection = MsSqlHook(mssql_conn_id=self.bccp_conn_id).get_sqlalchemy_connection()
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY source connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.task_metadata.write_result(result="ERROR when creating source connection", is_success=False)
            LOGGER.error("{} - pre_execute() - ERROR when creating source connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)
            raise
        
    def execute(self,
                context):
    # Hàm chạy sau hàm pre_execute
    ## context: context của task trong airflow
        LOGGER.info("{} - execute() - START execute extracting, validating and loading data".format(self.task_id.split(".")[-1]))
        total_rows = get_total_rows(self.bccp_connection,
                                    self.src_schema_name,
                                    self.src_table_name,
                                    self.cursor_field,
                                    self.start_time,
                                    self.end_time,
                                    type='mssql')
        print(f"total_rows:{total_rows}")
        sql_query = self.get_extract_query(list(self.columns.keys()),\
                                                    self.cursor_field)
        batch_counter = self.extract(sql_query=sql_query, 
                                     columns_type=self.columns,
                                     des_table_name=self.des_table_name,
                                     des_schema_name=self.des_schema_name)
        self.truncate_staging_table(connection=self.stg_connection,
                                    des_schema_name=self.des_schema_name,
                                    des_table_name=self.des_table_name)  
        rs = self.load_data(batch_counter, 
                            des_table_name=self.des_table_name,
                            des_schema_name=self.des_schema_name)
        LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
        return f"Successfully load {rs} rows from {self.src_schema_name}.{self.src_table_name} to {self.des_schema_name}.{self.des_table_name}"
 
    def post_execute(self,
                     context,
                     result=None):
    # Hàm chạy cuối cùng khi kết thúc Operator
    ## context: context của task trong airflow
    ## result: result được trả từ hàm execute
    # Trong Operator này, result không được trả -> set mặc định là None
        self.stg_connection.close()
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id.split(".")[-1]))
        self.task_metadata.write_result(result=result, is_success=True)
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id.split(".")[-1]))

    def get_extract_query(self, 
                          columns,
                          cursor_field,
                          order_by = None,
                          offset = None,
                          chunk_size = None):
    
        LOGGER.info("{} - get_extract_query() - START creating SQL select query".format(self.task_id.split(".")[-1]))

        sql_query = f"""
                    SELECT {", ".join(map(lambda x: f'"{x}"', columns))}
                    FROM {self.src_schema_name}.{self.src_table_name}
                    """
        codition = f"""
                    WHERE {cursor_field} >= CONVERT(DATETIME, '{self.start_time}', 120)
                    AND {cursor_field} < CONVERT(DATETIME, '{self.end_time}', 120)
                    """ if cursor_field else ""
        # chunk_query = f"""
        #             ORDER BY {", ".join(order_by)}
        #             OFFSET {offset} ROWS
        #             FETCH NEXT {chunk_size} ROWS ONLY
        #             """ if chunk_size else """"""
        sql_query = sql_query + codition
        LOGGER.info("{} - get_extract_query() - END creating SQL select query --> {}"
                    .format(self.task_id.split(".")[-1], sql_query))
        return sql_query
      
    def extract(self,
                sql_query,
                columns_type,
                des_table_name,
                des_schema_name,
                batch_size=50000):
        try:
            def to_snake_case(name: str) -> str:
                name = re.sub(r'([0-9])([A-Z])', r'\1_\2', name)
                name = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', name)
                name = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', name)
                return name.lower()
            LOGGER.info("{} - extract() - START fetching Dataframe, validate source data".format(self.task_id.split(".")[-1]))
            # print(type(self.bccp_connection))
            pyodbc_conn = pyodbc.connect(self.connection_string)
            cursor = pyodbc_conn.cursor()
            LOGGER.info("{} - execute() - START querying with query".format(self.task_id.split(".")[-1], sql_query))
            cursor.execute(sql_query)
            columns = [column[0] for column in cursor.description]
            batch_counter = 0
            total_row_extracted = 0
            while True:
                rows = cursor.fetchmany(batch_size)
                print(batch_counter)
                if not rows:
                    break
                rows = [list(row) for row in rows]  
                data = pd.DataFrame(rows, columns=columns)
                data = data.astype(columns_type)
                data.columns = [to_snake_case(col) for col in data.columns]
                data = data.astype(object)
                data = data.applymap(lambda x: x.replace('\r', '').replace('\x00', '') if isinstance(x, str) else x)
                data = data.where(pd.notnull(data), None)
                file_path = f"./data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv"
                data.to_csv(file_path, index=False, escapechar='\\')
                LOGGER.info("{} - extract() - Dataframe source data batch {} --> {}".format(self.task_id.split(".")[-1], batch_counter, data.head()))
                total_row_extracted += len(data)
                self.s3_hook.load_file(
                    file_path, 
                    f"{des_schema_name}/{des_table_name}/{re.sub(r'[:]', '-', self.start_time)}_{re.sub(r'[:]', '-', self.end_time)}/data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv", 
                    self.bucket,
                    replace = True
                )
                batch_counter += 1

            LOGGER.info("{} - extract() - TOTAL rows to CSV: {}".format(self.task_id.split(".")[-1], total_row_extracted))
            return batch_counter
        except Exception as extract_error:
            self.task_metadata.write_result(result="Fail while extracting", is_success=False)
            LOGGER.error("{} - extract() - ERROR when extracting --> {} "
                         .format(self.task_id.split(".")[-1].split(".")[-1], extract_error), exc_info=True)
            raise
    
    def validate_schema(self,
                        dataframe,
                        context):
    # Kiểm tra dữ liệu: đúng schema, đúng data type
    ## dataframe: pandas dataframe chứa dữ liệu để xử lý
    ## context: context của task trong airflow
    
        LOGGER.info("{} - validate_schema() - START validating source data".format(self.task_id.split(".")[-1]))
        scan = Scan()
        try:
            # Thêm config: nguồn dữ liệu
            scan.set_scan_definition_name(self.des_schema_name)
            scan.set_data_source_name("pandas")
            scan.add_pandas_dataframe(dataset_name=self.des_table_name, pandas_df=dataframe, data_source_name="pandas")
            scan.add_sodacl_yaml_file(f"/opt/airflow/soda/check/{self.des_schema_name}/{self.des_table_name}.yml")  # Thêm file check

            scan.set_verbose(True)  # Set format log
            scan.execute()  # Chạy check dữ liệu

            # Check liệu có lỗi hoặc check fail
            scan.assert_no_error_logs()
            scan.assert_no_checks_fail()
            LOGGER.info("{} - validate_schema() - END validating source data".format(self.task_id.split(".")[-1]))
        except Exception as e:
            if scan.has_error_logs():
                LOGGER.error("{} - validate_schema() - ERROR --> {} "
                             .format(self.task_id.split(".")[-1], scan.get_error_logs_text()))
                raise
            if scan.has_check_fails():
                LOGGER.error("{} - validate_schema() - ERROR --> {} "
                             .format(self.task_id.split(".")[-1], scan.get_checks_fail_text()))
                raise
    
    def load_data(self, 
                    batch_counter,
                    des_table_name,
                    des_schema_name):
    # Thêm dữ liệu vào bảng đích
    ## dataframe: pandas dataframe chứa dữ liệu để xử lý
    ## connect_uri: đường dẫn tới database đích
    ## context: context của task trong airflow

        LOGGER.info("{} - load_data() - START loading data to staging layer".format(self.task_id.split(".")[-1]))
        try:
            table_name = f"{des_schema_name}_{des_table_name}" if self.bucket != "manual" else f"{des_schema_name}_{des_table_name}_manual"
            with self.stg_connection.cursor() as cur:
                for bc in range(0, batch_counter):
                    file_path = f"./data_batch_{des_schema_name}_{des_table_name}_{bc}.csv"
                    
                    print(file_path)
                    with open(file_path, "r", encoding="utf-8") as f:
                        with cur.copy(
                            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier("staging", table_name))
                        ) as copy:
                            copy.write(f.read())

                self.stg_connection.commit()
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                                            sql.Identifier("staging", table_name)
                                        ))
                count = cur.fetchone()[0]
            LOGGER.info("{} - load_data() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
            return count
        except Exception as e:
            self.task_metadata.write_result(result="ERROR when loading data to staging layer", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when loading data to staging layer --> {} "
                        .format(self.task_id.split(".")[-1], e), exc_info=True)
            raise
        
    def truncate_staging_table(self, 
                               connection,
                               des_schema_name,
                               des_table_name):
        """
        """
        LOGGER.info("{} - truncate_staging_table() - START truncating staging table".format(self.task_id))
        try:
            table_name = f"{des_schema_name}_{des_table_name}" if self.bucket != "manual" else f"{des_schema_name}_{des_table_name}_manual"
            truncate_query = sql.SQL(
                "TRUNCATE TABLE {};"
            ).format(
                sql.Identifier("staging", table_name)
            )
            with connection.cursor() as cur:
                cur.execute(truncate_query)
            connection.commit()    
            LOGGER.info("{} - truncate_staging_table() - END truncating staging table".format(self.task_id))
        except Exception as e:
            self.task_metadata.write_result(result="ERROR while truncating staging table", is_success=False)
            LOGGER.error("{} - truncate_staging_table() - ERROR while truncating staging table --> {} "
                        .format(self.task_id, e), exc_info=True)
            raise

