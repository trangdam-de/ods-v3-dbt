# Standard library imports
import datetime
from datetime import date, datetime, timedelta
import logging
import os
import re
import sys
import traceback
from typing import Any

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

# Custom helper import
from helper1.task_logger import TaskRunMetadata
from helper1.optimize_compute import get_optimal_settings, get_total_rows, create_temp_dir

LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class BCCPToStagingDailyOperator(BaseOperator):
    # template_fields = ('prefix', 'src_bucket_name')
    @apply_defaults
    def __init__(self,
                 bccp_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 end_task_callback,
                 cursor_field: str = None,
                 columns: dict = "*",
                 sql: str = "",
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
        self.sql = sql
        self.end_task_callback = end_task_callback

    def pre_execute(self, context):
    # Hàm chạy sau khi khởi tạo Operator
    # Hàm khởi tạo cho các biến
    ## context: context của task trong airflow
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("result_connection_id").get_uri())

        try:
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY destination connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.end_task_callback(context=context,
                                key="Result",
                                value="Fail while connecting to destination database. Connection failed")
            LOGGER.error("{} - pre_execute() - ERROR when creating destination connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)
            raise
        
        try:
            self.bccp_connection = MsSqlHook(mssql_conn_id=self.bccp_conn_id).get_sqlalchemy_connection()
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY source connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.end_task_callback(context=context,
                                key="Result",
                                value="Fail while connecting to source database. Connection failed")
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
                                  self.cursor_field)
        if total_rows <= 50000:
            sql_query = self.get_extract_query(list(self.columns.keys()), self.cursor_field) if len(self.sql) == 0 else self.sql
            df = self.extract(context=context)
            result = self.load_data(dataframe=df, context=context) 
        else:
            temp_dir = create_temp_dir()
            chunk_size, processes = get_optimal_settings(total_rows)
            chunk_params = []
            for offset in range(0, total_rows, chunk_size):
                sql_query = self.get_extract_query(list(self.columns.keys()), \
                                                    self.cursor_field, \
                                                        offset=offset, \
                                                            chunk_size=chunk_size) \
                                if len(sql_query) == 0 else sql_query
                chunk_params.append((context, sql_query, offset, chunk_size, temp_dir)) 
            
            df = self.extract_all_chunks(context=context,\
                                            chunk_params=chunk_params, \
                                                processes=processes)
            
            result = self.load_data(dataframe=df, context=context)
        LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
        return f"Successfully load {result} rows from {self.src_schema_name}.{self.src_table_name} to {self.des_schema_name}.{self.des_table_name}"
        
    def post_execute(self,
                     context,
                     result=None):
    # Hàm chạy cuối cùng khi kết thúc Operator
    ## context: context của task trong airflow
    ## result: result được trả từ hàm execute
    # Trong Operator này, result không được trả -> set mặc định là None
        self.stg_connection.close()
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id.split(".")[-1]))
        self.end_task_callback(context=context,
                                key="Result",
                                value="Task succeed")
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id.split(".")[-1]))

    def get_extract_query(self, 
                          columns,
                          cursor_field,
                          offset =None,
                          chunk_size =None):
    # Tạo query để lấy dữ liệu
    ## columns: các cột dữ liệu lấy về
    ## cursor_field: cột dùng để xác định điều kiện lấy dữ liệu
    
        LOGGER.info("{} - get_extract_query() - START creating SQL select query".format(self.task_id.split(".")[-1]))
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        today = (datetime.now()).strftime('%Y-%m-%d')
        sql_query = f"""
                    SELECT {", ".join(map(lambda x: f'"{x}"', columns))}
                    FROM "{self.src_schema_name}"."{self.src_table_name}"
                    """
        codition_query = f"""
                    WHERE "{cursor_field}" >= TO_DATE('{yesterday}', 'YYYY-MM-DD')
                    AND "{cursor_field}" <= TO_DATE('{today}', 'YYYY-MM-DD')
                    """ if cursor_field else ""
        chunk_query = f"""
                    OFFSET {offset} ROWS
                    FETCH NEXT {chunk_size} ROWS ONLY
                    """ if offset and chunk_size else """"""
        # sql_query = sql_query + codition + chunk_query
        sql_query = sql_query + chunk_query
        LOGGER.info("{} - get_extract_query() - END creating SQL select query --> {}"
                    .format(self.task_id.split(".")[-1], sql_query))
        return sql_query
      
    def extract(self,
                    sql_query,
                    context):
        try:
            LOGGER.info("{} - extract() - START fetching Dataframe, validate source data".format(self.task_id.split(".")[-1]))

            df = pd.read_sql(sql_query, self.bccp_conn_id)
            df = df.astype(self.columns)
            def to_snake_case(name: str) -> str:
                name = re.sub(r'([0-9])([A-Z])', r'\1_\2', name)
                name = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', name)
                name = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', name)
                return name.lower()
            df.columns = [to_snake_case(col) for col in df.columns]
            LOGGER.info("{} - execute() - Dataframe source data --> {}".format(self.task_id.split(".")[-1], df.head()))

            self.validate_schema(dataframe=df,
                                    context=context)
            df = df.astype(object)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as extract_error:
            self.task_metadata.write_result(result="Fail while extracting", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when extracting --> {} "
                         .format(self.task_id.split(".")[-1].split(".")[-1], extract_error), exc_info=True)
            raise
          
    def extract_all_chunks(self, chunk_params, processes):    
        with ProcessPoolExecutor(max_workers=processes) as executor:
            futures = {executor.submit(self.extract, chunk_param) for chunk_param in chunk_params}
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as extract_error:
                    # self.task_metadata.write_result(result="Fail while extracting", is_success=False)
                    LOGGER.error("{} - extract() - ERROR extract data from source with those params {} --> {}"
                                .format(self.task_id, futures[future], extract_error))
            
            return results
      
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
            self.end_task_callback(context=context,
                                   key="Result",
                                   value="Fail while validating data")
            if scan.has_error_logs():
                LOGGER.error("{} - validate_schema() - ERROR --> {} "
                             .format(self.task_id.split(".")[-1], scan.get_error_logs_text()))
                raise
            if scan.has_check_fails():
                LOGGER.error("{} - validate_schema() - ERROR --> {} "
                             .format(self.task_id.split(".")[-1], scan.get_checks_fail_text()))
                raise
    
    def load_data(self, 
                  dataframe,
                  context):
    # Thêm dữ liệu vào bảng đích
    ## dataframe: pandas dataframe chứa dữ liệu để xử lý
    ## connect_uri: đường dẫn tới database đích
    ## context: context của task trong airflow

        LOGGER.info("{} - load_data() - START loading data to staging layer".format(self.task_id.split(".")[-1]))

        try:
            columns = dataframe.columns.map(str.lower).tolist()
            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({})"
            ).format(
                sql.Identifier("staging", f"{self.des_schema_name}_{self.des_table_name}"),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns))
            )
            
            truncate_query = sql.SQL(
                "TRUNCATE TABLE {};"
            ).format(
                sql.Identifier("staging", f"{self.des_schema_name}_{self.des_table_name}")
            )
            with self.stg_connection.cursor() as cur:
                cur.execute(truncate_query)
                cur.executemany(
                    insert_query,
                    dataframe.itertuples(index=False, name=None)
                )
            self.stg_connection.commit()
            LOGGER.info("{} - load_data() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
        except Exception as e:
            self.end_task_callback(context=context,
                                key="Result",
                                value="Fail while loading data")
            LOGGER.error("{} - load_data() - ERROR when loading data to staging layer --> {} "
                        .format(self.task_id.split(".")[-1], e), exc_info=True)
            raise
        
    def load_all_chunks(self, chunk_files, processes):
        load_params = [(f, self.stg_connection, self.des_schema_name, self.des_table_name, 'append' if i > 0 else if_exists)
                      for i, f in enumerate(chunk_files)]
        
        total_rows = 0
        with ProcessPoolExecutor(max_workers=processes) as executor:
            for rows_loaded in executor.map(self.load_data, load_params):
                total_rows += rows_loaded
        return total_rows