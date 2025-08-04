# Standard library imports
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import datetime
from datetime import date, datetime, timedelta
import logging
import re
import sys
import traceback
from typing import Any
import csv

# Third-party library imports
import pandas as pd
import dask
import dask.dataframe as dd
from psycopg import connect, sql
import psycopg
from soda.scan import Scan
import cx_Oracle

# Airflow imports
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from helper1.task_logger import TaskRunMetadata
from helper1.optimize_compute import get_optimal_settings, get_total_rows, create_temp_dir


LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class KHLToStagingDailyOperator(BaseOperator):
    # template_fields = ('prefix', 'src_bucket_name')
    def __init__(self,
                 khl_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 middle_storage_conn_id: str,
                 bucket: str,
                #  start_time,
                #  end_time,
                 cursor_field: str = None,
                 columns: dict = "*",
                 sql: str = "",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oracle_hook = None
        self.stg_connection = None
        self.khl_connection = None
        self.khl_conn_id = khl_conn_id
        self.staging_conn_id = staging_conn_id
        self.src_schema_name = src_schema_name
        self.src_table_name = src_table_name
        self.cursor_field = cursor_field
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = columns
        self.start_time = None
        self.end_time = None
        self.sql = sql
        self.middle_storage_conn_id = middle_storage_conn_id
        self.bucket = bucket

    def pre_execute(self, context):
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
        
        logical_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh')
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
            #hour = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').hour
            self.start_time = context['data_interval_start'].start_of('day').in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
            self.end_time = context['data_interval_end'].start_of('day').in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
            #if not (hour >= 21 or hour <= 9):
                #LOGGER.info(f"{self.task_id} - pre_execute() - SKIPPED due to start_time hour = {hour}")
                #raise AirflowSkipException("Skip task because hour not in allowed time range")
        else:
            self.start_time = context["dag_run"].conf.get("start_time")
            self.end_time = context["dag_run"].conf.get("end_time")
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
                # self.end_time = base_date.strftime('%Y-%m-%d %H:%M:%S')
                # self.start_time = base_date.subtract(days=1).strftime('%Y-%m-%d %H:%M:%S')
        
        LOGGER.info("{} - pre_execute() - START establishing source and staging connection".format(self.task_id.split(".")[-1]))
        try:
            self.s3_hook = S3Hook(aws_conn_id=self.middle_storage_conn_id)
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY middle storage connections established".format(self.task_id.split(".")[-1]))
            self.khl_connection = OracleHook(oracle_conn_id=self.khl_conn_id)
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY source connections established".format(self.task_id.split(".")[-1]))
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY destination connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.task_metadata.write_result(result="Fail in pre_execute()", is_success=False)
            LOGGER.error("{} - pre_execute() - ERROR when creating connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)
            raise
        # self.sql = self.get_extract_query(list(self.columns.keys()), self.cursor_field) if len(self.sql) == 0 else self.sql
        LOGGER.info("{} - pre_execute() - END connections established".format(self.task_id.split(".")[-1]))

    def execute(self,
                context):
    # Hàm chạy sau hàm pre_execute
    ## context: context của task trong airflow
    
        LOGGER.info("{} - execute() - START execute extracting, validating and loading data".format(self.task_id.split(".")[-1]))
        total_rows = get_total_rows(self.khl_connection,
                                    self.src_schema_name,
                                    self.src_table_name,
                                    self.cursor_field,
                                    self.start_time,
                                    self.end_time,
                                    type='oracle')
        # chunk_size, processes = get_optimal_settings(total_rows)
        self.truncate_staging_table(connection=self.stg_connection,
                                    des_schema_name=self.des_schema_name,
                                    des_table_name=self.des_table_name)
        row_len = 0
        sql_query = self.get_extract_query(list(self.columns.keys()),\
                                            self.cursor_field)
        batch_counter = self.extract(sql=sql_query, 
                                     columns_type=self.columns,
                                     des_table_name=self.des_table_name,
                                     des_schema_name=self.des_schema_name)
        rs = self.load_data(batch_counter, 
                            des_table_name=self.des_table_name,
                            des_schema_name=self.des_schema_name)
            
        LOGGER.info(f"Successfully load {rs} rows from {self.src_schema_name}.{self.src_table_name} to {self.des_schema_name}.{self.des_table_name}")
        LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
        return f"Successfully load {rs} rows from {self.src_schema_name}.{self.src_table_name} to {self.des_schema_name}.{self.des_table_name}"

    def post_execute(self,
                     context,
                     result=None):
    # Hàm chạy cuối cùng khi kết thúc Operator
    ## context: context của task trong airflow
    ## result: result được trả từ hàm execute
    
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id.split(".")[-1]))
        self.task_metadata.write_result(result=result, is_success=True)
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id.split(".")[-1]))

    def get_oracle_connection(self, connection_id):
        conn = BaseHook.get_connection(connection_id)
        host = conn.host
        port = conn.port or 1521  # Default Oracle port
        service_name = conn.extra_dejson.get('service_name')
        user = conn.login
        password = conn.password
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        return connection 

    def extract(self,
                sql,
                columns_type,
                des_table_name,
                des_schema_name,
                batch_size=50000):
        LOGGER.info("{} - extract() - START fetching Dataframe, validate source data".format(self.task_id.split(".")[-1]))
        try:
            connection = self.get_oracle_connection(self.khl_conn_id)
            cursor = connection.cursor()
            # df = self.khl_connection.get_pandas_df(sql)
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            batch_counter = 0
            while True:
                rows = cursor.fetchmany(batch_size)
                print(type(rows))
                print(batch_counter)
                if not rows:
                    break
                data = pd.DataFrame(rows, columns=columns)
                data = data.astype(columns_type)
                data.columns = data.columns.str.lower()
                data = data.astype(object)
                data = data.applymap(lambda x: x.replace('\r', '').replace('\\', '/').replace('\x00', '') if isinstance(x, str) else x)
                data = data.where(pd.notnull(data), None)

                #for col in data.columns:
                #    for val in data[col]:
                #        if isinstance(val, str) and ('\\' in val):
                #            LOGGER.info(f"Field contains special characters in column {col}: {val}")

                file_path = f"./data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv"
                data.to_csv(file_path, index=False, quoting=csv.QUOTE_MINIMAL, escapechar='\\' )
                self.s3_hook.load_file(
                    file_path, 
                    f"{des_schema_name}/{des_table_name}/{re.sub(r'[:]', '-', self.start_time)}_{re.sub(r'[:]', '-', self.end_time)}/data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv", 
                    self.bucket,
                    replace = True
                )
                LOGGER.info("{} - extract() - Dataframe source data batch {} --> {}".format(self.task_id.split(".")[-1], batch_counter, data.head()))
                batch_counter += 1
            # LOGGER.info("{} - extract() - Dataframe source data --> {}".format(self.task_id.split(".")[-1], df.head()))
            return batch_counter
        except Exception as extract_error:
            self.task_metadata.write_result(result="Fail while extracting", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when extracting --> {} "
                         .format(self.task_id.split(".")[-1], extract_error), exc_info=True)
            raise
    
    def get_extract_query(self, 
                          columns,
                          cursor_field,
                          offset = None,
                          chunk_size = None):

        LOGGER.info("{} - get_extract_query() - START creating SQL select query".format(self.task_id.split(".")[-1]))
        # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        selected_columns = []
        for col in columns:
            if col == "DOCUMENT_PROVIDE_DATE":
                selected_columns.append('CASE WHEN "DOCUMENT_PROVIDE_DATE" < TO_DATE(\'1700-01-01 00:00:00\',\'YYYY-MM-DD HH24:MI:SS\') THEN NULL ELSE "DOCUMENT_PROVIDE_DATE" END AS "DOCUMENT_PROVIDE_DATE"')
            elif col == "CUSTOMER_EMAIL":
                selected_columns.append('NULL AS "CUSTOMER_EMAIL"')
            elif col == "CUSTOMER_ADD":
                selected_columns.append('NULL AS "CUSTOMER_ADD"')
            elif col == "DOCUMENT_NUMBER":
                 selected_columns.append('NULL AS "DOCUMENT_NUMBER"')
            elif col == "CUSTOMER_PHONE":
                 selected_columns.append('NULL AS "CUSTOMER_PHONE"')
            else:
                selected_columns.append(f'"{col}"')
        sql_query = f"""
                    SELECT {", ".join(selected_columns)}
                    FROM "{self.src_schema_name}"."{self.src_table_name}"
                    """
        #sql_query = f"""
        #            SELECT {", ".join(map(lambda x: f'"{x}"', columns))}
        #            FROM "{self.src_schema_name}"."{self.src_table_name}"
        #            """
        codition = f"""
                    WHERE "{cursor_field}" >= TO_DATE('{self.start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    AND "{cursor_field}" < TO_DATE('{self.end_time}', 'YYYY-MM-DD HH24:MI:SS')
                    """ if cursor_field else ""
        chunk_query = f"""
                    OFFSET {offset} ROWS
                    FETCH NEXT {chunk_size} ROWS ONLY
                    """ if chunk_size else """"""
        sql_query = sql_query + codition + chunk_query
        
        LOGGER.info("{} - get_extract_query() - END creating SQL select query --> {}"
                    .format(self.task_id.split(".")[-1], sql_query))
        return sql_query

    def validate_schema(self,
                        dataframe,
                        context):
        """ Kiểm tra dữ liệu: đúng schema, đúng data type
            dataframe: pandas dataframe chứa dữ liệu để xử lý
            context: context của task trong airflow
        """
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
            self.task_metadata.write_result(result="Fail while validating data", is_success=False)
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
        LOGGER.info("{} - load_data() - START loading data to staging layer".format(self.task_id.split(".")[-1]))
        try:
            with self.stg_connection.cursor() as cur:
                for bc in range(0, batch_counter):
                    file_path = f"./data_batch_{des_schema_name}_{des_table_name}_{bc}.csv"
                    print(file_path)
                    with open(file_path, "r", encoding="utf-8") as f:
                        with cur.copy(
                            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier("staging", f"{des_schema_name}_{des_table_name}"))
                        ) as copy:
                            copy.write(f.read())

                self.stg_connection.commit()
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                                            sql.Identifier("staging", f"{des_schema_name}_{des_table_name}")
                                        ))
                count = cur.fetchone()[0]
            LOGGER.info("{} - load_data() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
            return count
        except Exception as e:
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
            truncate_query = sql.SQL(
                "TRUNCATE TABLE {};"
            ).format(
                sql.Identifier("staging", f"{des_schema_name}_{des_table_name}")
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
