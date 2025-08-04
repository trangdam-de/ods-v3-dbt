# filepath: d:\CÔNG TY\VNPOST\Kidoisoat\ods\airflow\plugins\operators\casreport_to_ods.py
# Standard library imports
import datetime
from datetime import date, datetime, timedelta
import logging
import re
import sys
import traceback
from typing import Any
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

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


class CASREPORTToStagingDailyOperator(BaseOperator):
    # template_fields = ('prefix', 'src_bucket_name')
    @apply_defaults
    def __init__(self,
                 casreport_conn_id: str,
                 staging_conn_id: str,
                 src_schema_name: str,
                 src_table_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 middle_storage_conn_id: str,
                 bucket: str,
                 cursor_field: str = None,
                 columns: dict = "*",
                 sql: str = "",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oracle_hook = None
        self.stg_connection = None
        self.casreport_connection = None
        self.casreport_conn_id = casreport_conn_id
        self.staging_conn_id = staging_conn_id
        self.src_schema_name = src_schema_name
        self.src_table_name = src_table_name
        self.cursor_field = cursor_field
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = columns
        self.sql = sql
        self.start_time = None
        self.end_time = None
        self.middle_storage_conn_id = middle_storage_conn_id
        self.bucket = bucket

    def pre_execute(self, context):
    # Hàm ch?y sau khi kh?i t?o Operator
    # Hàm kh?i t?o cho các bi?n
    ## context: context c?a task trong airflow
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
        
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
            self.start_time = context['data_interval_start'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')
            self.end_time = context['data_interval_end'].in_tz('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S')

        else:
            self.start_time = context["dag_run"].conf.get("start_time")
            self.end_time = context["dag_run"].conf.get("end_time")
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
        
        LOGGER.info("{} - pre_execute() - START establishing source and staging connection".format(self.task_id.split(".")[-1]))
        try:
            self.s3_hook = S3Hook(aws_conn_id=self.middle_storage_conn_id)
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY middle storage connections established".format(self.task_id.split(".")[-1]))
            self.casreport_connection = OracleHook(oracle_conn_id=self.casreport_conn_id)
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
    def get_my_total_rows(self, 
                        engine,
                        schema_name,
                        table_name, 
                        cursor_field,
                        start_time,
                        end_time,
                        type):
        """
        Hàm riêng d? d?m s? lu?ng b?n ghi v?i h? tr? cursor_field ki?u integer
        """
        # Xác d?nh ki?u d? li?u c?a cursor_field t? self.columns
        cursor_type = None
        if cursor_field and cursor_field in self.columns:
            cursor_type = self.columns[cursor_field]
        
        sql_query = f"""
                    SELECT count(1)
                    FROM "{schema_name}"."{table_name}"
                    """
        
        if cursor_field:
            if cursor_type == 'int':
                # X? lý cho cursor_field ki?u integer (yyyymmdd)
                start_date = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                end_date = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                start_int = int(start_date.strftime('%Y%m%d'))
                end_int = int(end_date.strftime('%Y%m%d'))
                
                condition = f"""
                        WHERE "{cursor_field}" >= {start_int} 
                        AND "{cursor_field}" < {end_int}
                        """
            else:
                # X? lý cho cursor_field ki?u datetime
                condition = f"""
                        WHERE "{cursor_field}" >= TO_DATE('{start_time}', 'YYYY-MM-DD HH24:MI:SS') 
                        AND "{cursor_field}" < TO_DATE('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                        """
            
            sql_query = sql_query + condition
        
        LOGGER.info("{} - get_my_total_rows() - SQL query: {}".format(self.task_id.split(".")[-1], sql_query))
        
        # S? d?ng tr?c ti?p k?t n?i Oracle thay vì qua engine
        try:
            connection = self.get_oracle_connection(self.casreport_conn_id)
            cursor = connection.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchone()
            count = result[0] if result else 0
            cursor.close()
            return count
        except Exception as e:
            LOGGER.error(f"Error executing count query: {str(e)}")
            # N?u có l?i, th? cách th? hai s? d?ng OracleHook
            try:
                result = engine.get_pandas_df(sql_query)
                return result.iloc[0, 0]
            except Exception as e2:
                LOGGER.error(f"Second attempt also failed: {str(e2)}")
                return 0
    def execute(self, context):
        # Hàm ch?y sau hàm pre_execute
        ## context: context c?a task trong airflow
        
        LOGGER.info("{} - execute() - START execute extracting, validating and loading data".format(self.task_id.split(".")[-1]))
        try:
            # S? d?ng hàm get_my_total_rows thay vì get_total_rows
            total_rows = self.get_my_total_rows(
                self.casreport_connection,
                self.src_schema_name,
                self.src_table_name,
                self.cursor_field,
                self.start_time,
                self.end_time,
                type='oracle'
            )
            
            LOGGER.info("{} - execute() - Total rows to process: {}".format(self.task_id.split(".")[-1], total_rows))
            
            # Ph?n còn l?i c?a phuong th?c
            self.truncate_staging_table(
                connection=self.stg_connection,
                des_schema_name=self.des_schema_name,
                des_table_name=self.des_table_name
            )
            
            sql_query = self.get_extract_query(
                list(self.columns.keys()),
                self.cursor_field
            )
            
            batch_counter = self.extract(
                sql=sql_query, 
                columns_type=self.columns,
                des_table_name=self.des_table_name,
                des_schema_name=self.des_schema_name
            )
            
            rs = self.load_data(
                batch_counter, 
                des_table_name=self.des_table_name,
                des_schema_name=self.des_schema_name
            )
                
            result_message = f"Successfully load {rs} rows from {self.src_schema_name}.{self.src_table_name} to {self.des_schema_name}.{self.des_table_name}"
            LOGGER.info(result_message)
            LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
            
            # Luu start_time và end_time vào XCom d? s? d?ng trong task sau
            context["ti"].xcom_push(key="start_time", value=self.start_time)
            context["ti"].xcom_push(key="end_time", value=self.end_time)
            
            return result_message
        except Exception as e:
            self.task_metadata.write_result(result=f"Failed to execute: {str(e)}", is_success=False)
            LOGGER.error("{} - execute() - ERROR during execution --> {} ".format(
                self.task_id.split(".")[-1], str(e)), exc_info=True)
            raise

    def post_execute(self,
                     context,
                     result=None):
    # Hàm ch?y cu?i cùng khi k?t thúc Operator
    ## context: context c?a task trong airflow
    ## result: result du?c tr? t? hàm execute
    ## Trong Operator này, result không du?c tr? -> set m?c d?nh là None
    
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
        
    def get_extract_query(self, 
                        columns,
                        cursor_field,
                        offset = None,
                        chunk_size = None):

        LOGGER.info("{} - get_extract_query() - START creating SQL select query".format(self.task_id.split(".")[-1]))
        # Ki?m tra ki?u d? li?u c?a cursor_field n?u có
        cursor_type = None
        if cursor_field and cursor_field in self.columns:
            cursor_type = self.columns[cursor_field]
        
        sql_query = f"""
                    SELECT {", ".join(map(lambda x: f'"{x}"', columns))}
                    FROM "{self.src_schema_name}"."{self.src_table_name}"
                    """
        
        # X? lý di?u ki?n l?c theo cursor_field
        condition = ""
        if cursor_field:
            if cursor_type == 'int':
                # Chuy?n d?i datetime thành integer format YYYYMMDD
                # S?a l?i dòng 186-189
                start_date = datetime.strptime(self.start_time, '%Y-%m-%d %H:%M:%S')
                end_date = datetime.strptime(self.end_time, '%Y-%m-%d %H:%M:%S')
                start_int = int(start_date.strftime('%Y%m%d'))
                end_int = int(end_date.strftime('%Y%m%d'))
                    
                condition = f"""
                            WHERE "{cursor_field}" >= {start_int}
                            AND "{cursor_field}" < {end_int}
                            """
            else:
                # X? lý bình thu?ng cho date/datetime
                condition = f"""
                            WHERE "{cursor_field}" >= TO_DATE('{self.start_time}', 'YYYY-MM-DD HH24:MI:SS')
                            AND "{cursor_field}" < TO_DATE('{self.end_time}', 'YYYY-MM-DD HH24:MI:SS')
                            """
        
        # X? lý phân trang
        chunk_query = f"""
                    OFFSET {offset} ROWS
                    FETCH NEXT {chunk_size} ROWS ONLY
                    """ if chunk_size else """"""
                    
        sql_query = sql_query + condition + chunk_query
        
        LOGGER.info("{} - get_extract_query() - END creating SQL select query --> {}"
                    .format(self.task_id.split(".")[-1], sql_query))
        return sql_query

    def extract(self,
                sql,
                columns_type,
                des_table_name,
                des_schema_name,
                batch_size=50000):
        LOGGER.info("{} - extract() - START fetching Dataframe, validate source data".format(self.task_id.split(".")[-1]))
        try:
            connection = self.get_oracle_connection(self.casreport_conn_id)
            cursor = connection.cursor()
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
                # X? lý các giá tr? NULL tru?c khi chuy?n d?i ki?u d? li?u
                for col, dtype in columns_type.items():
                    if dtype == 'int' or dtype == 'Int64':
                        data[col] = data[col].fillna(0)  # Thay th? NULL b?ng 0 cho c?t integer
                    elif dtype == 'float64':
                        data[col] = data[col].fillna(0.0)  # Thay th? NULL b?ng 0.0 cho c?t float
                    elif dtype == 'datetime64[ns]':
                        if col in data.columns:
                            if col in ['NGAY_HL', 'NGAY_KT']:
                            # X? lý d?c bi?t cho NGAY_HL và NGAY_KT
                                data[col] = data[col].astype(str)
                                data[col] = data[col].str.replace(r'^9999', '2261', regex=True)
                                data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                        else:
                            # X? lý các c?t datetime khác (TRANS_DATE, CREATE_DATE)
                            data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
 
                data = data.astype(columns_type)
                data.columns = data.columns.str.lower()
                data = data.astype(object)
                data = data.applymap(lambda x: x.replace('\r', '').replace('\x00','') if isinstance(x, str) else x)
                data = data.where(pd.notnull(data), None)
                file_path = f"./data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv"
                data.to_csv(file_path, index=False)
                self.s3_hook.load_file(
                    file_path, 
                    f"{des_schema_name}/{des_table_name}/{re.sub(r'[:]', '-', self.start_time)}_{re.sub(r'[:]', '-', self.end_time)}/data_batch_{des_schema_name}_{des_table_name}_{batch_counter}.csv", 
                    self.bucket,
                    replace = True
                )
                LOGGER.info("{} - extract() - Dataframe source data batch {} --> {}".format(self.task_id.split(".")[-1], batch_counter, data.head()))
                batch_counter += 1
            return batch_counter
        except Exception as extract_error:
            self.task_metadata.write_result(result="Fail while extracting", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when extracting --> {} "
                         .format(self.task_id.split(".")[-1], extract_error), exc_info=True)
            raise

    def validate_schema(self,
                        dataframe,
                        context):
    # Ki?m tra d? li?u: dúng schema, dúng data type
    ## dataframe: pandas dataframe ch?a d? li?u d? x? lý
    ## context: context c?a task trong airflow
    
        LOGGER.info("{} - validate_schema() - START validating source data".format(self.task_id.split(".")[-1]))
        scan = Scan()
        try:
            # Thêm config: ngu?n d? li?u
            scan.set_scan_definition_name(self.des_schema_name)
            scan.set_data_source_name("pandas")
            scan.add_pandas_dataframe(dataset_name=self.des_table_name, pandas_df=dataframe, data_source_name="pandas")
            scan.add_sodacl_yaml_file(f"/opt/airflow/soda/check/{self.des_schema_name}/{self.des_table_name}.yml")  # Thêm file check

            scan.set_verbose(True)  # Set format log
            scan.execute()  # Ch?y check d? li?u

            # Check li?u có l?i ho?c check fail
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
            self.task_metadata.write_result(result="Fail while loading data", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when loading data to staging layer --> {} "
                         .format(self.task_id.split(".")[-1], e), exc_info=True)
            raise
        
    def truncate_staging_table(self, 
                               connection,
                               des_schema_name,
                               des_table_name):
        """
        Xóa d? li?u trong b?ng staging tru?c khi t?i d? li?u m?i
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

