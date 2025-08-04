# Standard library imports
import datetime
from datetime import date, datetime, timedelta
import logging
import re
import sys
import traceback
from typing import Any

# Third-party library imports
import pandas as pd
import dask
import dask.dataframe as dd
from psycopg import connect, sql
import psycopg
import requests
from soda.scan import Scan

# Airflow imports
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

from helper1.task_logger import TaskRunMetadata

LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class HRMToStagingDailyOperator(BaseOperator):
    # template_fields = ('prefix', 'src_bucket_name')
    @apply_defaults
    def __init__(self,
                 staging_conn_id: str,
                 from_date: str,
                 to_date: str,
                 des_schema_name: str,
                 des_table_name: str,
                 src_columns: dict,
                 des_columns: list,
                 sql: str = "",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stg_connection = None
        self.staging_conn_id = staging_conn_id
        self.from_date = from_date         
        self.to_date = to_date         
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.src_columns = src_columns
        self.des_columns = des_columns
        self.sql = sql
        self.get_token_url = "https://hrm.vnpost.vn/token"
        self.get_token_headers = {
                        "Content-Type": "application/x-www-form-urlencoded"
                    }
        self.get_token_payload = {
                                "username": "hrmapi",
                                "password": "Hrmapi@123",
                                "grant_type": "password",
                                "client_id": "api_test",
                                "client_secret": "e37d1c7a-e1dd-41d0-a449-b15506550c92",
                                }
        self.get_data_url = None
        
    def pre_execute(self, context):
        self.task_metadata = TaskRunMetadata(context=context, 
                                        table_name=self.des_schema_name,
                                        result_connection=BaseHook.get_connection("staging_conn_id").get_uri())
        
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
            self.end_time = base_date.add(days=1).strftime('%d/%m/%Y')
            self.start_time = base_date.strftime('%d/%m/%Y')
        else:
            self.start_time = datetime.strptime(context["dag_run"].conf.get("start_time"), '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
            self.end_time = datetime.strptime(context["dag_run"].conf.get("end_time"), '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
    
       	self.get_data_url = f"https://hrm.vnpost.vn/api/nhansu/danhsachnhansu?TuNgay={self.from_date}&DenNgay={self.to_date}"
            
        LOGGER.info("{} - pre_execute() - START establishing source and staging connection".format(self.task_id.split(".")[-1]))
        
        try:
            # self.casadmin_connection = BaseHook.get_connection(self.casadmin_conn_id).get_uri()
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
            LOGGER.info("{} - pre_execute() - SUCCESSFULLY destination connections established".format(self.task_id.split(".")[-1]))
        except Exception as conn_error:
            self.task_metadata.write_result(result="Fail in pre_execute()", is_success=False)
            LOGGER.error("{} - pre_execute() - ERROR when creating destination connection --> {} "
                        .format(self.task_id.split(".")[-1], conn_error), exc_info=True)

        # self.sql = self.get_extract_query(list(self.columns.keys()), self.cursor_field) if len(self.sql) == 0 else self.sql
        LOGGER.info("{} - pre_execute() - END connections established".format(self.task_id.split(".")[-1]))

    def execute(self,
                context):
    # Hàm chạy sau hàm pre_execute
    ## context: context của task trong airflow
    
        LOGGER.info("{} - execute() - START execute extracting, validating and loading data".format(self.task_id.split(".")[-1]))
        token = self.get_access_token(url=self.get_token_url,
                                      headers=self.get_token_headers,
                                      payload=self.get_token_payload)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        df = self.extract(context=context, url=self.get_data_url, headers=headers)
        rs = self.load_data(dataframe=df,
                        context=context)
        LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id.split(".")[-1]))
        return f"Successfully load {rs} rows from HRM to {self.des_schema_name}.{self.des_table_name}"

    def post_execute(self,
                     context,
                     result=None):
    # Hàm chạy cuối cùng khi kết thúc Operator
    ## context: context của task trong airflow
    ## result: result được trả từ hàm execute
    
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id.split(".")[-1]))
        self.task_metadata.write_result(result=result, is_success=True)
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id.split(".")[-1]))

    def extract(self, context, url, headers):
        LOGGER.info("{} - extract() - START fetching Dataframe, validate source data".format(self.task_id.split(".")[-1]))
        try:
            print(url)
            response = requests.get(url, headers=headers)
            data = response.json()['DATA']
            df = pd.DataFrame(data=data)
            df = df.replace({'': None})
            df = df.astype(self.src_columns)
            df = df[list(self.src_columns.keys())]
            df.columns = self.des_columns
            LOGGER.info("{} - extract() - Dataframe source data --> {}".format(self.task_id.split(".")[-1], df.head()))
            # self.validate_schema(dataframe=df,
            #                  context=context)
            df = df.astype(object)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as extract_error:
            self.task_metadata.write_result(result="Fail while extracting", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when extracting --> {} "
                         .format(self.task_id.split(".")[-1], extract_error), exc_info=True)
            raise
    
    def get_access_token(self, url, headers, payload):
        res = requests.post(url=url, data=payload, headers=headers)
        return res.json()['access_token']
        
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
                  dataframe,
                  context):
        LOGGER.info("{} - load_data() - START loading data to staging layer".format(self.task_id.split(".")[-1]))
        try:
            columns = dataframe.columns.map(str.lower).tolist()
            for col in ['mobile', 'email']:
                if col in dataframe.columns:
                    dataframe[col] = None

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
            return dataframe.shape[0]
        except Exception as e:
            self.task_metadata.write_result(result="Fail while loading data", is_success=False)
            LOGGER.error("{} - load_data() - ERROR when loading data to staging layer --> {} "
                         .format(self.task_id.split(".")[-1], e), exc_info=True)
            raise
