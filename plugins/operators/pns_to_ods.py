# Standard library imports
import datetime
import logging
from datetime import datetime, timedelta

# Third-party library imports
import dask
import pandas as pd
import psycopg
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# Airflow imports
from airflow.utils.decorators import apply_defaults
from psycopg import sql
from soda.scan import Scan

from helper1.task_logger import TaskRunMetadata

LOGGER = logging.getLogger(__name__)

dask.config.set({"dataframe.convert-string": False})


class PNSToStagingDailyOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 staging_conn_id: str,
                 minio_conn_id: str,
                 minio_bucket_name: str,
                 des_schema_name: str,
                 des_table_name: str,
                 minio_key: str,
                 columns: dict,
                 file_prefix: str,
                 header_rows: int,
                 end_skip: int,
                 type: str,
                 cursor_field: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stg_connection = None
        self.src_file_path = None
        self.task_metadata = None
        self.minio_conn_id = minio_conn_id
        self.minio_bucket_name = minio_bucket_name
        self.staging_conn_id = staging_conn_id
        self.cursor_field = cursor_field
        self.des_schema_name = des_schema_name
        self.des_table_name = des_table_name
        self.columns = list(columns.keys())
        self.file_prefix = file_prefix
        self.header_rows = header_rows
        self.minio_key = minio_key
        self.file_dates = []
        self.end_skip = end_skip
        self.type = type
        self.start_time = None
        self.end_time = None

    def pre_execute(self, context):
        LOGGER.info("{} - pre_execute() - START establishing source and staging connection".format(self.task_id))
        base_date = context['logical_date'].in_tz('Asia/Ho_Chi_Minh').start_of('day')
        if context["dag_run"].run_type == "scheduled":
            self.file_dates.append(base_date.strftime('%d%m%Y'))
        else:
            self.start_time = datetime.strptime(context["dag_run"].conf.get("start_time"), "%Y-%m-%d %H:%M:%S")
            self.end_time = datetime.strptime(context["dag_run"].conf.get("end_time"), "%Y-%m-%d %H:%M:%S")
            current_date = self.start_time
            while current_date < self.end_time:
                file_date = current_date.strftime("%d%m%Y")
                self.file_dates.append(file_date)
                current_date += timedelta(days=1)
                print(current_date)
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
        
        self.task_metadata = TaskRunMetadata(context=context,
                                             table_name=self.des_schema_name,
                                             result_connection=BaseHook.get_connection(
                                                 "staging_conn_id").get_uri())
        try:
            self.stg_connection = psycopg.connect(BaseHook.get_connection(self.staging_conn_id).get_uri())
        except Exception as e:
            LOGGER.error("{} - pre_execute() - ERROR --> {}"
                         .format(self.task_id, e),
                         exc_info=True)
        LOGGER.info("{} - pre_execute() - END connections established".format(self.task_id))
 
    def execute(self, context):
        LOGGER.info("{} - execute() - START fetching Dataframe, validate source data".format(self.task_id))
        print(len(self.file_dates))
        for self.file_date in self.file_dates:

            _, data = self.get_file_from_minio()

            df = pd.read_excel(data, engine='openpyxl', header=None, skiprows=self.header_rows)
            if df.empty:
                LOGGER.warning(f"{self.task_id} - execute() - Skipping file for date {self.file_date} because it is empty.")
                continue

            df = df.iloc[:, :len(self.columns)]
            df.columns = self.columns

            LOGGER.info("{} - execute() DF INFO ----> {}".format(self.task_id, df.info()))

            df = df.astype(object)
            df = df.where(pd.notnull(df), None)

            if self.type == 'category':
                df.replace("NULL", None, inplace=True)
                if self.des_table_name == 'collection_delivery_route':
                    df = df.dropna(subset=['route_code'])
                    df = df.dropna(subset=['unit_code'])
                    df = df.drop_duplicates(subset=["route_code", "unit_code"], keep="first")
            else:
                df = df.dropna(subset=['lading_code'])
                df['etl_date'] = pd.to_datetime(datetime.today())  # todo: chuyen thanh run_date de chay backfill

            if self.end_skip > 0:
                df = df.iloc[:-self.end_skip]

            LOGGER.info("{} - execute() DF ----> ".format(df.info()))
            LOGGER.info("{} - execute() - Dataframe source data --> {}".format(self.task_id, df.head()))

            self.validate_schema(dataframe=df, context=context)
            self.load_data(dataframe=df, context=context)

        LOGGER.info("{} - execute() - END loading data to staging layer".format(self.task_id))


    def post_execute(self,
                     context,
                     result=None):
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id))
        self.stg_connection.close()
        self.task_metadata.write_result(
            result=result,
            is_success=True
        )
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id))

    def get_file_from_minio(self) -> tuple[str, bytes]:

        self.log.info(f"{self.task_id} - get_file_from_minio() - START getting xlsx file from MinIO")

        try:
            # Khởi tạo kết nối MinIO
            s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

            # Lấy danh sách các objects trong folder
            matching_files = []
            objects = s3_hook.list_keys(bucket_name=self.minio_bucket_name, prefix=self.minio_key)

            for obj_key in objects:
                file_name = obj_key.split('/')[-1]  # Lấy tên file từ đường dẫn đầy đủ
                # Kiểm tra điều kiện file
                if (self.type == 'detail' and
                        file_name.endswith('.xlsx') and
                        self.file_prefix.lower() in file_name.lower() and
                        self.file_date in file_name.lower()):
                    matching_files.append(obj_key)
                elif (self.type == 'category' and
                      file_name.endswith('.xlsx') and
                      self.file_prefix.lower() in file_name.lower()):
                    matching_files.append(obj_key)

            if matching_files:
                # Lấy file đầu tiên tìm thấy
                selected_file = matching_files[0]

                # Đọc nội dung file
                s3_object = s3_hook.get_key(key=selected_file, bucket_name=self.minio_bucket_name)
                file_data = s3_object.get()["Body"].read()

                if not file_data:
                    raise ExcelReadError(f"File {selected_file} is empty")

                self.log.info(
                    f"{self.task_id} - get_file_from_minio() - Successfully read file: {selected_file} "
                    f"(size: {len(file_data)} bytes)"
                )

                return selected_file, file_data

            else:
                error_msg = (
                    f"File xlsx không tồn tại với prefix '{self.file_prefix}' "
                    f"và ngày '{self.file_date}' trong folder {self.minio_key}."
                )
                self.log.error(f"{self.task_id} - {error_msg}")
                self.task_metadata.write_result(
                    result="Xlsx file does not exist",
                    is_success=False
                )
                raise FileNotFoundInMinIOException(error_msg)

        except Exception as e:
            self.log.error(
                f"{self.task_id} - Error while getting file from MinIO: {str(e)}",
                exc_info=True
            )
            self.task_metadata.write_result(
                result="Errors while getting file from MinIO",
                is_success=False
            )
            raise

    def validate_schema(self,
                        dataframe,
                        context):
        LOGGER.info("{} - validate_schema() - START validating source data".format(self.task_id))
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
            LOGGER.info("{} - validate_schema() - END validating source data".format(self.task_id))
        except Exception:
            self.task_metadata.write_result(
                result="Fail while validating data",
                is_success=False
            )
            if scan.has_error_logs():
                LOGGER.error("{} - validate_schema() - ERROR --> {}"
                             .format(self.task_id, scan.get_error_logs_text()),
                             exc_info=True)
                raise
            if scan.has_check_fails():
                LOGGER.error("{} - validate_schema() - ERROR --> {} "
                             .format(self.task_id, scan.get_checks_fail_text()))
                raise

    def load_data(self,
                  dataframe,
                  context):
        LOGGER.info("{} - load_data() - START loading data to staging layer".format(self.task_id))

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
            LOGGER.info("{} - load_data() - END loading data to staging layer".format(self.task_id))
        except Exception as e:
            self.task_metadata.write_result(
                result="Fail while loading data",
                is_success=False
            )
            LOGGER.error("{} - load_data() - ERROR when loading data to staging layer --> {} "
                         .format(self.task_id, e), exc_info=True)
            raise


class FileNotFoundErrorException(Exception):
    """Custom Exception khi không tìm thấy file phù hợp."""
    pass


class FileNotFoundInMinIOException(Exception):
    pass


class ExcelReadError(Exception):
    pass
