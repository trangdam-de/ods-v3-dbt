from datetime import datetime, timedelta
import logging
import io

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
from helper1.task_logger import TaskRunMetadata
from airflow.hooks.base import BaseHook

LOGGER = logging.getLogger(__name__)

class SFTPToMinIOOperator(BaseOperator):
    """
    Custom Operator để stream file từ SFTP trực tiếp lên MinIO.
    """

    @apply_defaults
    def __init__(self,
                 sftp_conn_id,
                 sftp_remote_path,
                 minio_conn_id,
                 minio_bucket_name,
                 minio_key,
                 type,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_remote_path = sftp_remote_path
        self.minio_conn_id = minio_conn_id
        self.minio_bucket_name = minio_bucket_name
        self.minio_key = f'{minio_key}{sftp_remote_path.split("/")[-1]}'
        self.sftp_hook = None
        self.minio_hook = None
        self.task_metadata = None
        self.file_dates = []
        self.type = type

    def pre_execute(self, context):
        LOGGER.info("{} - pre_execute() - START".format(self.task_id.split(".")[-1]))
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
            if not self.start_time or not self.end_time:
                LOGGER.error("{} - pre_execute() - ERROR there is no configuration for start_time and end_time"
                        .format(self.task_id.split(".")[-1]), exc_info=True)
                raise
        
        self.log.info("Connecting to SFTP server...")
        self.sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        self.log.info("Connecting to MinIO...")
        self.minio_hook = S3Hook(aws_conn_id=self.minio_conn_id)
        self.task_metadata = TaskRunMetadata(context=context,
                                             table_name='pns',
                                             result_connection=BaseHook.get_connection(
                                                 "staging_conn_id").get_uri())
        LOGGER.info("{} - pre_execute() - END".format(self.task_id.split(".")[-1]))

    def execute(self, context):
        # Kết nối tới SFTP và đọc file bằng luồng dữ liệu
        LOGGER.info("{} - execute() - START".format(self.task_id.split(".")[-1]))
        self.log.info("Reading file from SFTP: %s", self.sftp_remote_path)
        with self.sftp_hook.get_conn() as sftp_client:
            for self.file_date in self.file_dates:
                sftp_remote_path = f"{self.sftp_remote_path}{self.file_date}.xlsx" if self.type == 'detail' else self.sftp_remote_path
                try:
                    sftp_client.stat(sftp_remote_path)  # Kiểm tra nếu file tồn tại
                    self.log.info(f"File {sftp_remote_path} exists on SFTP server.")
                except FileNotFoundError:
                    error_message = f"File {sftp_remote_path} does not exist on SFTP server."
                    self.log.error(error_message)
                    raise AirflowException(error_message)

                # Nếu file tồn tại, tiến hành đọc file
                self.log.info("File exists. Streaming file from SFTP into memory...")
                with sftp_client.file(sftp_remote_path, mode='rb') as file_stream:
                    file_buffer = io.BytesIO(file_stream.read())

                self.log.info("Uploading file to MinIO: Bucket=%s, Key=%s", self.minio_bucket_name, self.minio_key+self.file_date+".xlsx")
                self.minio_hook.load_bytes(
                    bytes_data=file_buffer.getvalue(),
                    key=self.minio_key+self.file_date+".xlsx",
                    bucket_name=self.minio_bucket_name,
                    replace=True
                )
                    
        self.log.info("File successfully uploaded to MinIO.")
        LOGGER.info("{} - execute() - END".format(self.task_id.split(".")[-1]))

    def post_execute(self,
                     context,
                     result=None):
        LOGGER.info("{} - post_execute() - START calling task callback".format(self.task_id))
        self.task_metadata.write_result(
            result=result,
            is_success=True
        )
        LOGGER.info("{} - post_execute() - END calling task callback --> See the task results".format(self.task_id))
