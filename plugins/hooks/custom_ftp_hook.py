import os
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime, timedelta
import shutil


class CustomFTPHook(FTPHook):
    """
    Custom Hook kế thừa từ FTPHook, hỗ trợ kiểm tra kết nối, tải file/thư mục từ FTP server về local.
    """

    def test_connection(self):
        """
        Kiểm tra kết nối đến FTP Server.
        Trả về True nếu kết nối thành công, ngược lại trả về Exception.
        """
        try:
            conn = self.get_conn()
            files = conn.nlst('/')
            self.log.info(f"{self.hook_name} - Connection successful. Root directory contains: {files}")
            return True
        except Exception as e:
            self.log.error(f"{self.hook_name} - Failed to connect to FTP Server: {e}", exc_info=True)
            raise

    def download_file(self, remote_path: str, local_path: str):
        """
        Tải một file từ FTP server về máy.
        """
        self.log.info(
            f"{self.hook_name} - download_file() - START downloading file: {remote_path} -> {local_path}")
        try:
            conn = self.get_conn()
            with open(local_path, 'wb') as local_file:
                conn.retrbinary(f"RETR {remote_path}", local_file.write)
            self.log.info(f"{self.hook_name} - Successfully downloaded file: {remote_path} -> {local_path}")
            self.log.info(
                f"{self.hook_name} - download_file() - END downloading file: {remote_path} -> {local_path}")
        except Exception as e:
            self.log.error(f"{self.hook_name} - Failed to download file: {remote_path} --> Error: {e}", exc_info=True)
            raise

    def download_directory(self, remote_dir: str, local_dir: str, date_format: str = '%d%m%Y', t_minus: int = 1):
        """
        Tải toàn bộ thư mục từ FTP server về máy chỉ lấy ngày T-1.

        :param remote_dir: Thư mục trên FTP server.
        :param local_dir: Thư mục lưu trữ tại máy local.
        :param date_format: Định dạng ngày trong tên file (ví dụ: '%d%m%Y').
        :param t_minus: Số ngày lùi về trước, mặc định là 1 (ngày hôm qua).
        """
        self.log.info(f"{self.hook_name} - download_directory() - START downloading directory: {remote_dir} -> {local_dir}")
        try:
            conn = self.get_conn()
            file_list = conn.nlst(remote_dir)
            self.log.info(f"{self.hook_name} - download_directory() - REMOTE Files list --> {file_list}")
            os.makedirs(local_dir, exist_ok=True)  # Tạo thư mục local nếu chưa tồn tại

            for item in os.listdir(local_dir): # Clean thư mục download trước khi download data
                item_path = os.path.join(local_dir, item)

                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                    self.log.info("{} - download_directory() - File deleted -> {}"
                                .format(self.hook_name, item_path))
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    self.log.info("{} - download_directory() - Folder deleted -> {}"
                                .format(self.hook_name, item_path))

            file_names = [os.path.basename(file_path) for file_path in file_list]
            target_date = (datetime.now() - timedelta(days=t_minus)).strftime(date_format)

            for file_name in file_names:
                if target_date in file_name:
                    remote_file_path = os.path.join(remote_dir, file_name)
                    local_file_path = os.path.join(local_dir, file_name)
                    self.download_file(remote_file_path, local_file_path)
                else:
                    self.log.info(f"{self.hook_name} - This file contains invalid date: {file_name}")
            self.log.info(f"{self.hook_name} - Successfully downloaded directory: {remote_dir} -> {local_dir}")
            self.log.info(f"{self.hook_name} - download_directory() - END downloading directory: {remote_dir} -> {local_dir}")
        except Exception as e:
            self.log.error(f"{self.hook_name} - Failed to download directory: {remote_dir} --> Error: {e}",
                           exc_info=True)
            raise
