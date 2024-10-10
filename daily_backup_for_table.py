import os
import subprocess
import time
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# 환경 설정
USER = "backup"
PASSWORD = "비밀번호기재"
OUTPUT_DIR = "/db_backup_new/file_backup"
WEEKLY_OUTPUT_DIR = "/db_backup_new/weekly_backup"
DATE = datetime.now().strftime("%Y_%m_%d")
YOIL = datetime.now().strftime("%a")
BACKUP_MAX_THREAD = 4
MYSQL_SOCKET = "/db_new/analyze-db/tmp/all/mysql_all.sock"
BASE_DIR = "/db_new/bin/mariadb-10.5.12-linux-x86_64"

# 로그 설정
logging.basicConfig(filename=os.path.join(OUTPUT_DIR, 'backup.log'),
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class BackupExecute:
    def __init__(self, user, password, output_dir, weekly_output_dir, backup_max_thread, mysql_socket, base_dir):
        self.user = user
        self.password = password
        self.output_dir = output_dir
        self.weekly_output_dir = weekly_output_dir
        self.backup_max_thread = backup_max_thread
        self.mysql_socket = mysql_socket
        self.base_dir = base_dir

    def execute_backup_for_database(self, db):
        logging.info(f"Starting backup for database: {db}")
        self.create_backup_dir(self.output_dir, DATE)
        table_list = self.get_table_list(db)
        futures = []

        with ThreadPoolExecutor(max_workers=self.backup_max_thread) as executor:
            logging.info(f"ThreadPoolExecutor created with max_workers={self.backup_max_thread}")
            for table in table_list:
                futures.append(executor.submit(self.perform_backup, db, table))
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc}")

        # 활성화된 스레드 수를 로그에 기록
        logging.info(f"Backup for database {db} completed")

    def create_backup_dir(self, parent_dir, sub_dir):
        backup_dir = os.path.join(parent_dir, sub_dir)
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            logging.info(f"Created backup directory for {sub_dir}: {backup_dir}")
        else:
            logging.info(f"Backup directory for {sub_dir} already exists: {backup_dir}")

    def get_database_list(self):
        logging.info("Fetching database list")

        db_list_cmd = "SHOW DATABASES;"
        try:
            db_list_proc = subprocess.Popen(
                [f"{self.base_dir}/bin/mysql", "-N", "-u", self.user, f"-p{self.password}", f"--socket={self.mysql_socket}", "-e", db_list_cmd],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = db_list_proc.communicate()
            if db_list_proc.returncode != 0:
                logging.error(f"Error fetching database list: {stderr.decode('utf-8')}")
                return []
            db_list = stdout.decode("utf-8").split()
            logging.debug(f"Database list fetched: {db_list}")
            return db_list
        except Exception as e:
            logging.error(f"Exception occurred while fetching database list: {e}")
            return []

    def get_table_list(self, db):
        logging.info(f"Fetching table list for database: {db}")
        table_list_cmd = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db}' AND table_name NOT IN (SELECT TableName FROM Operating.BackupIgnoreTable ) ORDER BY data_length DESC"
        try:
            table_list_proc = subprocess.Popen(
                [f"{self.base_dir}/bin/mysql", "-N", "-u", self.user, f"-p{self.password}", db, f"--socket={self.mysql_socket}", "-e", table_list_cmd],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = table_list_proc.communicate()
            if table_list_proc.returncode != 0:
                logging.error(f"An error occurred while fetching table list for database '{db}': {stderr.decode('utf-8')}")
                return []
            table_list = stdout.decode("utf-8").split()
            logging.debug(f"Table list for database {db} fetched: {table_list}")
            return table_list
        except Exception as e:
            logging.error(f"Exception occurred while fetching table list for database '{db}': {e}")
            return []

    def perform_backup(self, db, table):
        logging.info(f"Starting backup for table: {table} in database: {db}")
        self.create_backup_dir(os.path.join(self.output_dir, DATE, db), "")
        self.write_backup_log(db, table)
        self.execute_backup(db, table)

    def write_backup_log(self, db, table):
        try:
            backup_log_path = os.path.join(self.output_dir, "backup.log")
            with open(backup_log_path, "a") as log_file:
                log_file.write(f"{table}: {datetime.now()}\n")
            logging.debug(f"Backup log written for table: {table} in database: {db}")
        except Exception as e:
            logging.error(f"An error occurred during logging for database '{db}' and table '{table}': {e}")

    def execute_backup(self, db, table):
        logging.info(f"Executing backup for table: {table} in database: {db}")
        db_backup_dir = os.path.join(self.output_dir, DATE, db)
        backup_cmd = [
            f"{self.base_dir}/bin/mariadb-dump",
            f"-u{self.user}",
            f"-p{self.password}",
            "--opt",
            "--single-transaction",
            f"--socket={self.mysql_socket}",
            db,
            table
        ]
        try:
            with open(f"{db_backup_dir}/{table}.sql.gz", "wb") as output_file:
                backup_proc = subprocess.Popen(
                    backup_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                stdout, stderr = backup_proc.communicate()
                output_file.write(stdout)
                if backup_proc.returncode != 0:
                    logging.error(f"An error occurred during executing backup command for database '{db}' and table '{table}': {stderr.decode('utf-8')}")
                else:
                    logging.info(f"Backup completed for table: {table} in database: {db}")
        except Exception as e:
            logging.error(f"Exception occurred during backup for table '{table}' in database '{db}': {e}")


    def execute_backup(self, db, table):
        logging.info(f"Executing backup for table: {table} in database: {db}")
        db_backup_dir = os.path.join(self.output_dir, DATE, db)
        os.makedirs(db_backup_dir, exist_ok=True)

        backup_cmd = [
            f"{self.base_dir}/bin/mariadb-dump",
            f"-u{self.user}",
            f"-p{self.password}",
            "--opt",
            "--single-transaction",
            f"--socket={self.mysql_socket}",
            db,
            table
        ]
        try:
            with open(f"{db_backup_dir}/{table}.sql.gz", "wb") as output_file:
                backup_proc = subprocess.Popen(
                    backup_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                gzip_proc = subprocess.Popen(
                    ["gzip"], stdin=backup_proc.stdout, stdout=output_file
                )
                backup_proc.stdout.close()  # Allow backup_proc to receive a SIGPIPE if gzip_proc exits
                _, stderr = backup_proc.communicate()
                gzip_proc.communicate()

                if backup_proc.returncode != 0:
                    logging.error(f"An error occurred during executing backup command for database '{db}' and table '{table}': {stderr.decode('utf-8')}")
                else:
                    logging.info(f"Backup completed for table: {table} in database: {db}")
        except Exception as e:
            logging.error(f"Exception occurred during backup for table '{table}' in database '{db}': {e}")

class ManagerBackupfile:
    def manage_weekly_backups(self, output_dir, weekly_output_dir):
        logging.info("Managing weekly backups")

        self.move_weekly_backups(output_dir, weekly_output_dir)
        self.erase_old_backups(output_dir)

    def move_weekly_backups(self, source_dir, target_dir):
        # 현재 날짜 확인
        today = datetime.now()

        # 금요일인지 확인 (weekday()에서 금요일은 4)
        if today.weekday() != 4:
            logging.info("Skip Move file backup to weekly backup")
            return

        # 오늘 날짜의 디렉토리 이름 생성 (YYYY_MM_DD 형식)
        today_dir_name = today.strftime("%Y_%m_%d")
        source_path = os.path.join(source_dir, today_dir_name)
        target_path = os.path.join(target_dir, today_dir_name)

        try:
            if os.path.exists(source_path):
                # 디렉토리 이동
                shutil.move(source_path, target_path)
                logging.info(f"Successfully Moving file backup directory to weekly backup directory : {source_path} -> {target_path}")
            else:
                logging.warning(f"Not founded file backup: {source_path}")

        except Exception as e:
            logging.error(f"Error Occurs during moving file backup directory to weekly backup directory: {e}")

    def erase_old_backups(self, target_dir):
        logging.info("Erasing old backups")

        # 3일 전 날짜 계산
        three_days_ago = datetime.now() - timedelta(days=3)

        try:
            # target_dir 바로 아래의 디렉토리 얻기
            subdirs = [d for d in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, d))]

            for subdir in subdirs:
                try:
                    # 디렉토리 이름을 날짜로 파싱
                    dir_date = datetime.strptime(subdir, "%Y_%m_%d")

                    if dir_date < three_days_ago:
                        full_path = os.path.join(target_dir, subdir)

                        logging.info(f"Deleting old backup: {full_path}")
                        shutil.rmtree(full_path)
                        print(f"Delete completed: {full_path}")

                except ValueError:
                    logging.warning(f"Directory name is not date format. : {subdir}")
                    continue

        except Exception as e:
            logging.error(f"erase_old_backups method occurs errors : {e}")


# 백업 스크립트 실행
if __name__ == "__main__":
    backup_manager = BackupExecute(user=USER, password=PASSWORD, output_dir=OUTPUT_DIR,
                                  weekly_output_dir=WEEKLY_OUTPUT_DIR, backup_max_thread=BACKUP_MAX_THREAD,
                                  mysql_socket=MYSQL_SOCKET, base_dir=BASE_DIR)

    database_list = backup_manager.get_database_list()
    for db in database_list:
        backup_manager.execute_backup_for_database(db)

    logging.info("Database backups completed")

    weekly_backup_manager = ManagerBackupfile()
    weekly_backup_manager.manage_weekly_backups(output_dir=OUTPUT_DIR, weekly_output_dir=WEEKLY_OUTPUT_DIR)
    logging.info("Weekly backup management completed")

