import pprint
import subprocess
import logging
import os
import re
import gzip
from datetime import datetime
import argparse

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Compiled regular expressions
BASEDIR_PATTERN = re.compile(r'--basedir=([^\s]+)')
DEFAULTS_FILE_PATTERN = re.compile(r'--defaults-file=([^\s]+)')
VERSION_PATTERN = re.compile(r'(mysql|mariadb)-?(\d+\.\d+\.\d+)')

# Version replacement mappings
BACKUPTOOL_VERSION_MAPPING = {
    'MySQL': {
        '8.0.24': '8.0.25',
        '5.7.38': '2.4.29',
        '8.0.19': '8.0.22'
    },
    'MariaDB': {
        '5.5.59': '10.1.26'
    }
}

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._init_config(cls._instance) # 설정 초기화
        return cls._instance

    @staticmethod
    def _init_config(self):
        self.REMOTE_USER = 'ec2-user'
        self.REMOTE_BACKUP_SERVER_IP = '아이피주소 기재'
        self.REMOTE_PEMKEY = '/home/ec2-user/pemkey/common-db.pem'
        self.REMOTE_BACKUP_DIR = '/binlog/daily_backup'
        self.REMOTE_HRD_BACKUP_DIR = '/binlog/hrd_daily_backup'
        self.BACKUP_OPTIONS = {'compress_threads': 2, 'parallel': 4}
        self.DB_PASSWORD = '비밀번호 기재'
        self.LOG_PATH = '/db/logs/backup.logs'
        self.DB_BINARY_PATH = '/db/bin'

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description="Process backup operations.", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('-l', '--list', action='store_true', help='Print list of database instances')
        parser.add_argument('-a', '--all', action='store_true', help='Print complete list of database instances with all details')
        parser.add_argument('-p', '--pretty', action='store_true', help='Print pretty complete list of database instances with all details')
        parser.add_argument('-b', '--backup', action='store_true', help='Start backup process')
        parser.add_argument('-i', '--instance_name', type=str, default=None, help='Optional: instance name for specific backup')
        return parser.parse_args()

class BackupFacade:
    def __init__(self, config):
        self.backup_command = BackupCommand(config)

    def run_backup(self, instance_name=None):
        self.backup_command.initiate_backup_process(instance_name)
        return True

    def print_instance_list(self, complete_info=None, pretty=None):
        self.backup_command.print_instance_list(complete_info, pretty)
        return True


class BackupCommand:
    def __init__(self, config):
        self.config = config

    def search_regex(self, pattern, text, idx):
        match = pattern.search(text)
        return match.group(idx) if match else None

    def extract_db_version(self, basedir):
        return self.search_regex(VERSION_PATTERN, basedir, 2) or 'unknown'

    def extract_db_type_from_line(self, line):
        line_lower = line.lower()
        if 'mariadb' in line_lower:
            return 'MariaDB'
        elif 'mysql' in line_lower:
            return 'MySQL'
        return None

    def get_backuptool_config(self, line):
        db_type = self.extract_db_type_from_line(line)

        if db_type and any(db in line for db in ['mysqld', 'mariadbd']):
            backuptool_config['db_type'] = db_type
            backuptool_config['defaults_file_path'] = self.search_regex(DEFAULTS_FILE_PATTERN, line, 1)
            backuptool_config['basedir'] = self.search_regex(BASEDIR_PATTERN, line, 1)

            if backuptool_config['basedir']:
                # TARGET DB 버전에 맞는 BACKUPTOOL VERSION 설정
                backuptool_config['backup_tool_version'] = self.mapping_backuptool_version(db_type, self.extract_db_version(backuptool_config['basedir']))

                # MYSQL일경우 basedir을 xtrabackup 경로로 변경
                if db_type == 'MySQL':
                    backuptool_config['basedir'] = f"{self.config.DB_BINARY_PATH}/xtrabackup-{backuptool_config['backup_tool_version']}"

                # MariaDB 인데 버전이 10.1.26일 경우 basedir 변경
                if db_type == 'MariaDB' and backuptool_config['backup_tool_version'] == "10.1.26":
                    backuptool_config['basedir'] = f"{self.config.DB_BINARY_PATH}/mariadb-10.1.26"

                return backuptool_config

            logging.warning(f"Failed to extract backuptool config from {line}.")
            return None

    def mapping_backuptool_version(self, db_type, backup_tool_version):
        ''' MYSQL, MariaDB와 맞는 Backup tool version mapping '''
        return BACKUPTOOL_VERSION_MAPPING.get(db_type, {}).get(backup_tool_version, backup_tool_version)

    def execute_command(self, command):
        try:
            return subprocess.check_output(command, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to execute command: {command}. Error: {e}")
            return ""

    def get_db_processes(self, instance_name=None):
        cmd_parts = ["ps -ef", "grep socket", "grep -v grep"]

        if instance_name:
            cmd_parts.append(f"grep --color=never '{instance_name}'")

        cmd = " | ".join(cmd_parts)
        return self.execute_command(cmd)

    def run_backup_over_ssh(self, backup_cmd, remote_backup_path):
        try:
            remote_ssh_cmd = f"ssh -i {self.config.REMOTE_PEMKEY} {self.config.REMOTE_USER}@{self.config.REMOTE_BACKUP_SERVER_IP} 'cat > {remote_backup_path}.gz'"
            full_cmd = ' '.join(backup_cmd) + " | gzip | " + remote_ssh_cmd

            subprocess.run(full_cmd, shell=True, check=True, stderr=subprocess.STDOUT)
            logging.info("Backup successfully executed")

            return True

        except subprocess.CalledProcessError as e:
            logging.error(f"Backup command failed with return code: {e.returncode}")
            return False
        except Exception as e:
            logging.error(f"An unexpected error occurred during backup: {e}")
            return False

    def configure_backup_command(self, backuptool_config):
        ''' SET BACKUP TOOL OPTION '''
        backup_tool = os.path.join(backuptool_config['basedir'], 'bin', 'mariabackup' if backuptool_config['db_type'] == 'MariaDB' else "xtrabackup")
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        instance_name = backuptool_config['defaults_file_path'].rsplit('_', 1)[-1]
        backup_filename = f"{instance_name}_{backuptool_config['db_type']}_backup_{now}.xbstream"

        if instance_name == "hrd":
            remote_backup_path = os.path.join(self.config.REMOTE_HRD_BACKUP_DIR, backup_filename)
        else:
            remote_backup_path = os.path.join(self.config.REMOTE_BACKUP_DIR, backup_filename)

        backup_cmd = [
            backup_tool,
            f"--defaults-file={backuptool_config['defaults_file_path']}",
            "--user=root",
            f"--password='{self.config.DB_PASSWORD}'",
            "--backup",
            "--safe-slave-backup",
            "--slave-info",
            "--stream=xbstream",
            "--compress",
            f"--compress-threads={self.config.BACKUP_OPTIONS['compress_threads']}",
            f"--parallel={self.config.BACKUP_OPTIONS['parallel']}"
        ]

        return backup_cmd, remote_backup_path

    def execute_backup(self, backuptool_config):
        backup_cmd, remote_backup_path = self.configure_backup_command(backuptool_config)

        result = self.run_backup_over_ssh(backup_cmd, remote_backup_path)

        if result:
            logging.info(f"Backup successfully completed and stored at {remote_backup_path}")
        else:
            logging.error("Backup process failed")
            return False

        return True

    def get_backuptool_config(self, line):
        db_type = self.extract_db_type_from_line(line)
        backuptool_config = {}

        if db_type and any(db in line for db in ['mysqld', 'mariadbd']):
            backuptool_config['db_type'] = db_type
            backuptool_config['defaults_file_path'] = self.search_regex(DEFAULTS_FILE_PATTERN, line, 1)
            backuptool_config['basedir'] = self.search_regex(BASEDIR_PATTERN, line, 1)

            if backuptool_config['basedir']:
                # TARGET DB 버전에 맞는 BACKUPTOOL VERSION 설정
                backuptool_config['backup_tool_version'] = self.mapping_backuptool_version(db_type, self.extract_db_version(backuptool_config['basedir']))

                # MYSQL일경우 basedir을 xtrabackup 경로로 변경
                if db_type == 'MySQL':
                    backuptool_config['basedir'] = f"{self.config.DB_BINARY_PATH}/xtrabackup-{backuptool_config['backup_tool_version']}"

                # MariaDB 인데 버전이 10.1.26일 경우 basedir 변경
                if db_type == 'MariaDB' and backuptool_config['backup_tool_version'] == "10.1.26":
                    backuptool_config['basedir'] = f"{self.config.DB_BINARY_PATH}/mariadb-10.1.26"

                return backuptool_config

            logging.warning(f"Failed to extract backuptool config from {line}.")
            return None

    def initiate_backup_process(self, instance_name=None):
        process_list = self.get_db_processes(instance_name)

        for line in process_list.split('\n'):
            backuptool_config = self.get_backuptool_config(line)
            if backuptool_config:
                logging.info(f"Found {backuptool_config['db_type']} database at {backuptool_config['basedir']}. {backuptool_config['defaults_file_path']} Starting backup...")

                if not self.execute_backup(backuptool_config):
                    logging.warning(f"Backup failed for {backuptool_config}, continuing with next database.")
                else:
                    logging.info(f"Backup successful")


    def print_instance_list(self, complete_info, pretty):
        process_list = self.get_db_processes()
        num = 1

        for line in process_list.split('\n'):
            if line:
                backuptool_config = self.get_backuptool_config(line)
                instance_name = backuptool_config['defaults_file_path'].rsplit('_', 1)[-1]

                if complete_info and pretty:
                    print(f"{num}. {instance_name}")
                    pprint.pprint(backuptool_config)
                elif complete_info:
                    print(f"{num}. {instance_name} - {backuptool_config}")
                else:
                    print(f"{num}. {instance_name}")
                num += 1

def main():
    config = Config()
    backup_facade = BackupFacade(config)

    args = config.parse_arguments()

    if args.list and args.all and args.pretty:
        backup_facade.print_instance_list(True, True)
    elif args.list and args.all:
        backup_facade.print_instance_list(True)
    elif args.list:
        backup_facade.print_instance_list()
    elif args.backup:
        backup_facade.run_backup(args.instance_name)
    else:
        print("No valid operation provided.")


if __name__ == "__main__":
    main()
