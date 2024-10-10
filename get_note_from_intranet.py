import os
import mysql.connector
import json
import time
import requests
import logging
import boto3
import datetime
import functools
import asyncio
import httpx
import time
from html import escape
from retry import retry  # 'retry' library needed

# Logging setup
logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s - %(message)s')
# logging.basicConfig(filename='/app/logs/app.log', level=logging.INFO, format='%(asctime)s - %(message)s')

logger = logging.getLogger(__name__)

console = logging.StreamHandler()
logger.addHandler(console)

def error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except mysql.connector.Error as db_err:
            logging.error(f'A database error occured in {func.__name__}: {db_err}')
            raise
        except requests.exceptions.RequestException as api_err:
            logging.error(f'An API error occurred in {func.__name__}: {api_err}')
            raise
        except Exception as e:
            logging.error(f'An error occured in {func.__name__}: {e}')
            raise

    return wrapper


def async_error_handler(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.error(f'An Async error occured in {func.__name__}: {e}')
            raise
    return wrapper


class Utility:

        # AWS Secrets Manager API info
        @staticmethod
        def get_secret():
                session = boto3.session.Session()
                client = session.client(
                        service_name='secretsmanager',
                        region_name='your_region'
                )
                get_secret_value_response = client.get_secret_value(
                        SecretId='your_secret_id'
                )
                secret = json.loads(get_secret_value_response['SecretString'])
                return secret['API_KEY']

        @staticmethod
        def datetime_handler(x):
                 if isinstance(x, datetime.datetime):
                  return x.isoformat()

                 raise TypeError("unknown type")


        # Read configuration file
        @staticmethod
        def load_config():
                with open('config.json', 'r') as f:
                        config = json.load(f)

                return config



class MysqlManager:

    def __init__(self):
        config = Utility.load_config()
        test_flag = 'native'

        if test_flag == 'docker':
                self.host = os.environ.get('MYSQL_HOST')
                self.user = os.environ.get('MYSQL_USER')
                self.password = os.environ.get('MYSQL_PASSWORD')
                self.database = os.environ.get('MYSQL_DATABASE')
        else:
                self.host = config['MYSQL_HOST']
                self.user = config['MYSQL_USER']
                self.password = config['MYSQL_PASSWORD']
                self.database = config['MYSQL_DATABASE']
                self.server_flag = config['SERVER_FLAG']  # intranet server type

    def connect_to_database(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    @error_handler
    def fetch_last_id(self, cursor, conn):
        # get current last_id
        cursor.execute("SELECT last_id FROM intrahac.NOTE_ID_APIGATEWAY_MNG")
        result = cursor.fetchone()
        last_id = result['last_id']

        return last_id

    @error_handler
    def fetch_notes(self, last_id, cursor, conn):

        server_flag = os.environ.get('SERVER_FLAG')  if os.environ.get('SERVER_FLAG') else self.server_flag
        limit = 10;

        # get note data
        cursor.execute(f"SELECT HM_CATEGORY, HM_RECV_USERID, HM_SEND_USERID, HM_SEND_REGDATE, HM_ID, HM_MEMO, '{server_flag}' AS SERVER_FLAG FROM intrahac.HAC6_MEMO WHERE HM_ID > %s ORDER BY HM_ID ASC LIMIT %s", (last_id, limit))
        records = cursor.fetchall()

        if not records:
         return []

        return records

    @error_handler
    def update_last_id(self, cursor, last_processed_id, conn):
        try:
                cursor.execute("UPDATE intrahac.NOTE_ID_APIGATEWAY_MNG SET last_id = %s", (last_processed_id,))
                logger.info(f'UPDATE rowcount : {cursor.rowcount}')
                conn.commit()
        except mysql.connector.Error as err:
                logger.info(f'last_id UPDATE ERROR : {err}')
                conn.rollback()


class ApiGatewayManager:
    def __init__(self):
        config = Utility.load_config()
        test_flag = 'native'

        if test_flag == 'docker':
                self.api_url = os.environ.get('API_URL')
                self.api_key = os.environ.get('API_KEY')
        else:
                self.api_url = config['API_URL']
                self.api_key = config['API_KEY']

        self.headers = {'Content-Type' : 'application/json', 'x-api-key' : self.api_key}

    # sync retry handler
    @retry(tries=3, delay=2, backoff=1)
    def post_data_with_retry(self, payload):
        if not self.post_data_to_apigateway(payload):
            raise Exception("API Gateway call failed.")
        else:
            logger.info('API CALL SUCCESS')

    # Async retry handler
    async def post_data_with_retry_async(self, payload, retries=3, delay=2):
        logger.info(f'called post_data_with_retry_async')

        for i in range(retries):
            try:
                return await self.post_batch_data_to_apigateway_async(payload)
            except Exception as e:
                if i == retries - 1:
                    raise Exception(f"API Gateway Async Call Retry {i+1}/{retries} failed: {e}")

                await asyncio.sleep(delay)

    # post with sync
    @error_handler
    def post_data_to_apigateway(self, payload):
        payload = json.dumps(payload, default=Utility.datetime_handler, indent=4)
        logger.info(f'payload : {payload}')

        response = requests.post(self.api_url, headers=self.headers, data=payload)
        logger.info(f'response = {response.status_code}')

        return response.status_code == 200

    # post with Async
    @async_error_handler
    async def post_batch_data_to_apigateway_async(self, payload):
        logger.info(f'payload : {payload}')
        payload = json.dumps(payload, default=Utility.datetime_handler, indent=4)

        async with httpx.AsyncClient() as client:
            response = await client.post(self.api_url, headers=self.headers, data=payload)
            logger.info(f'Async response = {response.status_code}')
            return response.status_code == 200

class BusinessService:
    def __init__(self, api_manager, mysql_manager):
        self.api_manager = api_manager
        self.mysql_manager = mysql_manager

    def send_mysql_data_to_dynamo(self):
        # Create mysql connection, cursor
        with mysql_manager.connect_to_database() as conn:
          with conn.cursor(dictionary=True) as cursor:

           while True:
            # Get last_id
            last_id = self.mysql_manager.fetch_last_id(cursor, conn)

            # Get Intranet's note data
            result = self.mysql_manager.fetch_notes(last_id, cursor, conn)

            if result:
            	# update new last_id
            	last_processed_id = result[-1]['HM_ID']
            	logger.info(f'update new last_id: {last_processed_id}')
            	self.mysql_manager.update_last_id(cursor, last_processed_id, conn)

            	# Post note to dynamo
            	self.api_manager.post_data_with_retry(result)

    # use async
    async def send_async_mysql_data_to_dynamo(self):
        with self.mysql_manager.connect_to_database() as conn:
                with conn.cursor(dictionary=True) as cursor:

                        # Change ISOLATION level
                        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;")

                        while True:
                                # Get last_id
                                last_id = self.mysql_manager.fetch_last_id(cursor, conn)

                                # Get notes
                                result = self.mysql_manager.fetch_notes(last_id, cursor, conn)

                                if result:
                                        last_processed_id = result[-1]['HM_ID']
                                        logger.info(f'update new last_id: {last_processed_id}')

                                        # Update last_id
                                        self.mysql_manager.update_last_id(cursor, last_processed_id, conn)

                                        await self.api_manager.post_data_with_retry_async(result)
                                        logger.info('Async send complete')
                                else:
                                    logger.info('Not found result')
                                    time.sleep(3)

async def main():
        # Initialize
        api_manager = ApiGatewayManager()
        mysql_manager = MysqlManager()
        service = BusinessService(api_manager, mysql_manager)

        try:
                # service.send_mysql_data_to_dynamo()
                await service.send_async_mysql_data_to_dynamo()

        except Exception as e:
                logging.critical(f"Max retries reached. Error: {e}")


if __name__ == '__main__':
        asyncio.run(main())

