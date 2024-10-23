import boto3
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import re
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import socket
import argparse

# 기본 설정값
DEFAULT_CONFIG = {
    "INSTANCE_IDENTIFIER": "eng-production-dbnode02",
    "REGION": "ap-northeast-2",
    "OUTPUT_DIR": "./aurora_logs",
    "STATS_OUTPUT_FILE": "./processed_slow_queries.log",
    "JSON_OUTPUT_FILE": "./processed_queries.json",
    "MIN_QUERY_TIME": 1.0,
    "ES_HOST": "http://10.80.68.115:9200",
    "ES_INDEX_PREFIX": "mysql-slowlog"
}

def setup_logger(name, log_file=None):
    """로깅 설정"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러 (지정된 경우)
    if log_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

class SlowQueryLogProcessor:
    def __init__(self, input_dir, stats_output_file, json_output_file, es_host=None, es_index_prefix='mysql-slowlog'):
        self.input_dir = input_dir
        self.stats_output_file = stats_output_file
        self.json_output_file = json_output_file
        self.es_host = es_host
        self.es_index_prefix = es_index_prefix
        self.logger = setup_logger('SlowQueryProcessor', 'slow_query_processor.log')
        if es_host:
            self.es = Elasticsearch([es_host])

    def is_start_line(self, line):
        """MySQL 시작 라인이나 메타데이터 라인인지 확인"""
        patterns = [
            "Version: 8.0.28",
            "started with:",
            "Tcp port:",
            "Time                Id Command    Argument"
        ]
        return any(pattern in line for pattern in patterns)

    def parse_query_block(self, lines):
        """쿼리 블록을 파싱하여 필요한 정보 추출"""
        result = {
            'timestamp': None,
            'user': None,
            'query_time': 0.0,
            'lock_time': 0.0,
            'rows_examined': 0,
            'rows_sent': 0,
            'query': []
        }

        try:
            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('# Time:'):
                    result['timestamp'] = line[7:].strip()
                elif line.startswith('# User@Host:'):
                    result['user'] = line[12:].strip()
                elif line.startswith('# Query_time:'):
                    try:
                        match = re.search(r'Query_time:\s*(\d+\.?\d*)\s+Lock_time:\s*(\d+\.?\d*)', line)
                        if match:
                            result['query_time'] = float(match.group(1))
                            result['lock_time'] = float(match.group(2))
                    except (ValueError, AttributeError) as e:
                        self.logger.warning(f"Query/Lock time 파싱 실패: {line} - {str(e)}")
                elif line.startswith('# Rows_examined:'):
                    try:
                        match = re.search(r'Rows_examined:\s*(\d+)\s+Rows_sent:\s*(\d+)', line)
                        if match:
                            result['rows_examined'] = int(match.group(1))
                            result['rows_sent'] = int(match.group(2))
                    except (ValueError, AttributeError) as e:
                        self.logger.warning(f"Rows 정보 파싱 실패: {line} - {str(e)}")
                elif not line.startswith('#'):
                    result['query'].append(line)

            result['query'] = ' '.join(result['query']).strip()

        except Exception as e:
            self.logger.error(f"쿼리 블록 파싱 중 에러 발생: {str(e)}")

        return result

    def clean_query(self, query):
        """쿼리에서 모든 메타데이터를 제거하고 실제 SQL만 추출"""
        try:
            patterns_to_remove = [
                r'Tcp port:.*?(?=\n|$)',
                r'Unix socket:.*?(?=\n|$)',
                r'Time\s+Id\s+Command\s+Argument.*?(?=\n|$)',
                r'\s*Id\s+Command\s+Argument.*?(?=\n|$)',
                r'use\s+\w+;\s*',
                r'SET\s+timestamp=\d+;\s*',
                r'/\*.*?\*/',
                r'[\r\n]+'
            ]

            clean = query
            for pattern in patterns_to_remove:
                clean = re.sub(pattern, ' ', clean, flags=re.IGNORECASE | re.DOTALL)

            clean = ' '.join(clean.split())
            clean = clean.rstrip(';')

            return clean.strip()

        except Exception as e:
            self.logger.warning(f"쿼리 클리닝 중 오류 발생: {str(e)}")
            return query

    def normalize_query(self, query):
        """쿼리 정규화: 실제 SQL 문만 추출하여 비교"""
        try:
            clean = self.clean_query(query)
            if not clean.upper().startswith('SELECT'):
                return None

            normalized = clean.upper()
            normalized = re.sub(r"'[^']*'", "'?'", normalized)
            normalized = re.sub(r"\d+", "?", normalized)
            normalized = re.sub(r'\s+', ' ', normalized)

            return normalized.strip()

        except Exception as e:
            self.logger.warning(f"쿼리 정규화 중 오류 발생: {str(e)}")
            return None

    def process_log_file(self, filepath):
        """개별 로그 파일 처리"""
        queries = []
        current_block = []

        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    if self.is_start_line(line):
                        continue

                    if line.startswith('# Time:'):
                        if current_block:
                            try:
                                parsed = self.parse_query_block(current_block)
                                if parsed.get('query') and 'SELECT' in parsed['query'].upper():
                                    parsed['query'] = self.clean_query(parsed['query'])
                                    if parsed['query'].strip():
                                        queries.append(parsed)
                            except Exception as e:
                                self.logger.warning(f"쿼리 블록 파싱 실패: {str(e)}")
                        current_block = []

                    current_block.append(line)

                if current_block:
                    parsed = self.parse_query_block(current_block)
                    if parsed.get('query') and 'SELECT' in parsed['query'].upper():
                        parsed['query'] = self.clean_query(parsed['query'])
                        if parsed['query'].strip():
                            queries.append(parsed)

        except Exception as e:
            self.logger.error(f"파일 읽기 실패 ({filepath}): {str(e)}")

        return queries

    def group_queries(self, queries):
        """쿼리를 그룹화하고 각 그룹의 통계를 계산"""
        query_groups = {}

        for query_info in queries:
            try:
                normalized_query = self.normalize_query(query_info['query'])
                if not normalized_query:
                    continue

                clean_original_query = self.clean_query(query_info['query'])
                if not clean_original_query:
                    continue

                if normalized_query not in query_groups:
                    query_groups[normalized_query] = {
                        'count': 1,
                        'total_time': query_info['query_time'],
                        'max_time': query_info['query_time'],
                        'min_time': query_info['query_time'],
                        'total_rows_examined': query_info['rows_examined'],
                        'total_rows_sent': query_info['rows_sent'],
                        'first_seen': query_info['timestamp'],
                        'last_seen': query_info['timestamp'],
                        'original_query': clean_original_query,
                        'user': query_info['user']
                    }
                else:
                    group = query_groups[normalized_query]
                    group['count'] += 1
                    group['total_time'] += query_info['query_time']
                    group['max_time'] = max(group['max_time'], query_info['query_time'])
                    group['min_time'] = min(group['min_time'], query_info['query_time'])
                    group['total_rows_examined'] += query_info['rows_examined']
                    group['total_rows_sent'] += query_info['rows_sent']
                    group['last_seen'] = query_info['timestamp']

                    if len(clean_original_query) < len(group['original_query']):
                        group['original_query'] = clean_original_query

            except Exception as e:
                self.logger.warning(f"쿼리 그룹화 중 오류 발생: {str(e)}")
                continue

        return self.format_query_groups(query_groups)

    def format_query_groups(self, query_groups):
        """쿼리 그룹 데이터를 최종 형식으로 변환"""
        result = []
        for normalized_query, group in query_groups.items():
            try:
                result.append({
                    'count': group['count'],
                    'avg_time': group['total_time'] / group['count'],
                    'max_time': group['max_time'],
                    'min_time': group['min_time'],
                    'total_time': group['total_time'],
                    'avg_rows_examined': group['total_rows_examined'] / group['count'],
                    'avg_rows_sent': group['total_rows_sent'] / group['count'],
                    'first_seen': group['first_seen'],
                    'last_seen': group['last_seen'],
                    'query': group['original_query'],
                    'user': group['user']
                })
            except Exception as e:
                self.logger.warning(f"결과 형식화 중 오류 발생: {str(e)}")
                continue

        return result

    def save_stats_format(self, queries, processed_files, total_queries):
        """통계 형식으로 파일 저장"""
        try:
            with open(self.stats_output_file, 'w', encoding='utf-8') as f:
                f.write(f"=== Slow Query Log Analysis ===\n")
                f.write(f"처리된 파일 수: {processed_files}\n")
                f.write(f"총 쿼리 수: {total_queries}\n")
                f.write(f"유니크 쿼리 수: {len(queries)}\n\n")

                for query in queries:
                    f.write(f"Count: {query['count']}\n")
                    f.write(f"Average Query Time: {query['avg_time']:.6f} sec\n")
                    f.write(f"Max Query Time: {query['max_time']:.6f} sec\n")
                    f.write(f"Min Query Time: {query['min_time']:.6f} sec\n")
                    f.write(f"Total Time: {query['total_time']:.6f} sec\n")
                    f.write(f"Average Rows Examined: {query['avg_rows_examined']:.1f}\n")
                    f.write(f"Average Rows Sent: {query['avg_rows_sent']:.1f}\n")
                    f.write(f"First Seen: {query['first_seen']}\n")
                    f.write(f"Last Seen: {query['last_seen']}\n")
                    f.write(f"User: {query['user']}\n")
                    f.write("Query:\n")
                    f.write(f"{query['query']}\n")
                    f.write("-" * 80 + "\n\n")

            self.logger.info(f"통계 결과가 {self.stats_output_file}에 저장되었습니다.")
        except Exception as e:
            self.logger.error(f"통계 파일 저장 중 에러 발생: {str(e)}")

    def convert_to_es_format(self, queries):
        """쿼리 데이터를 Elasticsearch 형식으로 변환"""
        es_formatted = []
        for query in queries:
            es_query = {
                "count": query['count'],
                "time": {
                    "avg": query['avg_time'],
                    "total": query['total_time']
                },
                "rows": {
                    "avg": query['avg_rows_examined'],
                    "total": query['avg_rows_examined'] * query['count']
                },
                "query": query['query'],
                "timestamp": datetime.now().isoformat(),
                "user": query['user'],
                "first_seen": query['first_seen'],
                "last_seen": query['last_seen']
            }
            es_formatted.append(es_query)
        return es_formatted

    def save_json_format(self, es_queries):
        """JSON 형식으로 파일 저장"""
        try:
            with open(self.json_output_file, 'w', encoding='utf-8') as f:
                json.dump({"queries": es_queries}, f, ensure_ascii=False, indent=2)
            self.logger.info(f"JSON 형식 결과가 {self.json_output_file}에 저장되었습니다.")
        except Exception as e:
            self.logger.error(f"JSON 파일 저장 중 에러 발생: {str(e)}")

    def index_to_elasticsearch(self, es_queries):
        """Elasticsearch에 데이터 인덱싱"""
        if not self.es_host:
            return

        try:
            dynamic_index = f"{self.es_index_prefix}-{socket.gethostname()}}"
            actions = [
                {
                    "_index": dynamic_index,
                    "_source": query,
                }
                for query in es_queries
            ]
            success, _ = bulk(self.es, actions)
            self.logger.info(f"{success}개의 쿼리가 Elasticsearch 인덱스 '{dynamic_index}'에 성공적으로 인덱싱되었습니다.")
        except Exception as e:
            self.logger.error(f"Elasticsearch 인덱싱 중 에러 발생: {str(e)}")

    def process_all_logs(self, min_query_time=0):
        """모든 로그 파일 처리 및 결과 저장"""
        all_queries = []
        processed_files = 0

        try:
            # 로그 파일 처리
            for filename in os.listdir(self.input_dir):
                if 'slowquery' in filename.lower():
                    filepath = os.path.join(self.input_dir, filename)
                    self.logger.info(f"Processing file: {filename}")

                    try:
                        queries = self.process_log_file(filepath)
                        if queries:
                            all_queries.extend(queries)
                            processed_files += 1
                    except Exception as e:
                        self.logger.error(f"파일 처리 실패 ({filename}): {str(e)}")
                        continue

            if not all_queries:
                self.logger.warning("처리된 쿼리가 없습니다.")
                return

            # 최소 쿼리 시간으로 필터링
            filtered_queries = [q for q in all_queries if q['query_time'] >= min_query_time]

            if not filtered_queries:
                self.logger.warning(f"최소 쿼리 시간({min_query_time}초) 이상의 쿼리가 없습니다.")
                return

            # 쿼리 그룹화 및 통계 계산
            grouped_queries = self.group_queries(filtered_queries)

            if not grouped_queries:
                self.logger.warning("그룹화된 쿼리가 없습니다.")
                return

            # 평균 실행 시간으로 정렬
            sorted_queries = sorted(grouped_queries, key=lambda x: x['avg_time'], reverse=True)

            # 1. 통계 형식으로 저장
            self.save_stats_format(sorted_queries, processed_files, len(filtered_queries))

            # 2. Elasticsearch 형식으로 변환
            es_queries = self.convert_to_es_format(sorted_queries)

            # 3. JSON 파일로 저장
            self.save_json_format(es_queries)

            # 4. Elasticsearch로 전송
            if self.es_host:
                self.index_to_elasticsearch(es_queries)

            self.logger.info("모든 처리가 완료되었습니다.")

        except Exception as e:
            self.logger.error(f"로그 처리 중 에러 발생: {str(e)}")
            raise

    def download_slow_query_logs(instance_identifier, region, output_dir='./logs'):
        """Aurora 인스턴스의 슬로우 쿼리 로그 파일을 다운로드"""
        logger = setup_logger('LogDownloader')

        try:
            rds_client = boto3.client('rds', region_name=region)
            os.makedirs(output_dir, exist_ok=True)

            logger.info(f"인스턴스 {instance_identifier}의 로그 파일 조회 중...")

            response = rds_client.describe_db_log_files(
                DBInstanceIdentifier=instance_identifier,
                FilenameContains='slowquery'
            )

            if not response['DescribeDBLogFiles']:
                logger.info("슬로우 쿼리 로그 파일이 없습니다.")
                return False

            logger.info("\n=== 발견된 로그 파일 ===")
            for log_file in response['DescribeDBLogFiles']:
                size_mb = log_file['Size'] / (1024 * 1024)
                last_written = datetime.fromtimestamp(log_file['LastWritten']/1000)
                logger.info(f"파일명: {log_file['LogFileName']}")
                logger.info(f"크기: {size_mb:.2f}MB")
                logger.info(f"마지막 수정: {last_written}")
                logger.info("-" * 50)

            user_input = input("\n이 파일들을 다운로드하시겠습니까? (y/n): ")
            if user_input.lower() != 'y':
                logger.info("다운로드를 취소합니다.")
                return False

            for log_file in response['DescribeDBLogFiles']:
                log_filename = log_file['LogFileName']
                output_file = os.path.join(output_dir, os.path.basename(log_filename))

                logger.info(f"다운로드 중: {log_filename}")

                with open(output_file, 'w') as file:
                    marker = '0'
                    while True:
                        log_portion = rds_client.download_db_log_file_portion(
                            DBInstanceIdentifier=instance_identifier,
                            LogFileName=log_filename,
                            Marker=marker
                        )

                        if 'LogFileData' in log_portion:
                            file.write(log_portion['LogFileData'])

                        if not log_portion['AdditionalDataPending']:
                            break

                        marker = log_portion['Marker']

                logger.info(f"다운로드 완료: {output_file}")

            logger.info(f"\n모든 로그 파일이 {os.path.abspath(output_dir)} 디렉토리에 저장되었습니다.")
            return True

        except Exception as e:
            logger.error(f"에러 발생: {str(e)}")
            raise

    def main():
        """메인 실행 함수"""
        parser = argparse.ArgumentParser(description='MySQL Slow Query Log Processor')
        parser.add_argument('--instance-id', default=DEFAULT_CONFIG['INSTANCE_IDENTIFIER'],
                          help='RDS/Aurora instance identifier')
        parser.add_argument('--region', default=DEFAULT_CONFIG['REGION'],
                          help='AWS region')
        parser.add_argument('--input-dir', default=DEFAULT_CONFIG['OUTPUT_DIR'],
                          help='Directory containing slow query logs')
        parser.add_argument('--stats-output', default=DEFAULT_CONFIG['STATS_OUTPUT_FILE'],
                          help='Output file for statistics')
        parser.add_argument('--json-output', default=DEFAULT_CONFIG['JSON_OUTPUT_FILE'],
                          help='Output file for JSON format')
        parser.add_argument('--es-host', default=DEFAULT_CONFIG['ES_HOST'],
                          help='Elasticsearch host (optional)')
        parser.add_argument('--es-index-prefix', default=DEFAULT_CONFIG['ES_INDEX_PREFIX'],
                          help='Elasticsearch index prefix')
        parser.add_argument('--min-query-time', type=float, default=DEFAULT_CONFIG['MIN_QUERY_TIME'],
                          help='Minimum query time in seconds')

        args = parser.parse_args()

        if download_slow_query_logs(args.instance_id, args.region, args.input_dir):
            user_input = input("\n다운로드된 로그를 처리하시겠습니까? (y/n): ")
            if user_input.lower() == 'y':
                processor = SlowQueryLogProcessor(
                    input_dir=args.input_dir,
                    stats_output_file=args.stats_output,
                    json_output_file=args.json_output,
                    es_host=args.es_host,
                    es_index_prefix=args.es_index_prefix
                )
                processor.process_all_logs(min_query_time=args.min_query_time)
            else:
                print("로그 처리를 건너뜁니다.")
        else:
            print("로그 처리를 건너뜁니다.")

    if __name__ == "__main__":
        main()
