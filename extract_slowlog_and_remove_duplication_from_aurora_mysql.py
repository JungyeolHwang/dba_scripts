import boto3
import os
import logging
from datetime import datetime
import re

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SlowQueryLogProcessor:
    def __init__(self, input_dir, output_file):
        self.input_dir = input_dir
        self.output_file = output_file

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
            'query_time': 0.0,  # 기본값을 0.0으로 설정
            'lock_time': 0.0,  # 기본값을 0.0으로 설정
            'rows_examined': 0,  # 기본값을 0으로 설정
            'rows_sent': 0,    # 기본값을 0으로 설정
            'query': []
        }

        try:
            for line in lines:
                line = line.strip()
                if not line:  # 빈 라인 스킵
                    continue

                if line.startswith('# Time:'):
                    result['timestamp'] = line[7:].strip()
                elif line.startswith('# User@Host:'):
                    result['user'] = line[12:].strip()
                elif line.startswith('# Query_time:'):
                    # 정규식 패턴 개선 및 예외 처리 추가
                    try:
                        match = re.search(r'Query_time:\s*(\d+\.?\d*)\s+Lock_time:\s*(\d+\.?\d*)', line)
                        if match:
                            result['query_time'] = float(match.group(1))
                            result['lock_time'] = float(match.group(2))
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Query/Lock time 파싱 실패: {line} - {str(e)}")

                elif line.startswith('# Rows_examined:'):
                    try:
                        match = re.search(r'Rows_examined:\s*(\d+)\s+Rows_sent:\s*(\d+)', line)
                        if match:
                            result['rows_examined'] = int(match.group(1))
                            result['rows_sent'] = int(match.group(2))
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Rows 정보 파싱 실패: {line} - {str(e)}")

                elif not line.startswith('#'):
                    result['query'].append(line)

            result['query'] = ' '.join(result['query']).strip()

        except Exception as e:
            logger.error(f"쿼리 블록 파싱 중 에러 발생: {str(e)}")

        return result

    def clean_query(self, query):
        """
        쿼리에서 모든 메타데이터를 제거하고 실제 SQL만 추출
        """
        try:
            # 여러 줄의 메타데이터 패턴 제거
            patterns_to_remove = [
                r'Tcp port:.*?(?=\n|$)',  # Tcp port로 시작하는 라인
                r'Unix socket:.*?(?=\n|$)',  # Unix socket으로 시작하는 라인
                r'Time\s+Id\s+Command\s+Argument.*?(?=\n|$)',  # Time Id Command Argument 라인
                r'\s*Id\s+Command\s+Argument.*?(?=\n|$)',  # Id Command Argument 라인
                r'use\s+\w+;\s*',  # use 문
                r'SET\s+timestamp=\d+;\s*',  # SET timestamp 문
                r'/\*.*?\*/',  # SQL 주석
                r'[\r\n]+',  # 줄바꿈을 공백으로
            ]

            # 각 패턴 적용
            clean = query
            for pattern in patterns_to_remove:
                clean = re.sub(pattern, ' ', clean, flags=re.IGNORECASE | re.DOTALL)

            # 연속된 공백 제거 및 양쪽 공백 제거
            clean = ' '.join(clean.split())

            # 끝에 있는 세미콜론 제거
            clean = clean.rstrip(';')

            return clean.strip()

        except Exception as e:
            logger.warning(f"쿼리 클리닝 중 오류 발생: {str(e)}")
            return query

    def normalize_query(self, query):
        """
        쿼리 정규화: 실제 SQL 문만 추출하여 비교
        """
        try:
            # 먼저 쿼리 클리닝
            clean = self.clean_query(query)

            # SELECT 문장만 추출
            if not clean.upper().startswith('SELECT'):
                return None

            # 정규화
            normalized = clean.upper()  # 대문자로 통일

            # 변수 값들을 일반화
            normalized = re.sub(r"'[^']*'", "'?'", normalized)  # 문자열 값을 '?'로 대체
            normalized = re.sub(r"\d+", "?", normalized)        # 숫자를 ?로 대체
            normalized = re.sub(r'\s+', ' ', normalized)        # 연속된 공백을 하나로

            return normalized.strip()

        except Exception as e:
            logger.warning(f"쿼리 정규화 중 오류 발생: {str(e)}")
            return None


    def group_queries(self, queries):
        """쿼리를 그룹화하고 각 그룹의 통계를 계산"""
        query_groups = {}

        for query_info in queries:
            try:
                # 쿼리 정규화
                normalized_query = self.normalize_query(query_info['query'])
                if not normalized_query:  # 빈 쿼리 무시
                    continue

                # 원본 쿼리에서 메타데이터 제거
                clean_original_query = query_info['query'].split('Tcp port:')[0].strip()
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

                    # 더 깨끗한 형태의 쿼리로 업데이트
                    if len(clean_original_query) < len(group['original_query']):
                        group['original_query'] = clean_original_query

            except Exception as e:
                logger.warning(f"쿼리 그룹화 중 오류 발생: {str(e)}")
                continue

        # 결과 형식화
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
                logger.warning(f"결과 형식화 중 오류 발생: {str(e)}")
                continue

        return result

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
                        # 이전 블록 처리
                        if current_block:
                            try:
                                parsed = self.parse_query_block(current_block)
                                if parsed.get('query'):  # 쿼리가 있는 경우만
                                    if 'SELECT' in parsed['query'].upper():
                                        # 메타데이터 제거
                                        parsed['query'] = self.clean_query(parsed['query'])
                                        if parsed['query'].strip():
                                            queries.append(parsed)
                            except Exception as e:
                                logger.warning(f"쿼리 블록 파싱 실패: {str(e)}")
                        current_block = []

                    current_block.append(line)

                # 마지막 블록 처리
                if current_block:
                    try:
                        parsed = self.parse_query_block(current_block)
                        if parsed.get('query'):
                            if 'SELECT' in parsed['query'].upper():
                                parsed['query'] = self.clean_query(parsed['query'])
                                if parsed['query'].strip():
                                    queries.append(parsed)
                    except Exception as e:
                        logger.warning(f"마지막 쿼리 블록 파싱 실패: {str(e)}")

        except Exception as e:
            logger.error(f"파일 읽기 실패 ({filepath}): {str(e)}")

        return queries

    def process_all_logs(self, min_query_time=0):
        """모든 로그 파일 처리 및 결과 저장"""
        all_queries = []
        processed_files = 0

        try:
            # 로그 파일 처리
            for filename in os.listdir(self.input_dir):
                if 'slowquery' in filename.lower():
                    filepath = os.path.join(self.input_dir, filename)
                    logger.info(f"Processing file: {filename}")

                    try:
                        queries = self.process_log_file(filepath)
                        if queries:  # None이나 빈 리스트가 아닌 경우에만 추가
                            all_queries.extend(queries)
                            processed_files += 1
                    except Exception as e:
                        logger.error(f"파일 처리 실패 ({filename}): {str(e)}")
                        continue

            if not all_queries:
                logger.warning("처리된 쿼리가 없습니다.")
                return

            # 최소 쿼리 시간으로 필터링
            filtered_queries = [q for q in all_queries if q['query_time'] >= min_query_time]

            if not filtered_queries:
                logger.warning(f"최소 쿼리 시간({min_query_time}초) 이상의 쿼리가 없습니다.")
                return

            # 쿼리 그룹화 및 통계 계산
            grouped_queries = self.group_queries(filtered_queries)

            if not grouped_queries:
                logger.warning("그룹화된 쿼리가 없습니다.")
                return

            # 평균 실행 시간으로 정렬
            sorted_queries = sorted(grouped_queries, key=lambda x: x['avg_time'], reverse=True)

            # 결과 저장
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write(f"=== Slow Query Log Analysis ===\n")
                f.write(f"처리된 파일 수: {processed_files}\n")
                f.write(f"총 쿼리 수: {len(filtered_queries)}\n")
                f.write(f"유니크 쿼리 수: {len(grouped_queries)}\n")
                f.write(f"최소 쿼리 시간: {min_query_time} 초\n\n")

                for query in sorted_queries:
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

            logger.info(f"처리된 파일 수: {processed_files}")
            logger.info(f"총 쿼리 수: {len(filtered_queries)}")
            logger.info(f"유니크 쿼리 수: {len(grouped_queries)}")
            logger.info(f"결과가 {self.output_file}에 저장되었습니다.")

        except Exception as e:
            logger.error(f"로그 처리 중 에러 발생: {str(e)}")
            raise

def download_slow_query_logs(instance_identifier, region, output_dir='./logs'):
    """
    Aurora 인스턴스의 슬로우 쿼리 로그 파일을 다운로드합니다.
    """
    try:
        # RDS 클라이언트 생성
        rds_client = boto3.client('rds', region_name=region)

        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"인스턴스 {instance_identifier}의 로그 파일 조회 중...")

        # 로그 파일 목록 조회
        response = rds_client.describe_db_log_files(
            DBInstanceIdentifier=instance_identifier,
            FilenameContains='slowquery'
        )

        if not response['DescribeDBLogFiles']:
            logger.info("슬로우 쿼리 로그 파일이 없습니다.")
            return False

        # 로그 파일 정보 출력
        logger.info("\n=== 발견된 로그 파일 ===")
        for log_file in response['DescribeDBLogFiles']:
            size_mb = log_file['Size'] / (1024 * 1024)
            last_written = datetime.fromtimestamp(log_file['LastWritten']/1000)
            logger.info(f"파일명: {log_file['LogFileName']}")
            logger.info(f"크기: {size_mb:.2f}MB")
            logger.info(f"마지막 수정: {last_written}")
            logger.info("-" * 50)

        # 다운로드 여부 확인
        user_input = input("\n이 파일들을 다운로드하시겠습니까? (y/n): ")
        if user_input.lower() != 'y':
            logger.info("다운로드를 취소합니다.")
            return False

        # 각 로그 파일 다운로드
        for log_file in response['DescribeDBLogFiles']:
            log_filename = log_file['LogFileName']
            output_file = os.path.join(output_dir, os.path.basename(log_filename))

            logger.info(f"다운로드 중: {log_filename}")

            # 파일 다운로드
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

if __name__ == "__main__":
    # 설정값
    INSTANCE_IDENTIFIER = "eng-production-dbnode02"  # Writer 인스턴스 ID
    REGION = "ap-northeast-2"
    OUTPUT_DIR = "./aurora_logs"
    PROCESSED_OUTPUT_FILE = "./processed_slow_queries.log"  # 처리된 결과 파일
    MIN_QUERY_TIME = 1.0  # 최소 쿼리 시간 (초)

    # 로그 다운로드
    if download_slow_query_logs(INSTANCE_IDENTIFIER, REGION, OUTPUT_DIR):
        # 다운로드 성공 시 로그 처리
        user_input = input("\n다운로드된 로그를 처리하시겠습니까? (y/n): ")
        if user_input.lower() == 'y':
            processor = SlowQueryLogProcessor(OUTPUT_DIR, PROCESSED_OUTPUT_FILE)
            processor.process_all_logs(min_query_time=MIN_QUERY_TIME)
            logger.info("모든 처리가 완료되었습니다.")
        else:
            logger.info("로그 처리를 건너뜁니다.")
    else:
        logger.info("로그 처리를 건너뜁니다.")