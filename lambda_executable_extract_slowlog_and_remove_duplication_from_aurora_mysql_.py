from dataclasses import dataclass
from typing import Optional, List, Dict, Generator, Any, Tuple, Set
import boto3
import os
import logging
import json
from datetime import datetime, timedelta
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import socket
from functools import wraps
import resource
import hashlib  # 추가된 import

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dataclass
class Config:
    """어플리케이션 설정"""
    instance_id: str
    region: str = "ap-northeast-2"
    min_query_time: float = 0.1
    es_host: str = "http://10.80.68.115:9200"
    es_index_prefix: str = "mysql-slowlog"
    es_connection_timeout: int = 30
    es_retry_count: int = 3
    batch_size: int = 1000
    
    @classmethod
    def from_event(cls, event: Dict[str, Any]) -> 'Config':
        """이벤트에서 설정 생성"""
        return cls(
            instance_id=event.get('instance_id') or os.environ.get('INSTANCE_ID'),
            region=event.get('region') or os.environ.get('REGION', "ap-northeast-2"),
            min_query_time=float(event.get('min_query_time') or os.environ.get('MIN_QUERY_TIME', 1.0)),
            es_host=event.get('es_host') or os.environ.get('ES_HOST'),
            es_index_prefix=event.get('es_index_prefix') or os.environ.get('ES_INDEX_PREFIX', "mysql-slowlog"),
            es_connection_timeout=int(event.get('es_connection_timeout') or os.environ.get('ES_CONNECTION_TIMEOUT', 30)),
            es_retry_count=int(event.get('es_retry_count') or os.environ.get('ES_RETRY_COUNT', 3)),
            batch_size=int(event.get('batch_size') or os.environ.get('BATCH_SIZE', 1000))
        )

def monitor_memory(func):
    """메모리 사용량 모니터링 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        
        result = func(*args, **kwargs)
        
        end_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        memory_used = (end_mem - start_mem) / 1024  # MB 단위
        
        logger.info(f"Function {func.__name__} used {memory_used:.2f} MB of memory")
        return result
    return wrapper

def get_elasticsearch_client(config: Config) -> Elasticsearch:
    """Elasticsearch 클라이언트 생성"""
    try:
        es_client = Elasticsearch(
            [config.es_host],
            retry_on_timeout=True,
            timeout=config.es_connection_timeout,
            max_retries=config.es_retry_count
        )
        
        if not es_client.ping():
            raise Exception("Elasticsearch 연결 테스트 실패")
            
        return es_client
        
    except Exception as e:
        logger.error(f"Elasticsearch 연결 실패: {str(e)}")
        raise

def setup_index_mapping(es_client: Elasticsearch, index_name: str) -> None:
    """인덱스 매핑 설정"""
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "keyword"},
                        "host": {"type": "keyword"}
                    }
                },
                "query": {"type": "text"},
                "normalized_query": {"type": "text"},
                "query_hash": {"type": "keyword"},
                "query_time": {"type": "float"},
                "lock_time": {"type": "float"},
                "rows_examined": {"type": "integer"},
                "rows_sent": {"type": "integer"},
                "execution_count": {"type": "integer"},
                "timestamps": {"type": "date"},
                "max_query_time": {"type": "float"},
                "last_seen": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
    }
    
    try:
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"Successfully created index {index_name} with mapping")
    except Exception as e:
        logger.error(f"Error setting up index mapping: {str(e)}")
        raise

class QueryNormalizer:
    """SQL 쿼리 정규화 클래스"""
    
    def __init__(self):
        self._init_patterns()
        self._init_keywords()
    
    def _init_patterns(self) -> None:
        """정규식 패턴 초기화"""
        self._patterns = {
            # 기본 패턴
            'whitespace': re.compile(r'\s+'),
            'comments': re.compile(r'/\*.*?\*/|--[^\n]*'),
            'semicolon': re.compile(r';+$'),
            
            # 쿼리 요소 패턴
            'date_format': re.compile(r"DATE_FORMAT\s*\([^,]+,\s*'[^']*'\)", re.IGNORECASE),
            'numbers': re.compile(r'\b\d+\b'),
            'quoted_strings': re.compile(r"'[^']*'"),
            'in_clause': re.compile(r'IN\s*\([^)]+\)', re.IGNORECASE),
            
            # 식별자 패턴
            'table_aliases': re.compile(
                r'\b(?:AS\s+)?'  # AS (선택사항)
                r'(?:'          # 그룹 시작
                r'[`"][\w\s]+[`"]'  # 백틱/따옴표로 감싸진 별칭
                r'|'            # 또는
                r'[\w$]+)'      # 일반 별칭
                r'\b', 
                re.IGNORECASE
            ),
            'column_aliases': re.compile(
                r'\b(?:'        # 단어 경계와 그룹 시작
                r'[`"][\w\s]+[`"]'  # 백틱/따옴표로 감싸진 별칭
                r'|'            # 또는
                r'[\w$]+)'      # 일반 별칭
                r'\.'           # 점
                r'[\w$]+\b',    # 컬럼명
                re.IGNORECASE
            )
        }
    
    def _init_keywords(self) -> None:
        """SQL 키워드 초기화"""
        self._keywords = {
            # DDL 키워드
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME',
            # DML 키워드
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
            # 조인 관련 키워드
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'NATURAL', 'ON',
            # 조건절 키워드
            'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
            # 그룹/정렬 키워드
            'GROUP BY', 'HAVING', 'ORDER BY', 'ASC', 'DESC',
            # 기타 키워드
            'AS', 'DISTINCT', 'UNION', 'ALL', 'LIMIT', 'OFFSET'
        }
        # 정규식 패턴 생성 (긴 키워드가 먼저 매칭되도록 길이 기준 정렬)
        keywords_pattern = '|'.join(sorted(self._keywords, key=len, reverse=True))
        self._patterns['keywords'] = re.compile(r'\b({0})\b'.format(keywords_pattern), re.IGNORECASE)

    def _normalize_identifiers(self, query: str) -> str:
        """식별자(테이블명, 컬럼명 등) 정규화"""
        try:
            # 테이블 별칭 정규화
            def normalize_table_alias(match):
                alias = match.group(0)
                # 백틱/따옴표로 감싸진 경우
                if alias.startswith('`') or alias.startswith('"'):
                    return alias.upper()
                # AS가 포함된 경우
                if ' AS ' in alias.upper():
                    parts = alias.split(maxsplit=2)
                    return f"{parts[0]} AS {parts[2].upper()}"
                return alias.upper()

            # 컬럼 참조 정규화
            def normalize_column_ref(match):
                ref = match.group(0)
                # 백틱/따옴표로 감싸진 경우
                if ref.startswith('`') or ref.startswith('"'):
                    return ref.upper()
                return ref.upper()

            query = self._patterns['table_aliases'].sub(normalize_table_alias, query)
            query = self._patterns['column_aliases'].sub(normalize_column_ref, query)
            return query

        except Exception as e:
            logger.error(f"식별자 정규화 중 오류 발생: {str(e)}")
            return query

    def _normalize_date_format(self, query: str) -> str:
        """DATE_FORMAT 함수 정규화"""
        def replace_date_format(match):
            # DATE_FORMAT 함수 전체를 추출하고 정규화
            func = match.group(0)
            # 첫 번째 인자와 포맷 문자열 분리
            parts = func.split(',', 1)
            if len(parts) == 2:
                # 첫 번째 인자는 그대로 유지하고 포맷 문자열을 ?로 대체
                return "{0}, '?')".format(parts[0].strip())
            return func
        return self._patterns['date_format'].sub(replace_date_format, query)

    def _normalize_in_clause(self, query: str) -> str:
        """IN 절 정규화"""
        def replace_in_clause(match):
            content = match.group(0)
            is_not = 'NOT IN' in content.upper()
            prefix = 'NOT IN' if is_not else 'IN'
            
            # 괄호 내용 추출 및 정규화
            start_idx = content.find('(')
            end_idx = content.rfind(')')
            if start_idx != -1 and end_idx != -1:
                # 값들을 추출하고 정규화
                values = [v.strip().strip("'").strip('"') for v in content[start_idx+1:end_idx].split(',')]
                # 정렬하고 대문자로 변환
                sorted_values = sorted(map(str.upper, values))
                # 문자열로 결합 (f-string 대신 .format 사용)
                values_str = ", ".join("'{0}'".format(v) for v in sorted_values)
                return "{0} ({1})".format(prefix, values_str)
            return content
        
        return self._patterns['in_clause'].sub(replace_in_clause, query)

    def normalize_query(self, query: str) -> str:
        """쿼리 문자열 정규화"""
        try:
            # SET TIMESTAMP 문 제거
            query = re.sub(r'SET TIMESTAMP=\d+;', '', query)
            
            # USE 문 제거
            query = re.sub(r'USE \w+;', '', query)
            
            # 기존 정규화 로직
            query = query.strip()
            query = self._patterns['semicolon'].sub('', query)
            query = self._patterns['comments'].sub('', query)
            query = self._patterns['whitespace'].sub(' ', query)
            query = query.upper()
            
            # SQL 키워드 정규화
            query = self._patterns['keywords'].sub(lambda m: m.group(0).upper(), query)
            
            # 각 요소 정규화
            query = self._normalize_date_format(query)
            query = self._normalize_in_clause(query)
            query = self._normalize_identifiers(query)
            
            # 최종 공백 처리
            query = ' '.join(query.split())
            
            return query
                
        except Exception as e:
            logger.error(f"쿼리 정규화 중 오류 발생: {str(e)}")
            return query.strip()

    def generate_hash(self, query: str) -> str:
        """정규화된 쿼리의 해시값 생성"""
        normalized_query = self.normalize_query(query)
        # 디버그를 위한 로깅 추가
        logger.debug("Original query: {0}".format(query))
        logger.debug("Normalized query: {0}".format(normalized_query))
        return hashlib.md5(normalized_query.encode()).hexdigest()
        
class SlowQueryLogProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.es_client = get_elasticsearch_client(config) if config.es_host else None
        self.query_normalizer = QueryNormalizer()
        self._compile_regex_patterns()
        
    def _compile_regex_patterns(self) -> None:
        """정규식 패턴 컴파일"""
        self._patterns = {
            'literals': re.compile(r"'[^']*'"),
            'numbers': re.compile(r"\d+"),
            'whitespace': re.compile(r'\s+'),
            'start_lines': re.compile(r"(Version: 8\.0\.28|started with:|Tcp port:|Time\s+Id Command\s+Argument)")
        }
        
    def stream_log_files(self, instance_identifier: str, region: str) -> Generator[str, None, None]:
        """RDS 로그 파일을 스트리밍 방식으로 읽기"""
        rds_client = boto3.client('rds', region_name=region)
        start_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
        
        try:
            response = rds_client.describe_db_log_files(
                DBInstanceIdentifier=instance_identifier,
                FilenameContains='slowquery',
                FileLastWritten=start_time
            )
            
            for log_file in response['DescribeDBLogFiles']:
                log_filename = log_file['LogFileName']
                marker = '0'
                
                while True:
                    log_portion = rds_client.download_db_log_file_portion(
                        DBInstanceIdentifier=instance_identifier,
                        LogFileName=log_filename,
                        Marker=marker
                    )
                    
                    if 'LogFileData' in log_portion:
                        yield log_portion['LogFileData']
                    
                    if not log_portion['AdditionalDataPending']:
                        break
                        
                    marker = log_portion['Marker']
                    
        except Exception as e:
            logger.error(f"로그 스트리밍 중 에러 발생: {str(e)}")
            raise

    def process_streaming_content(self, content_stream: Generator[str, None, None]) -> Generator[Dict[str, Any], None, None]:
        """스트리밍 컨텐츠 처리"""
        current_block: List[str] = []
        
        for chunk in content_stream:
            lines = chunk.split('\n')
            
            for line in lines:
                if self._patterns['start_lines'].search(line):
                    continue
                    
                if line.startswith('# Time:'):
                    if current_block:
                        parsed = self._parse_query_block(current_block)
                        if self._is_valid_query(parsed):
                            yield parsed
                    current_block = []
                    
                current_block.append(line)
                
        if current_block:
            parsed = self._parse_query_block(current_block)
            if self._is_valid_query(parsed):
                yield parsed

    def _parse_query_block(self, lines: List[str]) -> Dict[str, Any]:
        """쿼리 블록 파싱"""
        result = {
            'timestamp': None,
            'user': {
                'name': None,
                'host': None
            },
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
                    user_host = line[12:].strip()
                    # User@Host: user[user] @ localhost [127.0.0.1] 형식 파싱
                    user_match = re.search(r'(\w+)\[\w+\]\s*@\s*([\w\.-]+)', user_host)
                    if user_match:
                        result['user']['name'] = user_match.group(1)
                        result['user']['host'] = user_match.group(2)
                elif line.startswith('# Query_time:'):
                    match = re.search(r'Query_time:\s*(\d+\.?\d*)\s+Lock_time:\s*(\d+\.?\d*)', line)
                    if match:
                        result['query_time'] = float(match.group(1))
                        result['lock_time'] = float(match.group(2))
                elif line.startswith('# Rows_examined:'):
                    match = re.search(r'Rows_examined:\s*(\d+)\s+Rows_sent:\s*(\d+)', line)
                    if match:
                        result['rows_examined'] = int(match.group(1))
                        result['rows_sent'] = int(match.group(2))
                elif not line.startswith('#'):
                    result['query'].append(line)

            result['query'] = self._clean_query(' '.join(result['query']))

        except Exception as e:
            logger.error(f"쿼리 블록 파싱 중 에러 발생: {str(e)}")

        return result

    def _clean_query(self, query: str) -> str:
        """쿼리 문자열 정규화"""
        try:
            return self.query_normalizer.normalize_query(query)
        except Exception as e:
            logger.warning("쿼리 클리닝 중 오류 발생: {0}".format(str(e)))
            return query.strip()
    
    def _generate_query_hash(self, query: str) -> str:
        """안정적인 쿼리 해시 생성"""
        return self.query_normalizer.generate_hash(query)
        
    def _is_valid_query(self, parsed: Dict[str, Any]) -> bool:
        """쿼리 유효성 검사"""
        return (
            parsed.get('query') and 
            'SELECT' in parsed['query'].upper() and 
            parsed['query_time'] >= self.config.min_query_time
        )

    def stream_to_elasticsearch(self, query_stream: Generator[Dict[str, Any], None, None]) -> Generator[int, None, None]:
        """쿼리를 Elasticsearch로 스트리밍"""
        if not self.es_client:
            return
            
        batch: List[Dict[str, Any]] = []
        
        for query in query_stream:
            batch.append(query)
            
            if len(batch) >= self.config.batch_size:
                success_count = self._index_batch(batch)
                yield success_count
                batch = []
        
        if batch:
            success_count = self._index_batch(batch)
            yield success_count

    def _index_batch(self, batch: List[Dict[str, Any]]) -> int:
        """배치 인덱싱 with upsert"""
        try:
            index_name = f"{self.config.es_index_prefix}-eng-dbnode02"
            
            # 인덱스 매핑 설정
            setup_index_mapping(self.es_client, index_name)
            
            actions = []
            for query_data in batch:
                # 쿼리 정규화 및 해시 생성
                original_query = query_data['query']
                normalized_query = self.query_normalizer.normalize_query(original_query)
                # 원본 쿼리도 정규화 패턴 적용
                cleaned_query = re.sub(r'USE \w+;\s*', '', original_query)
                cleaned_query = re.sub(r'SET TIMESTAMP=\d+;\s*', '', cleaned_query)
                query_hash = self.query_normalizer.generate_hash(normalized_query)
                
                current_timestamp = query_data['timestamp']
                
                # update 작업으로 변경
                doc = {
                    '_op_type': 'update',
                    '_index': index_name,
                    '_id': query_hash,
                    'script': {
                        'source': '''
                            ctx._source.last_seen = params.timestamp;
                            ctx._source.execution_count = (ctx._source.execution_count ?: 0) + 1;
                            ctx._source.max_query_time = Math.max(ctx._source.max_query_time, params.query_time);
                            if (ctx._source.timestamps === null) ctx._source.timestamps = [];
                            if (!ctx._source.timestamps.contains(params.timestamp)) {
                                ctx._source.timestamps.add(params.timestamp);
                            }
                        ''',
                        'params': {
                            'timestamp': current_timestamp,
                            'query_time': query_data['query_time']
                        }
                    },
                    'upsert': {
                        'query': cleaned_query,  # 정규화된 원본 쿼리 저장
                        'normalized_query': normalized_query,
                        'query_hash': query_hash,
                        'timestamp': current_timestamp,
                        'user': query_data['user'],
                        'query_time': query_data['query_time'],
                        'lock_time': query_data['lock_time'],
                        'rows_examined': query_data['rows_examined'],
                        'rows_sent': query_data['rows_sent'],
                        'execution_count': 1,
                        'timestamps': [current_timestamp],
                        'max_query_time': query_data['query_time'],
                        'last_seen': current_timestamp
                    }
                }
                
                actions.append(doc)
            
            # 벌크 작업 실행
            if actions:
                success, errors = bulk(self.es_client, actions, stats_only=True, refresh=True)
                logger.info(f"배치 인덱싱 완료 - 성공: {success}, 실패: {errors}")
                return success
            return 0
                
        except Exception as e:
            logger.error(f"배치 인덱싱 중 에러 발생: {str(e)}")
            return 0

@monitor_memory
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 핸들러"""
    try:
        # 설정 초기화
        config = Config.from_event(event)
        processor = SlowQueryLogProcessor(config)
        
        # CloudWatch 클라이언트
        cloudwatch = boto3.client('cloudwatch')
        
        # 처리 카운터
        total_processed = 0
        total_indexed = 0
        
        # 로그 스트리밍 처리
        log_stream = processor.stream_log_files(config.instance_id, config.region)
        query_stream = processor.process_streaming_content(log_stream)
        
        # Elasticsearch 인덱싱
        for batch_size in processor.stream_to_elasticsearch(query_stream):
            total_indexed += batch_size
            total_processed += batch_size
            
            # CloudWatch 메트릭 업데이트
            # cloudwatch.put_metric_data(
            #     Namespace='SlowQueryProcessor',
            #     MetricData=[{
            #         'MetricName': 'ProcessedQueries',
            #         'Value': batch_size,
            #         'Unit': 'Count'
            #     }]
            # )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'total_processed': total_processed,
                'total_indexed': total_indexed,
                'message': 'Successfully processed slow query logs'
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda 실행 중 에러 발생: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error processing slow query logs'
            })
        }