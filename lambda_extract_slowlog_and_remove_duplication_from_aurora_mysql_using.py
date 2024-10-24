from dataclasses import dataclass
from typing import Optional, List, Dict, Generator, Any, Tuple, Set, Protocol
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
import hashlib
from abc import ABC, abstractmethod

# 로깅 설정을 별도 함수로 분리
def setup_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger

logger = setup_logging()

# Protocol 정의로 인터페이스 명확화
class QueryNormalizerProtocol(Protocol):
    def normalize_query(self, query: str) -> str: ...
    def generate_hash(self, query: str) -> str: ...

# 기본 설정값을 상수로 분리
DEFAULT_CONFIG = {
    "region": "ap-northeast-2",
    "min_query_time": 1.0,
    "es_index_prefix": "mysql-slowlog",
    "es_connection_timeout": 30,
    "es_retry_count": 3,
    "batch_size": 1000
}

@dataclass
class Config:
    """어플리케이션 설정"""
    instance_id: str
    region: str = DEFAULT_CONFIG["region"]
    min_query_time: float = DEFAULT_CONFIG["min_query_time"]
    es_host: str = ""
    es_index_prefix: str = DEFAULT_CONFIG["es_index_prefix"]
    es_connection_timeout: int = DEFAULT_CONFIG["es_connection_timeout"]
    es_retry_count: int = DEFAULT_CONFIG["es_retry_count"]
    batch_size: int = DEFAULT_CONFIG["batch_size"]
    
    @classmethod
    def from_event(cls, event: Dict[str, Any]) -> 'Config':
        """이벤트에서 설정 생성"""
        return cls(
            instance_id=event.get('instance_id') or os.environ.get('INSTANCE_ID'),
            region=event.get('region') or os.environ.get('REGION', DEFAULT_CONFIG["region"]),
            min_query_time=float(event.get('min_query_time') or os.environ.get('MIN_QUERY_TIME', DEFAULT_CONFIG["min_query_time"])),
            es_host=event.get('es_host') or os.environ.get('ES_HOST'),
            es_index_prefix=event.get('es_index_prefix') or os.environ.get('ES_INDEX_PREFIX', DEFAULT_CONFIG["es_index_prefix"]),
            es_connection_timeout=int(event.get('es_connection_timeout') or os.environ.get('ES_CONNECTION_TIMEOUT', DEFAULT_CONFIG["es_connection_timeout"])),
            es_retry_count=int(event.get('es_retry_count') or os.environ.get('ES_RETRY_COUNT', DEFAULT_CONFIG["es_retry_count"])),
            batch_size=int(event.get('batch_size') or os.environ.get('BATCH_SIZE', DEFAULT_CONFIG["batch_size"]))
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

class QueryNormalizer:
    """SQL 쿼리 정규화 클래스"""
    def __init__(self):
        self._init_patterns()
        self._init_keywords()
    
    def _init_patterns(self) -> None:
        """정규식 패턴 초기화"""
        self._patterns = {
            'whitespace': re.compile(r'\s+'),
            'comments': re.compile(r'/\*.*?\*/|--[^\n]*'),
            'semicolon': re.compile(r';+$'),
            'date_format': re.compile(r"DATE_FORMAT\s*\([^,]+,\s*'[^']*'\)", re.IGNORECASE),
            'numbers': re.compile(r'\b\d+\b'),
            'quoted_strings': re.compile(r"'[^']*'"),
            'in_clause': re.compile(r'IN\s*\([^)]+\)', re.IGNORECASE),
            'table_aliases': re.compile(
                r'\b(?:AS\s+)?(?:[`"][\w\s]+[`"]|[\w$]+)\b', 
                re.IGNORECASE
            ),
            'column_aliases': re.compile(
                r'\b(?:[`"][\w\s]+[`"]|[\w$]+)\.[\w$]+\b',
                re.IGNORECASE
            )
        }
    
    def _init_keywords(self) -> None:
        """SQL 키워드 초기화"""
        self._keywords = {
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME',
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'NATURAL', 'ON',
            'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
            'GROUP BY', 'HAVING', 'ORDER BY', 'ASC', 'DESC',
            'AS', 'DISTINCT', 'UNION', 'ALL', 'LIMIT', 'OFFSET'
        }
        keywords_pattern = '|'.join(sorted(self._keywords, key=len, reverse=True))
        self._patterns['keywords'] = re.compile(r'\b({0})\b'.format(keywords_pattern), re.IGNORECASE)

    def normalize_query(self, query: str) -> str:
        """쿼리 문자열 정규화"""
        try:
            # SET TIMESTAMP 문 제거
            query = re.sub(r'SET TIMESTAMP=\d+;', '', query)
            # USE 문 제거
            query = re.sub(r'USE \w+;', '', query)
            
            query = query.strip()
            query = self._patterns['semicolon'].sub('', query)
            query = self._patterns['comments'].sub('', query)
            query = self._patterns['whitespace'].sub(' ', query)
            query = query.upper()
            query = self._normalize_date_format(query)
            query = self._normalize_in_clause(query)
            query = self._normalize_identifiers(query)
            return ' '.join(query.split())
        except Exception as e:
            logger.error(f"쿼리 정규화 중 오류 발생: {str(e)}")
            return query.strip()

    def generate_hash(self, query: str) -> str:
        """정규화된 쿼리의 해시값 생성"""
        normalized_query = self.normalize_query(query)
        return hashlib.md5(normalized_query.encode()).hexdigest()

    def _normalize_identifiers(self, query: str) -> str:
        """식별자 정규화"""
        for pattern_name in ['table_aliases', 'column_aliases']:
            query = self._patterns[pattern_name].sub(lambda m: m.group(0).upper(), query)
        return query

    def _normalize_date_format(self, query: str) -> str:
        """DATE_FORMAT 함수 정규화"""
        return self._patterns['date_format'].sub(
            lambda m: f"{m.group(0).split(',')[0].strip()}, '?')", 
            query
        )

    def _normalize_in_clause(self, query: str) -> str:
        """IN 절 정규화"""
        def replace_in_clause(match):
            content = match.group(0)
            is_not = 'NOT IN' in content.upper()
            prefix = 'NOT IN' if is_not else 'IN'
            
            start_idx = content.find('(')
            end_idx = content.rfind(')')
            if start_idx != -1 and end_idx != -1:
                values = [v.strip().strip("'").strip('"') for v in content[start_idx+1:end_idx].split(',')]
                sorted_values = sorted(map(str.upper, values))
                values_str = ", ".join("'{0}'".format(v) for v in sorted_values)
                return f"{prefix} ({values_str})"
            return content
        
        return self._patterns['in_clause'].sub(replace_in_clause, query)
        
class ElasticsearchManager:
    """Elasticsearch 관리 클래스"""
    def __init__(self, config: Config):
        self.config = config
        self.es_client = self._setup_client()

    def _setup_client(self) -> Optional[Elasticsearch]:
        """Elasticsearch 클라이언트 설정"""
        if not self.config.es_host:
            return None
            
        try:
            es_client = Elasticsearch(
                [self.config.es_host],
                retry_on_timeout=True,
                timeout=self.config.es_connection_timeout,
                max_retries=self.config.es_retry_count
            )
            
            if not es_client.ping():
                raise Exception("Elasticsearch 연결 테스트 실패")
                
            return es_client
            
        except Exception as e:
            logger.error(f"Elasticsearch 연결 실패: {str(e)}")
            return None

    def setup_index_mapping(self, index_name: str) -> None:
        """인덱스 매핑 설정"""
        if not self.es_client:
            return

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
            if not self.es_client.indices.exists(index=index_name):
                self.es_client.indices.create(index=index_name, body=mapping)
                logger.info(f"Successfully created index {index_name} with mapping")
        except Exception as e:
            logger.error(f"Error setting up index mapping: {str(e)}")
            raise

    def bulk_index(self, documents: List[Dict[str, Any]], index_name: str) -> int:
        """문서 일괄 인덱싱"""
        if not self.es_client or not documents:
            return 0

        try:
            success, errors = bulk(self.es_client, documents, stats_only=True, refresh=True)
            logger.info(f"Bulk indexing completed - success: {success}, errors: {errors}")
            return success
        except Exception as e:
            logger.error(f"Bulk indexing failed: {str(e)}")
            return 0

class LogStreamProcessor:
    """로그 스트림 처리 클래스"""
    def __init__(self, config: Config, query_normalizer: QueryNormalizerProtocol):
        self.config = config
        self.query_normalizer = query_normalizer
        self._patterns = self._compile_regex_patterns()

    @staticmethod
    def _compile_regex_patterns() -> Dict[str, re.Pattern]:
        return {
            'start_lines': re.compile(r"(Version: 8\.0\.28|started with:|Tcp port:|Time\s+Id Command\s+Argument)"),
            'time_line': re.compile(r"^# Time:"),
            'user_host': re.compile(r"# User@Host:\s*(\w+)\[\w+\]\s*@\s*([\w\.-]+)"),
            'query_stats': re.compile(r"Query_time:\s*(\d+\.?\d*)\s+Lock_time:\s*(\d+\.?\d*)"),
            'rows_stats': re.compile(r"Rows_examined:\s*(\d+)\s+Rows_sent:\s*(\d+)")
        }

    def stream_log_files(self) -> Generator[str, None, None]:
        """RDS 로그 파일 스트리밍"""
        rds_client = boto3.client('rds', region_name=self.config.region)
        start_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
        
        try:
            response = rds_client.describe_db_log_files(
                DBInstanceIdentifier=self.config.instance_id,
                FilenameContains='slowquery',
                FileLastWritten=start_time
            )
            
            for log_file in response['DescribeDBLogFiles']:
                yield from self._process_log_file(rds_client, log_file)
                    
        except Exception as e:
            logger.error(f"로그 스트리밍 중 에러 발생: {str(e)}")
            raise

    def _process_log_file(self, rds_client: Any, log_file: Dict[str, Any]) -> Generator[str, None, None]:
        """개별 로그 파일 처리"""
        marker = '0'
        while True:
            log_portion = rds_client.download_db_log_file_portion(
                DBInstanceIdentifier=self.config.instance_id,
                LogFileName=log_file['LogFileName'],
                Marker=marker
            )
            
            if 'LogFileData' in log_portion:
                yield log_portion['LogFileData']
            
            if not log_portion['AdditionalDataPending']:
                break
                
            marker = log_portion['Marker']

    def process_streaming_content(self, content_stream: Generator[str, None, None]) -> Generator[Dict[str, Any], None, None]:
        """스트리밍 컨텐츠 처리"""
        current_block: List[str] = []
        
        try:
            for chunk in content_stream:
                for line in chunk.split('\n'):
                    if self._patterns['start_lines'].search(line):
                        continue
                        
                    if self._patterns['time_line'].match(line):
                        if current_block:
                            parsed_data = self._parse_query_block(current_block)
                            if self._is_valid_query(parsed_data):
                                yield parsed_data
                        current_block = []
                        
                    current_block.append(line)
                    
            if current_block:
                parsed_data = self._parse_query_block(current_block)
                if self._is_valid_query(parsed_data):
                    yield parsed_data
                    
        except Exception as e:
            logger.error(f"스트리밍 컨텐츠 처리 중 오류 발생: {str(e)}")
            raise

    def _parse_query_block(self, lines: List[str]) -> Dict[str, Any]:
        """쿼리 블록 파싱"""
        result = {
            'timestamp': None,
            'user': {'name': None, 'host': None},
            'query_time': 0.0,
            'lock_time': 0.0,
            'rows_examined': 0,
            'rows_sent': 0,
            'query': []
        }

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith('# Time:'):
                result['timestamp'] = line[7:].strip()
            elif line.startswith('# User@Host:'):
                if match := self._patterns['user_host'].search(line):
                    result['user']['name'] = match.group(1)
                    result['user']['host'] = match.group(2)
            elif line.startswith('# Query_time:'):
                if match := self._patterns['query_stats'].search(line):
                    result['query_time'] = float(match.group(1))
                    result['lock_time'] = float(match.group(2))
            elif line.startswith('# Rows_examined:'):
                if match := self._patterns['rows_stats'].search(line):
                    result['rows_examined'] = int(match.group(1))
                    result['rows_sent'] = int(match.group(2))
            elif not line.startswith('#'):
                result['query'].append(line)

        if result['query']:
            result['query'] = self.query_normalizer.normalize_query(' '.join(result['query']))
            
        return result

    def _is_valid_query(self, parsed: Dict[str, Any]) -> bool:
        """쿼리 유효성 검사"""
        return (
            parsed.get('query') and 
            'SELECT' in parsed['query'].upper() and 
            parsed['query_time'] >= self.config.min_query_time
        )

class SlowQueryLogProcessor:
    """슬로우 쿼리 로그 처리기"""
    def __init__(self, config: Config, normalizer: QueryNormalizerProtocol = None):  # Protocol 사용
        self.config = config
        self.query_normalizer = normalizer or QueryNormalizer()
        self.es_manager = ElasticsearchManager(config)
        self.stream_processor = LogStreamProcessor(config, self.query_normalizer)
        self._batch: List[Dict[str, Any]] = []

    def process_logs(self) -> Dict[str, int]:
        """로그 처리 실행"""
        total_processed = 0
        total_indexed = 0
        
        try:
            log_stream = self.stream_processor.stream_log_files()
            
            for query_data in self.stream_processor.process_streaming_content(log_stream):
                es_doc = self._prepare_es_document(query_data)
                self._batch.append(es_doc)
                total_processed += 1
                
                if len(self._batch) >= self.config.batch_size:
                    indexed = self._index_batch()
                    total_indexed += indexed
                    self._batch = []
            
            if self._batch:
                indexed = self._index_batch()
                total_indexed += indexed
            
            return {'processed': total_processed, 'indexed': total_indexed}
            
        except Exception as e:
            logger.error(f"로그 처리 중 에러 발생: {str(e)}")
            raise

    def _prepare_es_document(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """Elasticsearch 문서 준비"""
        normalized_query = self.query_normalizer.normalize_query(query_data['query'])
        query_hash = self.query_normalizer.generate_hash(normalized_query)
        
        return {
            '_op_type': 'update',
            '_index': f"{self.config.es_index_prefix}-eng-dbnode02",
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
                    'timestamp': query_data['timestamp'],
                    'query_time': query_data['query_time']
                }
            },
            'upsert': {
                'query': query_data['query'],
                'normalized_query': normalized_query,
                'query_hash': query_hash,
                'timestamp': query_data['timestamp'],
                'user': query_data['user'],
                'query_time': query_data['query_time'],
                'lock_time': query_data['lock_time'],
                'rows_examined': query_data['rows_examined'],
                'rows_sent': query_data.get('rows_sent', 0),
                'execution_count': 1,
                'timestamps': [query_data['timestamp']],
                'max_query_time': query_data['query_time'],
                'last_seen': query_data['timestamp']
            }
        }

    def _index_batch(self) -> int:
        """배치 인덱싱 실행"""
        if not self._batch:
            return 0
            
        index_name = f"{self.config.es_index_prefix}-eng-dbnode02"
        self.es_manager.setup_index_mapping(index_name)
        return self.es_manager.bulk_index(self._batch, index_name)

@monitor_memory
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 핸들러"""
    try:
        config = Config.from_event(event)
        processor = SlowQueryLogProcessor(config)
        
        results = processor.process_logs()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'total_processed': results['processed'],
                'total_indexed': results['indexed'],
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
