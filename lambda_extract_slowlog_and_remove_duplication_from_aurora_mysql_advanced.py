# config.py
from dataclasses import dataclass
from typing import Dict, Any
import os
import re
import hashlib
import logging
from typing import Dict, List, Generator, Any, Optional, Pattern
from datetime import datetime
import boto3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
from functools import wraps
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) 

DEFAULT_CONFIG = {
    "region": "ap-northeast-2",
    "min_query_time": 0.1,
    "es_index_prefix": "mysql-slowlog",
    "es_connection_timeout": 30,
    "es_retry_count": 3,
    "batch_size": 1000,
    "max_buffer_size_mb": 10,
    "chunk_size": 1000
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
    max_buffer_size_mb: int = DEFAULT_CONFIG["max_buffer_size_mb"]
    chunk_size: int = DEFAULT_CONFIG["chunk_size"]
    
    @classmethod
    def from_event(cls, event: Dict[str, Any]) -> 'Config':
        return cls(
            instance_id=event.get('instance_id') or os.environ.get('INSTANCE_ID'),
            region=event.get('region') or os.environ.get('REGION', DEFAULT_CONFIG["region"]),
            min_query_time=float(event.get('min_query_time') or os.environ.get('MIN_QUERY_TIME', DEFAULT_CONFIG["min_query_time"])),
            es_host=event.get('es_host') or os.environ.get('ES_HOST'),
            es_index_prefix=event.get('es_index_prefix') or os.environ.get('ES_INDEX_PREFIX', DEFAULT_CONFIG["es_index_prefix"]),
            es_connection_timeout=int(event.get('es_connection_timeout') or os.environ.get('ES_CONNECTION_TIMEOUT', DEFAULT_CONFIG["es_connection_timeout"])),
            es_retry_count=int(event.get('es_retry_count') or os.environ.get('ES_RETRY_COUNT', DEFAULT_CONFIG["es_retry_count"])),
            batch_size=int(event.get('batch_size') or os.environ.get('BATCH_SIZE', DEFAULT_CONFIG["batch_size"])),
            max_buffer_size_mb=int(event.get('max_buffer_size_mb') or os.environ.get('MAX_BUFFER_SIZE_MB', DEFAULT_CONFIG["max_buffer_size_mb"])),
            chunk_size=int(event.get('chunk_size') or os.environ.get('CHUNK_SIZE', DEFAULT_CONFIG["chunk_size"]))
        )

    def validate(self) -> None:
        if not self.instance_id:
            raise ValueError("instance_id is required")
        if self.min_query_time <= 0:
            raise ValueError("min_query_time must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_buffer_size_mb <= 0:
            raise ValueError("max_buffer_size_mb must be positive")
            
class QueryNormalizer:
    """SQL 쿼리 정규화 클래스"""
    def __init__(self):
        self._init_patterns()
        self._init_keywords()
        self._query_cache: Dict[str, str] = {}
        self._hash_cache: Dict[str, str] = {}
        self.cache_hits = 0
        self.cache_misses = 0
    
    def _init_patterns(self) -> None:
        self._patterns = {
            'whitespace': re.compile(r'\s+'),
            'comments': re.compile(r'/\*.*?\*/|--[^\n]*'),
            'semicolon': re.compile(r';+$'),
            'date_format': re.compile(r"DATE_FORMAT\s*\([^,]+,\s*'[^']*'\)", re.IGNORECASE),
            'numbers': re.compile(r'\b\d+\b'),
            'quoted_strings': re.compile(r"'[^']*'"),
            'in_clause': re.compile(r'IN\s*\([^)]+\)', re.IGNORECASE),
            'table_aliases': re.compile(r'\b(?:AS\s+)?(?:[`"][\w\s]+[`"]|[\w$]+)\b', re.IGNORECASE),
            'column_aliases': re.compile(r'\b(?:[`"][\w\s]+[`"]|[\w$]+)\.[\w$]+\b', re.IGNORECASE),
            'use_statement': re.compile(r'(?i)^use\s+[\w_]+\s*;?\s*'),
            'set_timestamp': re.compile(r'SET\s+timestamp\s*=\s*\d+\s*;?\s*', re.IGNORECASE)
        }
    
    def _init_keywords(self) -> None:
        self._keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WHERE', 
            'FROM', 'JOIN', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT'
        }
        keywords_pattern = '|'.join(sorted(self._keywords, key=len, reverse=True))
        self._patterns['keywords'] = re.compile(r'\b({0})\b'.format(keywords_pattern), re.IGNORECASE)

    def normalize_query(self, query: str) -> str:
        if query in self._query_cache:
            self.cache_hits += 1
            return self._query_cache[query]
            
        self.cache_misses += 1
        normalized = self._normalize_query_impl(query)
        
        # 정규화 전후 비교 로깅
        logger.debug(f"Original query: {query}")
        logger.debug(f"Normalized query: {normalized}")
        
        if len(self._query_cache) <= 10000:
            self._query_cache[query] = normalized
            
        return normalized

    def _normalize_query_impl(self, query: str) -> str:
        # USE 구문과 SET TIMESTAMP 구문 제거
        query = self._patterns['use_statement'].sub('', query)
        query = self._patterns['set_timestamp'].sub('', query)
            
        # 주요 정규화 단계들
        query = self._patterns['comments'].sub('', query)
        query = self._patterns['whitespace'].sub(' ', query)
        query = self._patterns['semicolon'].sub('', query)
        query = self._patterns['numbers'].sub('?', query)
        query = self._patterns['quoted_strings'].sub('?', query)
        query = self._patterns['in_clause'].sub('IN (?)', query)
        
        # 결과 정리
        query = query.strip()
        
        # 디버그 로깅
        logger.debug(f"Normalized query result: {query}")
        
        return query.strip()

    def generate_hash(self, query: str) -> str:
        if query in self._hash_cache:
            return self._hash_cache[query]
            
        normalized_query = self.normalize_query(query)
        query_hash = hashlib.md5(normalized_query.encode()).hexdigest()
        
        if len(self._hash_cache) <= 10000:
            self._hash_cache[query] = query_hash
        
        return query_hash            
        
class LogStreamProcessor:
    def __init__(self, config: 'Config', query_normalizer: 'QueryNormalizer'):
        self.config = config
        self.query_normalizer = query_normalizer
        self._patterns = self._compile_regex_patterns()
        self._buffer = []
        self._buffer_size = 0
        self._max_buffer_size = self.config.max_buffer_size_mb * 1024 * 1024
        self._large_query_stats = {
            'skipped_queries': 0,
            'largest_query_size': 0,
            'large_query_timestamps': []
        }

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
        rds_client = boto3.client('rds', region_name=self.config.region)
        try:
            response = rds_client.describe_db_log_files(
                DBInstanceIdentifier=self.config.instance_id,
                FilenameContains='slowquery'
            )
            
            for log_file in response['DescribeDBLogFiles']:
                yield from self._process_log_file(rds_client, log_file)
                    
        except Exception as e:
            logger.error(f"로그 스트리밍 중 에러 발생: {str(e)}")
            raise

    # LogStreamProcessor 클래스에 추가
    def _process_log_file(self, rds_client: Any, log_file: Dict[str, Any]) -> Generator[str, None, None]:
        """개별 로그 파일 처리"""
        marker = '0'
        partial_content = ""  # 이전 청크에서 남은 불완전한 내용 저장
        
        while True:
            try:
                log_portion = rds_client.download_db_log_file_portion(
                    DBInstanceIdentifier=self.config.instance_id,
                    LogFileName=log_file['LogFileName'],
                    Marker=marker,
                    NumberOfLines=1000
                )
                
                if 'LogFileData' in log_portion:
                    content = partial_content + log_portion['LogFileData']
                    
                    # 마지막 완전한 쿼리 블록의 끝 위치 찾기
                    last_complete_block = content.rfind('\n# Time:')
                    
                    if last_complete_block != -1:
                        # 완전한 블록들만 yield
                        yield content[:last_complete_block]
                        # 나머지 부분은 다음 반복에서 사용
                        partial_content = content[last_complete_block:]
                    else:
                        partial_content = content
                
                if not log_portion.get('AdditionalDataPending', False):
                    if partial_content:  # 마지막 부분 처리
                        yield partial_content
                    break
                    
                marker = log_portion['Marker']
                
            except Exception as e:
                logger.error(f"로그 파일 부분 다운로드 중 에러 발생: {str(e)}")
                raise
            
    def process_streaming_content(self, content_stream: Generator[str, None, None]) -> Generator[Dict[str, Any], None, None]:
        current_block: List[str] = []
        buffer_size = 0
        current_query_metadata = {}
        
        try:
            for chunk in content_stream:
                for line in chunk.split('\n'):
                    line_size = len(line.encode('utf-8'))
                    
                    # 메타데이터 처리
                    if line.startswith('# Time:'):
                        if current_block:
                            yield from self._handle_query_block(
                                current_block,
                                buffer_size,
                                current_query_metadata
                            )
                        current_block = []
                        buffer_size = 0
                        current_query_metadata = {'timestamp': line[7:].strip()}
                        
                    elif line.startswith('# Query_time:'):
                        if match := self._patterns['query_stats'].search(line):
                            current_query_metadata['query_time'] = float(match.group(1))
                    
                    # 버퍼 크기 체크 및 처리
                    if buffer_size + line_size > self._max_buffer_size:
                        if current_block:
                            compressed_block = self._try_compress_query(current_block)
                            if compressed_block:
                                yield from self._handle_query_block(
                                    compressed_block,
                                    len(''.join(compressed_block).encode('utf-8')),
                                    current_query_metadata
                                )
                        current_block = []
                        buffer_size = 0
                        continue
                    
                    if not self._patterns['start_lines'].search(line):
                        current_block.append(line)
                        buffer_size += line_size
                        
            # 마지막 블록 처리
            if current_block:
                yield from self._handle_query_block(
                    current_block,
                    buffer_size,
                    current_query_metadata
                )
                    
        except Exception as e:
            logger.error(f"스트리밍 컨텐츠 처리 중 오류 발생: {str(e)}")
            raise

    def _handle_query_block(self, query_block: List[str], buffer_size: int, metadata: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        if not query_block:
            return
            
        try:
            parsed_data = self._parse_query_block(query_block)
            if metadata:
                parsed_data.update(metadata)
                
            if self._is_valid_query(parsed_data):
                yield parsed_data
                
        except Exception as e:
            logger.error(f"쿼리 블록 처리 중 오류 발생: {str(e)}")
            self._update_error_stats(str(e), metadata.get('timestamp'))        
            
    def _update_error_stats(self, error_msg: str, timestamp: Optional[str]) -> None:
        """에러 통계 업데이트"""
        if not hasattr(self, '_error_stats'):
            self._error_stats = {
                'total_errors': 0,
                'error_types': {},
                'error_timestamps': []
            }
        
        self._error_stats['total_errors'] += 1
        self._error_stats['error_types'][error_msg] = self._error_stats['error_types'].get(error_msg, 0) + 1
        
        if timestamp:
            self._error_stats['error_timestamps'].append({
                'timestamp': timestamp,
                'error': error_msg
            })

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

        query_lines = []
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
            elif not line.startswith('#'):  # 실제 쿼리 라인
                query_lines.append(line)

        # 쿼리 라인들을 하나의 문자열로 결합
        if query_lines:
            result['query'] = ' '.join(query_lines)
                
        return result
    
    def _try_compress_query(self, query_block: List[str]) -> Optional[List[str]]:
        """
        긴 쿼리 블록을 압축하여 처리 가능한 크기로 만듭니다.
        
        Args:
            query_block (List[str]): 압축할 쿼리 블록
            
        Returns:
            Optional[List[str]]: 압축된 쿼리 블록. 압축이 불가능한 경우 None 반환
        """
        try:
            # 메타데이터 라인과 쿼리 라인 분리
            metadata_lines = []
            query_lines = []
            
            for line in query_block:
                if line.startswith('#'):
                    metadata_lines.append(line)
                else:
                    query_lines.append(line)
                    
            if not query_lines:  # 쿼리가 없는 경우
                return None
                
            # 쿼리 라인 합치기
            full_query = ' '.join(query_lines)
            
            # 쿼리 압축 시도
            compressed_query = self._compress_query_content(full_query)
            
            if compressed_query:
                # 메타데이터와 압축된 쿼리 합치기
                return metadata_lines + [compressed_query]
                
            return None
            
        except Exception as e:
            logger.error(f"쿼리 압축 중 오류 발생: {str(e)}")
            return None
        
    def _compress_query_content(self, query: str) -> Optional[str]:
        """
        긴 쿼리를 압축하는 로직
        
        Args:
            query (str): 압축할 쿼리 문자열
            
        Returns:
            Optional[str]: 압축된 쿼리. 압축이 불가능한 경우 None 반환
        """
        try:
            # 기본적인 압축 규칙들
            compression_rules = [
                # 불필요한 공백 제거
                (r'\s+', ' '),
                # IN 절 압축 (IN (1,2,3,4,5) -> IN (...))
                (r'IN\s*\([^)]+\)', 'IN (...)'),
                # VALUES 절 압축
                (r'VALUES\s*\([^)]+\)', 'VALUES (...)'),
                # CASE WHEN 절 단순화
                (r'CASE\s+WHEN.+?END', 'CASE ... END'),
                # 긴 문자열 리터럴 압축
                (r"'[^']{100,}'", "'...'"),
                # 긴 숫자 리스트 압축
                (r'\d+(?:\s*,\s*\d+){5,}', '...'),
                # ORDER BY 절의 긴 컬럼 리스트 압축
                (r'ORDER BY.*?(,.*?){5,}', 'ORDER BY ...'),
                # GROUP BY 절의 긴 컬럼 리스트 압축
                (r'GROUP BY.*?(,.*?){5,}', 'GROUP BY ...')
            ]
            
            compressed = query
            for pattern, replacement in compression_rules:
                compressed = re.sub(pattern, replacement, compressed, flags=re.IGNORECASE)
                
            # 압축 후 크기가 원본의 50% 이상 줄어들지 않으면 압축 실패로 간주
            if len(compressed.encode('utf-8')) > len(query.encode('utf-8')) * 0.5:
                logger.warning("쿼리 압축 효과가 충분하지 않음")
                return None
                
            return compressed
            
        except Exception as e:
            logger.error(f"쿼리 내용 압축 중 오류 발생: {str(e)}")
            return None
            
    def _is_valid_query(self, parsed: Dict[str, Any]) -> bool:
        """
        쿼리 유효성 검사
        
        Args:
            parsed: 파싱된 쿼리 데이터 딕셔너리
            
        Returns:
            bool: 유효한 쿼리인지 여부
        """
        try:
            # 쿼리가 존재하는지 확인
            if not parsed.get('query'):
                return False
                
            # SELECT 쿼리인지 확인
            if 'SELECT' not in str(parsed['query']).upper():
                return False
                
            # 쿼리 실행 시간이 최소 시간을 초과하는지 확인
            query_time = float(parsed.get('query_time', 0.0))
            if query_time < self.config.min_query_time:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"쿼리 유효성 검사 중 오류 발생: {str(e)}")
            return False
            
class ElasticsearchManager:
    def __init__(self, config: 'Config'):
        self.config = config
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.es_client = self._setup_client()

    def _setup_client(self) -> Optional[Elasticsearch]:
        if not self.config.es_host:
            self.logger.error("ES_HOST is not configured")
            return None
            
        try:
            self.logger.debug(f"Connecting to ES host: {self.config.es_host}")
            es_client = Elasticsearch(
                [self.config.es_host],
                timeout=self.config.es_connection_timeout,
                retry_on_timeout=True,
                max_retries=self.config.es_retry_count
            )
            
            if not es_client.ping():
                self.logger.error("Failed to ping Elasticsearch")
                raise Exception("Elasticsearch connection test failed")
                
            self.logger.info(f"Successfully connected to Elasticsearch: {es_client.info()}")
            return es_client
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            return None

    def bulk_index(self, documents: List[Dict[str, Any]], index_name: str) -> int:
        if not self.es_client:
            self.logger.error("No ES client available")
            return 0
                
        try:
            # 인덱스가 없을 때만 생성
            if not self.es_client.indices.exists(index=index_name):
                self.logger.info(f"Creating new index: {index_name}")
                mappings = {
                    "mappings": {
                        "properties": {
                            "query": {"type": "text"},
                            "normalized_query": {"type": "text"},
                            "query_hash": {"type": "keyword"},
                            "timestamp": {
                                "type": "date",
                                "format": "strict_date_optional_time||epoch_millis"
                            },
                            "query_time": {"type": "float"},
                            "lock_time": {"type": "float"},
                            "rows_examined": {"type": "long"},
                            "rows_sent": {"type": "long"},
                            "execution_count": {"type": "long"},
                            "timestamps": {
                                "type": "date",
                                "format": "strict_date_optional_time||epoch_millis"
                            },
                            "last_seen": {
                                "type": "date",
                                "format": "strict_date_optional_time||epoch_millis"
                            },
                            "max_query_time": {"type": "float"},
                            "instance_id": {"type": "keyword"}
                        }
                    },
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                }
                self.es_client.indices.create(index=index_name, body=mappings)
            
            # 벌크 인덱싱 전 로그
            self.logger.info(f"Starting bulk index of {len(documents)} documents to {index_name}")
            self.logger.debug(f"Sample document: {json.dumps(documents[0], indent=2) if documents else 'No documents'}")
            
            # 벌크 인덱싱 수행
            success, failed = bulk(
                self.es_client,
                documents,
                stats_only=True,  # 성능을 위해 stats_only=True로 변경
                refresh=True,     # 즉시 검색 가능하도록
                request_timeout=30,
                raise_on_error=False  # 일부 실패해도 계속 진행
            )
            
            # 결과 로깅
            self.logger.info(f"Bulk indexing completed - Success: {success}, Failed: {failed}")
            
            if failed > 0:
                self.logger.warning(f"Some documents failed to index: {failed} failures")
                
            return success
                
        except Exception as e:
            self.logger.error(f"Bulk indexing failed with error: {str(e)}")
            self.logger.exception("Detailed error trace:")
            raise
            
def monitor_memory(func):
    """메모리 모니터링 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.info({
                "function": func.__name__,
                "execution_time_sec": f"{execution_time:.2f}",
                "timestamp": datetime.now().isoformat()
            })
            
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
            
    return wrapper

class SlowQueryLogProcessor:
    """슬로우 쿼리 로그 처리기"""
    def __init__(self, config: 'Config', normalizer: 'QueryNormalizer' = None):
        self.config = config
        self.query_normalizer = normalizer or QueryNormalizer()
        self.es_manager = ElasticsearchManager(config)
        self.stream_processor = LogStreamProcessor(config, self.query_normalizer)
        self._batch: List[Dict[str, Any]] = []
        self._stats = {
            'processed': 0,
            'indexed': 0,
            'errors': 0
        }
        # 로깅 설정
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

    @monitor_memory
    def process_logs(self) -> Dict[str, int]:
        """로그 처리 실행"""
        try:
            log_stream = self.stream_processor.stream_log_files()
            
            for query_data in self.stream_processor.process_streaming_content(log_stream):
                self._stats['processed'] += 1
                
                try:
                    es_doc = self._prepare_es_document(query_data)
                    self._batch.append(es_doc)
                    
                    if len(self._batch) >= self.config.batch_size:
                        self._process_batch()
                except Exception as e:
                    logger.error(f"문서 준비 중 에러 발생: {str(e)}")
                    self._stats['errors'] += 1
            
            if self._batch:
                self._process_batch()
            
            return self._stats
            
        except Exception as e:
            logger.error(f"로그 처리 중 에러 발생: {str(e)}")
            raise
        finally:
            self._cleanup()

    def _prepare_es_document(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """ES 문서 준비"""
        try:
            normalized_query = self.query_normalizer.normalize_query(query_data['query'])
            query_hash = self.query_normalizer.generate_hash(normalized_query)
            
            current_timestamp = query_data['timestamp']
            self.logger.debug(f"Processing query with timestamp: {current_timestamp}")
            
            # 인스턴스 ID를 포함한 동적 인덱스 이름 생성
            index_name = f"{self.config.es_index_prefix}-{self.config.instance_id}"
        
            document = {
                '_op_type': 'update',
                '_index': index_name,
                '_id': query_hash,
                'script': {
                    'source': '''
                        if (ctx._source.timestamps == null) {
                            ctx._source.timestamps = new ArrayList();
                        }
                        if (!ctx._source.timestamps.contains(params.timestamp)) {
                            ctx._source.timestamps.add(params.timestamp);
                        }
                        ctx._source.last_seen = params.timestamp;
                        ctx._source.execution_count = (ctx._source.containsKey('execution_count') ? ctx._source.execution_count : 0) + 1;
                        ctx._source.max_query_time = Math.max(ctx._source.containsKey('max_query_time') ? ctx._source.max_query_time : 0, params.query_time);
                    ''',
                    'lang': 'painless',
                    'params': {
                        'timestamp': current_timestamp,
                        'query_time': float(query_data['query_time'])
                    }
                },
                'upsert': {
                    'query': query_data['query'],
                    'normalized_query': normalized_query,
                    'query_hash': query_hash,
                    'timestamp': current_timestamp,
                    'user': query_data['user'],
                    'query_time': float(query_data['query_time']),
                    'lock_time': float(query_data['lock_time']),
                    'rows_examined': int(query_data['rows_examined']),
                    'rows_sent': int(query_data.get('rows_sent', 0)),
                    'execution_count': 1,
                    'timestamps': [current_timestamp],
                    'max_query_time': float(query_data['query_time']),
                    'last_seen': current_timestamp,
                    'instance_id': self.config.instance_id  # 인스턴스 ID도 문서에 포함
                },
                'retry_on_conflict': 3  # 충돌 발생 시 재시도
            }
            
            self.logger.debug(f"Prepared document: {json.dumps(document, indent=2)}")
            return document
            
        except Exception as e:
            self.logger.error(f"Error preparing ES document: {str(e)}")
            self.logger.exception("Detailed error trace:")
            raise


    def _process_batch(self) -> None:
        """배치 처리"""
        if not self._batch:
            return
            
        try:
            # 동적 인덱스 이름 생성
            index_name = f"{self.config.es_index_prefix}-{self.config.instance_id}"
            indexed = self.es_manager.bulk_index(self._batch, index_name)
            self._stats['indexed'] += indexed
            
        except Exception as e:
            logger.error(f"배치 처리 중 에러 발생: {str(e)}")
            self._stats['errors'] += len(self._batch)
            raise
        finally:
            self._batch = []

    def _cleanup(self) -> None:
        """리소스 정리"""
        self._batch = []
        logger.info(f"처리 통계: {json.dumps(self._stats)}")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 핸들러"""
    start_time = time.time()
    
    try:
        config = Config.from_event(event)
        config.validate()
        
        processor = SlowQueryLogProcessor(config)
        results = processor.process_logs()
        
        execution_time = time.time() - start_time
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'stats': results,
                'execution_time_seconds': f"{execution_time:.2f}",
                'message': 'Successfully processed slow query logs'
            })
        }
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': str(ve),
                'message': 'Invalid configuration'
            })
        }
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error processing slow query logs'
            })
        }            
            
