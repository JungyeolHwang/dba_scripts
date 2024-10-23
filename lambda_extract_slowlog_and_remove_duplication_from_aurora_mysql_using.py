from dataclasses import dataclass
from typing import Optional, List, Dict, Generator, Any, Tuple
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

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dataclass
class Config:
    """어플리케이션 설정"""
    instance_id: str
    region: str = "ap-northeast-2"
    min_query_time: float = 1.0
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
                "query_time": {"type": "float"},
                "lock_time": {"type": "float"},
                "rows_examined": {"type": "integer"},
                "rows_sent": {"type": "integer"},
                "query": {"type": "text"}
            }
        }
    }
    
    try:
        # 기존 인덱스가 있다면 삭제
        if es_client.indices.exists(index=index_name):
            es_client.indices.delete(index=index_name)
            
        # 새 인덱스 생성
        es_client.indices.create(index=index_name, body=mapping)
        logger.info(f"Successfully created index {index_name} with mapping")
    except Exception as e:
        logger.error(f"Error setting up index mapping: {str(e)}")
        raise

class SlowQueryLogProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.es_client = get_elasticsearch_client(config) if config.es_host else None
        self._compile_regex_patterns()

    def _compile_regex_patterns(self) -> None:
        """정규 표현식 패턴 컴파일"""
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
        """쿼리 클리닝"""
        try:
            normalized = query.strip()
            normalized = re.sub(r'/\*.*?\*/', '', normalized)
            normalized = re.sub(r'[\r\n]+', ' ', normalized)
            return ' '.join(normalized.split())
        except Exception as e:
            logger.warning(f"쿼리 클리닝 중 오류 발생: {str(e)}")
            return query

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
        """배치 인덱싱"""
        try:
            index_name = f"{self.config.es_index_prefix}-eng-dbnode02"
            
            # 인덱스 매핑 설정
            setup_index_mapping(self.es_client, index_name)
            
            actions = [
                {
                    "_index": index_name,
                    "_source": query
                }
                for query in batch
            ]
            
            success, failed = bulk(self.es_client, actions, stats_only=True)
            logger.info(f"배치 인덱싱 완료 - 성공: {success}, 실패: {failed}")
            
            return success
            
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
