"""RDS 슬로우 쿼리 로그 스트리밍 및 처리를 담당하는 모듈

이 모듈은 AWS RDS의 슬로우 쿼리 로그를 청크 단위로 효율적으로 처리하며,
메모리 사용량을 제어하고 처리 진행상황을 추적합니다.
"""

import re
import boto3
import logging
import psutil
import time
from typing import Dict, List, Generator, Any, Optional, Pattern, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from src.config.config import Config
from src.processors.query_normalizer import QueryNormalizer

logger = logging.getLogger(__name__)

@dataclass
class LogParsingResult:
    """로그 파싱 결과를 담는 데이터 클래스"""
    timestamp: Optional[str] = None
    user: Dict[str, Optional[str]] = field(default_factory=lambda: {"name": None, "host": None})
    query_time: float = 0.0
    lock_time: float = 0.0
    rows_examined: int = 0
    rows_sent: int = 0
    query: str = ""

@dataclass
class ProcessingMetrics:
    """처리 메트릭을 관리하는 데이터 클래스
    
    Attributes:
        processed_bytes: 처리된 총 바이트
        current_memory_usage: 현재 메모리 사용량
        peak_memory_usage: 최대 메모리 사용량
        processed_chunks: 처리된 청크 수
        failed_chunks: 실패한 청크 수
        processing_speed: 초당 처리된 바이트
    """
    processed_bytes: int = 0
    current_memory_usage: float = 0.0
    peak_memory_usage: float = 0.0
    processed_chunks: int = 0
    failed_chunks: int = 0
    processing_speed: float = 0.0
    start_time: float = field(default_factory=time.time)

    def update_memory_stats(self) -> None:
        """메모리 사용량 통계를 업데이트합니다."""
        process = psutil.Process()
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        self.current_memory_usage = current_memory
        self.peak_memory_usage = max(self.peak_memory_usage, current_memory)

    def update_processing_speed(self) -> None:
        """처리 속도를 업데이트합니다."""
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0:
            self.processing_speed = self.processed_bytes / elapsed_time

    def get_progress_status(self) -> Dict[str, Any]:
        """현재 처리 진행상황을 반환합니다."""
        return {
            'processed_bytes': self.processed_bytes,
            'current_memory_mb': round(self.current_memory_usage, 2),
            'peak_memory_mb': round(self.peak_memory_usage, 2),
            'processed_chunks': self.processed_chunks,
            'failed_chunks': self.failed_chunks,
            'processing_speed_bps': round(self.processing_speed, 2),
            'elapsed_time': round(time.time() - self.start_time, 2)
        }

@dataclass
class ChunkInfo:
    """청크 정보를 관리하는 데이터 클래스"""
    chunk_id: str
    start_position: int
    size: bytes
    status: str = 'pending'
    retry_count: int = 0
    error: Optional[str] = None

@dataclass
class LogStreamProcessor:
    """로그 스트림 처리기
    
    AWS RDS에서 슬로우 쿼리 로그를 청크 단위로 효율적으로 처리합니다.
    
    Attributes:
        config: 애플리케이션 설정
        query_normalizer: SQL 쿼리 정규화기
        metrics: 처리 메트릭
        _patterns: 정규표현식 패턴들
        _buffer: 로그 라인 버퍼
        _chunk_map: 청크 처리 상태 맵
    """
    
    config: Config
    query_normalizer: QueryNormalizer
    metrics: ProcessingMetrics = field(default_factory=ProcessingMetrics)
    _patterns: Dict[str, Pattern] = field(default_factory=dict)
    _buffer: List[str] = field(default_factory=list)
    _chunk_map: Dict[str, ChunkInfo] = field(default_factory=dict)

    def __post_init__(self):
        """초기화 메서드"""
        self._patterns = self._compile_regex_patterns()
        self._max_buffer_size = self.config.max_buffer_size_mb * 1024 * 1024
        self._memory_threshold = 0.8  # 80% 메모리 사용량 임계치

    @staticmethod
    def _compile_regex_patterns() -> Dict[str, Pattern]:
        """정규표현식 패턴을 컴파일합니다."""
        return {
            'start_lines': re.compile(r"(Version: 8\.0\.28|started with:|Tcp port:|Time\s+Id Command\s+Argument)"),
            'time_line': re.compile(r"^# Time:\s*(.+)"),
            'user_host': re.compile(r"# User@Host:\s*(\w+)\[\w+\]\s*@\s*([\w\.-]+)"),
            'query_stats': re.compile(r"Query_time:\s*(\d+\.?\d*)\s+Lock_time:\s*(\d+\.?\d*)"),
            'rows_stats': re.compile(r"Rows_examined:\s*(\d+)\s+Rows_sent:\s*(\d+)")
        }

    def stream_log_files(self) -> Generator[Tuple[str, str], None, None]:
        """RDS 로그 파일을 스트리밍 방식으로 읽습니다.
        
        Yields:
            Tuple[str, str]: (청크 ID, 청크 데이터)
        """
        try:
            rds_client = boto3.client('rds', region_name=self.config.region)
            
            response = rds_client.describe_db_log_files(
                DBInstanceIdentifier=self.config.instance_id,
                FilenameContains='slowquery'
            )
            
            for log_file in response['DescribeDBLogFiles']:
                logger.info(f"Processing log file: {log_file['LogFileName']}")
                yield from self._process_log_file(rds_client, log_file)
                
        except Exception as e:
            logger.error(f"로그 스트리밍 중 오류 발생: {str(e)}")
            raise

    def _process_log_file(
        self, 
        rds_client: Any, 
        log_file: Dict[str, Any]
    ) -> Generator[Tuple[str, str], None, None]:
        """개별 로그 파일을 처리합니다."""
        marker = '0'
        partial_content = ""
        file_name = log_file['LogFileName']
        
        logger.info(f"로그 파일 처리 시작: {file_name}")
        
        try:
            # 첫 번째 부분 다운로드
            log_portion = rds_client.download_db_log_file_portion(
                DBInstanceIdentifier=self.config.instance_id,
                LogFileName=file_name,
                Marker=marker,
                NumberOfLines=self.config.chunk_size
            )
            
            # 파일 내용 확인을 위한 로깅 추가
            if 'LogFileData' in log_portion:
                content = log_portion['LogFileData']
                logger.info(f"파일 '{file_name}' 내용 샘플 (크기: {len(content)} bytes):")
                lines = content.split('\n')
                for line in lines[:5]:  # 처음 5줄만 출력
                    logger.info(f"  {line}")
                logger.info(f"총 라인 수: {len(lines)}")
                
                # Time 패턴이 있는지 확인
                time_lines = [line for line in lines if line.startswith('# Time:')]
                logger.info(f"발견된 Time 패턴 수: {len(time_lines)}")
                if time_lines:
                    logger.info(f"첫 번째 Time 라인 샘플: {time_lines[0]}")
                    
                while True:
                    content = partial_content + log_portion['LogFileData']
                    
                    # 청크 생성
                    chunk_id = f"{file_name}_{marker}"
                    chunk_size = len(content.encode('utf-8'))
                    
                    self._chunk_map[chunk_id] = ChunkInfo(
                        chunk_id=chunk_id,
                        start_position=marker,  # marker를 문자열로 유지
                        size=chunk_size,
                        status='processing'
                    )
                    
                    # 완전한 쿼리 블록 찾기
                    last_complete_block = content.rfind('\n# Time:')
                    
                    if last_complete_block != -1:
                        chunk_content = content[:last_complete_block]
                        partial_content = content[last_complete_block:]
                        
                        # 메트릭 업데이트
                        self.metrics.processed_bytes += len(chunk_content.encode('utf-8'))
                        self.metrics.processed_chunks += 1
                        self.metrics.update_processing_speed()
                        
                        logger.debug(
                            f"청크 준비 완료 - ID: {chunk_id}, "
                            f"크기: {len(chunk_content)} bytes, "
                            f"쿼리 블록 수: {chunk_content.count('# Time:')}"
                        )
                        
                        # 청크 상태 업데이트
                        self._chunk_map[chunk_id].status = 'completed'
                        
                        yield chunk_id, chunk_content
                    else:
                        partial_content = content
                        logger.debug(f"완전한 쿼리 블록을 찾지 못함 - 부분 컨텐츠 크기: {len(partial_content)}")
                    
                    if not log_portion.get('AdditionalDataPending', False):
                        if partial_content:
                            final_chunk_id = f"{file_name}_final"
                            logger.debug(f"마지막 청크 처리 - 크기: {len(partial_content)} bytes")
                            yield final_chunk_id, partial_content
                        break
                        
                    marker = log_portion['Marker']
                    log_portion = rds_client.download_db_log_file_portion(
                        DBInstanceIdentifier=self.config.instance_id,
                        LogFileName=file_name,
                        Marker=marker,
                        NumberOfLines=self.config.chunk_size
                    )
            else:
                logger.warning(f"파일 '{file_name}'에 데이터가 없습니다")
                
        except Exception as e:
            logger.error(f"파일 '{file_name}' 처리 중 오류 발생: {str(e)}")
            if 'chunk_id' in locals():
                if chunk_id in self._chunk_map:
                    self._chunk_map[chunk_id].status = 'failed'
                    self._chunk_map[chunk_id].error = str(e)
            raise

    def process_streaming_content(self, content_stream: Generator[Tuple[str, str], None, None]) -> Generator[Dict[str, Any], None, None]:
        """스트리밍 컨텐츠를 처리합니다."""
        try:
            for chunk_id, chunk_content in content_stream:
                # 각 청크를 process_chunk를 통해 처리
                try:
                    yield from self.process_chunk(chunk_id, chunk_content)
                except Exception as e:
                    logger.error(f"청크 {chunk_id} 처리 중 오류 발생: {str(e)}")
                    if chunk_id in self._chunk_map:
                        self._chunk_map[chunk_id].status = 'failed'
                        self._chunk_map[chunk_id].error = str(e)
                    continue

        except Exception as e:
            logger.error(f"스트리밍 컨텐츠 처리 중 오류 발생: {str(e)}")
            raise

    def _should_throttle(self) -> bool:
        """처리 속도 조절이 필요한지 확인합니다."""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            total_memory = psutil.virtual_memory().total
            
            # 실제 사용 메모리 비율 계산
            memory_usage_percent = (memory_info.rss / total_memory) * 100
            
            # 임계치를 총 할당된 메모리의 80%로 설정
            threshold_mb = (self.config.memory_threshold_mb 
                        if hasattr(self.config, 'memory_threshold_mb') 
                        else 1024)  # 기본값 1GB
            current_usage_mb = memory_info.rss / (1024 * 1024)
            
            if current_usage_mb > threshold_mb:
                logger.warning(
                    f"높은 메모리 사용량: {current_usage_mb:.1f}MB "
                    f"(임계치: {threshold_mb}MB)"
                )
                return True
                
            # 메모리 사용량이 낮은 경우 처리 속도 조절 불필요
            if memory_usage_percent < 50:  # 50% 미만이면 조절 안 함
                return False
                
            return memory_usage_percent > 80  # 80% 초과시에만 조절
            
        except ImportError:
            logger.warning("psutil을 사용할 수 없어 메모리 모니터링이 비활성화됩니다")
            return False

    def process_chunk(self, chunk_id: str, content: str) -> Generator[Dict[str, Any], None, None]:
        """청크를 처리합니다.
        
        Args:
            chunk_id: 청크 식별자
            content: 청크 내용
            
        Yields:
            Dict[str, Any]: 처리된 쿼리 정보
        """
        if chunk_id not in self._chunk_map:
            chunk_size = len(content.encode('utf-8'))
            self._chunk_map[chunk_id] = ChunkInfo(
                chunk_id=chunk_id,
                start_position='0',  # 문자열로 처리
                size=chunk_size,
                status='processing'
            )

        chunk_info = self._chunk_map[chunk_id]
        chunk_info.status = 'processing'
        logger.debug(f"청크 처리 시작: {chunk_id}, 크기: {len(content)} bytes")

        try:
            current_block: List[str] = []
            current_metadata = LogParsingResult()
            parsed_queries = 0
            
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    continue
                    
                # 새로운 쿼리 블록 시작
                if line.startswith('# Time:'):
                    # 이전 블록 처리
                    if current_block:
                        results = list(self._handle_query_block(current_block, current_metadata))
                        if results:  # 결과가 있으면 파싱 성공
                            parsed_queries += 1
                            yield from results
                            
                    current_block = []
                    current_metadata = LogParsingResult(timestamp=line[7:].strip())
                
                current_block.append(line)
            
            # 마지막 블록 처리
            if current_block:
                results = list(self._handle_query_block(current_block, current_metadata))
                if results:
                    parsed_queries += 1
                    yield from results
            
            chunk_info.status = 'completed'
            logger.info(f"청크 처리 완료: {chunk_id}, 파싱된 쿼리 수: {parsed_queries}")
            
        except Exception as e:
            logger.error(f"청크 {chunk_id} 처리 실패: {str(e)}")
            chunk_info.status = 'failed'
            chunk_info.error = str(e)
            chunk_info.retry_count += 1
            raise
    
    def _handle_query_block(
        self, 
        block: List[str], 
        metadata: LogParsingResult
    ) -> Generator[Dict[str, Any], None, None]:
        """쿼리 블록을 처리하고 파싱된 결과를 생성합니다."""
        try:
            # 1. 블록 파싱
            if not self._parse_block_content(block, metadata):
                logger.debug("블록 파싱 실패")
                return
                
            # 2. 검증 및 처리
            if self._validate_query(metadata):
                # LogParsingResult를 딕셔너리로 변환
                query_doc = {
                    'timestamp': metadata.timestamp,
                    'user': metadata.user,
                    'query_time': metadata.query_time,
                    'lock_time': metadata.lock_time,
                    'rows_examined': metadata.rows_examined,
                    'rows_sent': metadata.rows_sent,
                    'query': metadata.query
                }
                
                self._update_processing_metrics()
                yield query_doc
                logger.debug("쿼리 문서 생성 성공")
                
        except Exception as e:
            logger.error(f"쿼리 블록 처리 중 오류 발생: {str(e)}")
            logger.debug(f"문제의 블록: {block[:5]}...")
            raise

    def _parse_block_content(
        self, 
        block: List[str], 
        metadata: LogParsingResult
    ) -> bool:
        """블록의 내용을 파싱하여 메타데이터를 추출합니다."""
        query_lines = []
        logger.debug(f"쿼리 블록 파싱 시작 - {len(block)} 라인")
        
        # 원본 블록 내용 로깅
        logger.debug("블록 내용:")
        for line in block[:10]:  # 처음 10줄 출력
            logger.debug(f"  {line}")
        
        for line in block:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('#'):
                # 사용자 정보 파싱 개선
                if 'User@Host:' in line:
                    logger.debug(f"사용자 라인 발견: {line}")
                    # 다양한 형식 지원
                    patterns = [
                        r"# User@Host: (\w+)\s*(?:\[[\w\-]+\])?\s*@\s*([\w\.\-]+)",  # 기본 형식
                        r"# User@Host: (\w+)\s*@\s*([\w\.\-]+)",  # 단순 형식
                        r"# User@Host: (\w+)(?:\[[^\]]+\])*\s*@\s*([^\s]+)",  # 확장 형식
                    ]
                    
                    for pattern in patterns:
                        if user_match := re.search(pattern, line):
                            metadata.user['name'] = user_match.group(1)
                            metadata.user['host'] = user_match.group(2)
                            logger.debug(f"파싱된 사용자 정보: {metadata.user}")
                            break
                    
                    if not metadata.user['name']:
                        logger.warning(f"사용자 정보 파싱 실패: {line}")
                        
                elif time_match := self._patterns['time_line'].match(line):
                    metadata.timestamp = time_match.group(1).strip()
                    logger.debug(f"타임스탬프 발견: {metadata.timestamp}")
                    
                elif query_match := self._patterns['query_stats'].search(line):
                    metadata.query_time = float(query_match.group(1))
                    metadata.lock_time = float(query_match.group(2))
                    logger.debug(f"쿼리 통계 발견: time={metadata.query_time}, lock={metadata.lock_time}")
                    
                elif rows_match := self._patterns['rows_stats'].search(line):
                    metadata.rows_examined = int(rows_match.group(1))
                    metadata.rows_sent = int(rows_match.group(2))
                    logger.debug(f"행 통계 발견: examined={metadata.rows_examined}, sent={metadata.rows_sent}")
            else:
                # SET timestamp 명령 무시
                if not line.startswith('SET timestamp='):
                    query_lines.append(line)
        
        if query_lines:
            metadata.query = ' '.join(query_lines)
            logger.debug(f"쿼리 파싱 완료: {metadata.query[:100]}...")
            return self._validate_query(metadata)
        
        logger.debug("쿼리 라인 없음")
        return False

    def _validate_query(self, metadata: LogParsingResult) -> bool:
        """파싱된 쿼리를 검증합니다."""
        # 설정 및 현재 쿼리 정보 로깅
        logger.info(f"쿼리 검증 시작:")
        logger.info(f"  설정된 min_query_time: {self.config.min_query_time}")
        logger.info(f"  현재 쿼리의 query_time: {metadata.query_time}")
        logger.info(f"  타임스탬프: {metadata.timestamp}")
        logger.info(f"  사용자 정보: {metadata.user}")
        
        # 쿼리 내용 샘플 로깅 (너무 길면 자름)
        query_sample = metadata.query[:200] + "..." if len(metadata.query) > 200 else metadata.query
        logger.info(f"  쿼리 샘플: {query_sample}")
        
        validations = [
            (bool(metadata.query), "쿼리가 비어있음"),
            (bool(metadata.timestamp), "타임스탬프 없음"),
            (metadata.query_time >= self.config.min_query_time, 
            f"최소 쿼리 시간({self.config.min_query_time}초) 미달: {metadata.query_time}초"),
            (bool(metadata.user.get('name')), "사용자 정보 없음"),
            ('SELECT' in metadata.query.upper(), "SELECT 문이 없음")
        ]

        # 각 검증 단계 상세 로깅
        for i, (condition, message) in enumerate(validations, 1):
            if not condition:
                logger.info(f"검증 단계 {i} 실패: {message}")
                return False
            logger.debug(f"검증 단계 {i} 통과")

        logger.info("모든 검증 단계 통과")
        return True

    def _create_query_document(self, metadata: LogParsingResult) -> Optional[Dict[str, Any]]:
        """Elasticsearch에 저장할 쿼리 문서를 생성합니다.
        
        Args:
            metadata: 쿼리 메타데이터
            
        Returns:
            Optional[Dict[str, Any]]: 생성된 쿼리 문서 또는 None
            
        Note:
            - 쿼리 정규화 및 해시 생성 포함
            - Elasticsearch 업데이트 스크립트 포함
            - 충돌 처리를 위한 재시도 설정 포함
        """
        try:
            # 쿼리 정규화 및 해시 생성
            normalized_query = self.query_normalizer.normalize_query(metadata.query)
            query_hash = self.query_normalizer.generate_hash(metadata.query)

            return {
                '_op_type': 'update',
                '_index': self.config.get_es_index_name(),
                '_id': query_hash,
                'script': {
                    'source': """
                        if (ctx._source.timestamps == null) {
                            ctx._source.timestamps = new ArrayList();
                        }
                        if (!ctx._source.timestamps.contains(params.timestamp)) {
                            ctx._source.timestamps.add(params.timestamp);
                        }
                        ctx._source.last_seen = params.timestamp;
                        ctx._source.execution_count = (ctx._source.containsKey('execution_count') ? 
                            ctx._source.execution_count : 0) + 1;
                        ctx._source.max_query_time = Math.max(
                            ctx._source.containsKey('max_query_time') ? 
                            ctx._source.max_query_time : 0, 
                            params.query_time
                        );
                    """,
                    'lang': 'painless',
                    'params': {
                        'timestamp': metadata.timestamp,
                        'query_time': float(metadata.query_time)
                    }
                },
                'upsert': {
                    'query': metadata.query,
                    'normalized_query': normalized_query,
                    'query_hash': query_hash,
                    'timestamp': metadata.timestamp,
                    'user': metadata.user,
                    'query_time': float(metadata.query_time),
                    'lock_time': float(metadata.lock_time),
                    'rows_examined': int(metadata.rows_examined),
                    'rows_sent': int(metadata.rows_sent),
                    'execution_count': 1,
                    'timestamps': [metadata.timestamp],
                    'max_query_time': float(metadata.query_time),
                    'last_seen': metadata.timestamp,
                    'instance_id': self.config.instance_id
                },
                'retry_on_conflict': 3
            }
        except Exception as e:
            logger.error(f"쿼리 문서 생성 중 오류: {str(e)}")
            return None

    def _update_processing_metrics(self) -> None:
        """처리 메트릭을 업데이트합니다."""
        self.metrics.update_memory_stats()
        self.metrics.processed_chunks += 1
        
        if self.metrics.processed_chunks % 1000 == 0:
            logger.info(
                f"처리 현황: {self.metrics.processed_chunks}개 청크, "
                f"메모리: {self.metrics.current_memory_usage:.1f}MB, "
                f"속도: {self.metrics.processing_speed:.1f} bytes/sec"
            )

    def get_processing_status(self) -> Dict[str, Any]:
        """현재 처리 상태를 반환합니다."""
        chunk_status = {
            'pending': 0,
            'processing': 0,
            'completed': 0,
            'failed': 0
        }
        
        for chunk in self._chunk_map.values():
            chunk_status[chunk.status] += 1
            
        return {
            'metrics': self.metrics.get_progress_status(),
            'chunk_status': chunk_status,
            'failed_chunks': [
                {
                    'chunk_id': k,
                    'retry_count': v.retry_count,
                    'error': v.error
                }
                for k, v in self._chunk_map.items()
                if v.status == 'failed'
            ]
        }

