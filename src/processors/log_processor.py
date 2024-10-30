"""RDS 슬로우 쿼리 로그 스트리밍 및 처리를 담당하는 모듈

이 모듈은 AWS RDS의 슬로우 쿼리 로그를 스트리밍 방식으로 읽고 파싱하는 기능을 제공합니다.
대용량 로그 파일을 효율적으로 처리하기 위한 버퍼링 및 청킹 전략을 구현합니다.
"""

import re
import boto3
import logging
from typing import Dict, List, Generator, Any, Optional, Pattern
from dataclasses import dataclass, field

from src.config.config import Config
from src.processors.query_normalizer import QueryNormalizer

logger = logging.getLogger(__name__)

@dataclass
class LogParsingResult:
    """로그 파싱 결과를 담는 데이터 클래스
    
    Attributes:
        timestamp (str): 쿼리 실행 시간
        user (Dict[str, str]): 사용자 정보 (이름, 호스트)
        query_time (float): 쿼리 실행 시간
        lock_time (float): 락 대기 시간
        rows_examined (int): 검사한 행 수
        rows_sent (int): 반환한 행 수
        query (str): SQL 쿼리
    """
    timestamp: Optional[str] = None
    user: Dict[str, Optional[str]] = field(default_factory=lambda: {"name": None, "host": None})
    query_time: float = 0.0
    lock_time: float = 0.0
    rows_examined: int = 0
    rows_sent: int = 0
    query: str = ""

@dataclass
class LogStreamProcessor:
    """로그 스트림 처리기
    
    AWS RDS에서 슬로우 쿼리 로그를 스트리밍 방식으로 읽고 파싱합니다.
    
    Attributes:
        config (Config): 애플리케이션 설정
        query_normalizer (QueryNormalizer): SQL 쿼리 정규화기
        _patterns (Dict[str, Pattern]): 정규표현식 패턴들
        _buffer (List[str]): 로그 라인 버퍼
        _buffer_size (int): 현재 버퍼 크기
        _stats (Dict[str, Any]): 처리 통계
    """
    
    config: Config
    query_normalizer: QueryNormalizer
    _patterns: Dict[str, Pattern] = field(default_factory=dict)
    _buffer: List[str] = field(default_factory=list)
    _buffer_size: int = 0
    _stats: Dict[str, Any] = field(default_factory=lambda: {
        'processed_files': 0,
        'processed_queries': 0,
        'skipped_queries': 0,
        'parsing_errors': 0,
        'total_bytes_processed': 0
    })

    def __post_init__(self):
        """초기화 메서드: 정규표현식 패턴을 컴파일합니다."""
        self._patterns = self._compile_regex_patterns()
        self._max_buffer_size = self.config.max_buffer_size_mb * 1024 * 1024

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

    def stream_log_files(self) -> Generator[str, None, None]:
        """RDS 로그 파일을 스트리밍 방식으로 읽습니다.
        
        Yields:
            str: 로그 파일의 청크 데이터
            
        Raises:
            Exception: AWS API 호출 실패 시
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
                self._stats['processed_files'] += 1
                
        except Exception as e:
            logger.error(f"로그 스트리밍 중 오류 발생: {str(e)}")
            raise

    def _process_log_file(self, rds_client: Any, log_file: Dict[str, Any]) -> Generator[str, None, None]:
        """개별 로그 파일을 처리합니다.
        
        Args:
            rds_client: boto3 RDS 클라이언트
            log_file: 로그 파일 정보
            
        Yields:
            str: 로그 파일의 청크 데이터
        """
        marker = '0'
        partial_content = ""
        
        while True:
            try:
                log_portion = rds_client.download_db_log_file_portion(
                    DBInstanceIdentifier=self.config.instance_id,
                    LogFileName=log_file['LogFileName'],
                    Marker=marker,
                    NumberOfLines=self.config.chunk_size
                )
                
                if 'LogFileData' in log_portion:
                    content = partial_content + log_portion['LogFileData']
                    content_size = len(content.encode('utf-8'))
                    self._stats['total_bytes_processed'] += content_size
                    
                    # 완전한 쿼리 블록의 끝 찾기
                    last_complete_block = content.rfind('\n# Time:')
                    
                    if last_complete_block != -1:
                        yield content[:last_complete_block]
                        partial_content = content[last_complete_block:]
                    else:
                        partial_content = content
                
                if not log_portion.get('AdditionalDataPending', False):
                    if partial_content:
                        yield partial_content
                    break
                    
                marker = log_portion['Marker']
                
            except Exception as e:
                logger.error(f"로그 파일 부분 다운로드 중 오류: {str(e)}")
                raise

    def process_streaming_content(self, content_stream: Generator[str, None, None]) -> Generator[Dict[str, Any], None, None]:
        """스트리밍 컨텐츠를 처리합니다.
        
        Args:
            content_stream: 로그 컨텐츠 스트림
            
        Yields:
            Dict[str, Any]: 파싱된 쿼리 정보
        """
        current_block: List[str] = []
        buffer_size = 0
        current_metadata = LogParsingResult()
        
        try:
            for chunk in content_stream:
                for line in chunk.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                        
                    line_size = len(line.encode('utf-8'))
                    
                    # 새로운 쿼리 블록 시작
                    if time_match := self._patterns['time_line'].match(line):
                        if current_block:
                            yield from self._handle_query_block(current_block, current_metadata)
                        current_block = []
                        buffer_size = 0
                        current_metadata = LogParsingResult(timestamp=time_match.group(1))
                    
                    # 버퍼 크기 체크
                    if buffer_size + line_size > self._max_buffer_size:
                        if current_block:
                            yield from self._handle_query_block(current_block, current_metadata)
                        current_block = []
                        buffer_size = 0
                        continue
                    
                    # 라인 처리
                    if not self._patterns['start_lines'].search(line):
                        current_block.append(line)
                        buffer_size += line_size
                        self._update_metadata(line, current_metadata)
            
            # 마지막 블록 처리
            if current_block:
                yield from self._handle_query_block(current_block, current_metadata)
                
        except Exception as e:
            logger.error(f"스트리밍 컨텐츠 처리 중 오류: {str(e)}")
            raise

    def _update_metadata(self, line: str, metadata: LogParsingResult) -> None:
        """메타데이터를 업데이트합니다.
        
        Args:
            line: 로그 라인
            metadata: 현재 메타데이터
        """
        if user_match := self._patterns['user_host'].search(line):
            metadata.user['name'] = user_match.group(1)
            metadata.user['host'] = user_match.group(2)
        elif query_match := self._patterns['query_stats'].search(line):
            metadata.query_time = float(query_match.group(1))
            metadata.lock_time = float(query_match.group(2))
        elif rows_match := self._patterns['rows_stats'].search(line):
            metadata.rows_examined = int(rows_match.group(1))
            metadata.rows_sent = int(rows_match.group(2))

    def _handle_query_block(self, block: List[str], metadata: LogParsingResult) -> Generator[Dict[str, Any], None, None]:
        """쿼리 블록을 처리합니다.
        
        Args:
            block: 로그 라인들
            metadata: 쿼리 메타데이터
            
        Yields:
            Dict[str, Any]: 파싱된 쿼리 정보
        """
        try:
            # 쿼리 추출
            query_lines = [line for line in block if not line.startswith('#')]
            if not query_lines:
                return

            metadata.query = ' '.join(query_lines)
            
            # 쿼리 검증
            if self._is_valid_query(metadata):
                self._stats['processed_queries'] += 1
                yield self._create_query_document(metadata)
            else:
                self._stats['skipped_queries'] += 1
                
        except Exception as e:
            logger.error(f"쿼리 블록 처리 중 오류: {str(e)}")
            self._stats['parsing_errors'] += 1

    def _is_valid_query(self, metadata: LogParsingResult) -> bool:
        """쿼리의 유효성을 검사합니다.
        
        Args:
            metadata: 쿼리 메타데이터
            
        Returns:
            bool: 유효한 쿼리인지 여부
        """
        return (
            bool(metadata.query) and
            'SELECT' in metadata.query.upper() and
            metadata.query_time >= self.config.min_query_time
        )

    def _create_query_document(self, metadata: LogParsingResult) -> Dict[str, Any]:
        """쿼리 문서를 생성합니다.
        
        Args:
            metadata: 쿼리 메타데이터
            
        Returns:
            Dict[str, Any]: ES에 저장할 문서
        """
        return {
            'timestamp': metadata.timestamp,
            'user': metadata.user,
            'query_time': metadata.query_time,
            'lock_time': metadata.lock_time,
            'rows_examined': metadata.rows_examined,
            'rows_sent': metadata.rows_sent,
            'query': metadata.query
        }

    def get_stats(self) -> Dict[str, Any]:
        """처리 통계를 반환합니다.
        
        Returns:
            Dict[str, Any]: 처리 통계 정보
        """
        return self._stats

    def reset_stats(self) -> None:
        """통계를 초기화합니다."""
        self._stats = {
            'processed_files': 0,
            'processed_queries': 0,
            'skipped_queries': 0,
            'parsing_errors': 0,
            'total_bytes_processed': 0
        }