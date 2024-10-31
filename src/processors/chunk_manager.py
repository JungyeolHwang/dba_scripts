"""대용량 로그를 청크 단위로 관리하는 모듈

이 모듈은 대용량 로그 파일을 메모리 효율적으로 처리하기 위한
청크 단위 관리 기능을 제공합니다.
"""

import os
import time
import logging
from typing import Dict, List, Generator, Any, Optional
from dataclasses import dataclass, field
import boto3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class ChunkMetadata:
    """청크 처리 메타데이터
    
    Attributes:
        chunk_id: 청크 식별자
        start_position: 로그 파일 내 시작 위치
        end_position: 로그 파일 내 종료 위치
        size_bytes: 청크 크기
        status: 처리 상태
        attempt_count: 처리 시도 횟수
        last_error: 마지막 에러 메시지
        processing_time: 처리 소요 시간
    """
    chunk_id: str
    start_position: int
    end_position: int
    size_bytes: int
    status: str = "pending"  # pending, processing, completed, failed
    attempt_count: int = 0
    last_error: Optional[str] = None
    processing_time: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    last_processed_at: Optional[datetime] = None

@dataclass
class ChunkProcessingStats:
    """청크 처리 통계
    
    Attributes:
        total_chunks: 전체 청크 수
        processed_chunks: 처리된 청크 수
        failed_chunks: 실패한 청크 수
        total_bytes: 전체 처리된 바이트
        processing_time: 전체 처리 시간
    """
    total_chunks: int = 0
    processed_chunks: int = 0
    failed_chunks: int = 0
    total_bytes: int = 0
    processing_time: float = 0.0
    
    def get_progress(self) -> float:
        """처리 진행률을 반환"""
        if self.total_chunks == 0:
            return 0.0
        return (self.processed_chunks / self.total_chunks) * 100

class LogChunkManager:
    """로그 청크 관리자
    
    대용량 로그 파일을 청크 단위로 나누어 처리하는 기능을 제공합니다.
    메모리 사용량을 모니터링하고 제어하며, 청크 처리 상태를 추적합니다.
    
    Attributes:
        instance_id: RDS 인스턴스 ID
        region: AWS 리전
        max_chunk_size: 최대 청크 크기 (바이트)
        max_memory_usage: 최대 메모리 사용량 (MB)
        chunks: 청크 메타데이터 목록
        stats: 처리 통계
    """
    
    def __init__(
        self,
        instance_id: str,
        region: str,
        max_chunk_size: int = 5 * 1024 * 1024,  # 5MB
        max_memory_usage: int = 1024,  # 1GB
        retry_limit: int = 3
    ):
        self.instance_id = instance_id
        self.region = region
        self.max_chunk_size = max_chunk_size
        self.max_memory_usage = max_memory_usage
        self.retry_limit = retry_limit
        
        self.chunks: Dict[str, ChunkMetadata] = {}
        self.stats = ChunkProcessingStats()
        
        self._rds_client = boto3.client('rds', region_name=region)
        self._last_memory_check = time.time()
        self._memory_check_interval = 5  # 5초

    def prepare_chunks(self, log_file: Dict[str, Any]) -> None:
        """로그 파일을 청크 단위로 분할 준비
        
        Args:
            log_file: RDS 로그 파일 정보
        """
        logger.info(f"로그 파일 청크 분할 시작: {log_file['LogFileName']}")
        
        try:
            # 로그 파일 크기 확인
            file_size = log_file.get('Size', 0)
            if file_size == 0:
                logger.warning(f"빈 로그 파일: {log_file['LogFileName']}")
                return
                
            # 청크 수 계산
            chunk_count = (file_size + self.max_chunk_size - 1) // self.max_chunk_size
            
            # 청크 메타데이터 생성
            for i in range(chunk_count):
                start_pos = i * self.max_chunk_size
                end_pos = min(start_pos + self.max_chunk_size, file_size)
                
                chunk_id = f"{log_file['LogFileName']}_{i}"
                self.chunks[chunk_id] = ChunkMetadata(
                    chunk_id=chunk_id,
                    start_position=start_pos,
                    end_position=end_pos,
                    size_bytes=end_pos - start_pos
                )
            
            self.stats.total_chunks = len(self.chunks)
            self.stats.total_bytes = file_size
            
            logger.info(
                f"청크 분할 완료 - 파일: {log_file['LogFileName']}, "
                f"크기: {file_size:,} bytes, 청크 수: {chunk_count}"
            )
            
        except Exception as e:
            logger.error(f"청크 준비 중 오류 발생: {str(e)}")
            raise

    def get_next_chunk(self) -> Generator[tuple[str, bytes], None, None]:
        """처리할 다음 청크를 반환
        
        Yields:
            tuple[str, bytes]: (청크ID, 청크 데이터)
        """
        for chunk_id, metadata in self.chunks.items():
            # 이미 처리완료 또는 처리중인 청크 스킵
            if metadata.status in ("completed", "processing"):
                continue
                
            # 재시도 횟수 초과 청크 스킵
            if metadata.attempt_count >= self.retry_limit:
                continue
                
            # 메모리 사용량 체크
            if self._should_check_memory():
                if not self._check_memory_usage():
                    logger.warning("메모리 사용량 초과, 청크 처리 일시 중단")
                    break
            
            try:
                # 청크 데이터 로드
                chunk_data = self._load_chunk_data(metadata)
                
                # 메타데이터 업데이트
                metadata.status = "processing"
                metadata.attempt_count += 1
                metadata.last_processed_at = datetime.now()
                
                yield chunk_id, chunk_data
                
            except Exception as e:
                logger.error(f"청크 로드 중 오류: {str(e)}")
                metadata.status = "failed"
                metadata.last_error = str(e)
                continue

    def _load_chunk_data(self, metadata: ChunkMetadata) -> bytes:
        """청크 데이터를 로드
        
        Args:
            metadata: 청크 메타데이터
            
        Returns:
            bytes: 청크 데이터
        """
        try:
            response = self._rds_client.download_db_log_file_portion(
                DBInstanceIdentifier=self.instance_id,
                LogFileName=metadata.chunk_id.split('_')[0],
                Marker=str(metadata.start_position),
                NumberOfLines=None,  # 바이트 기반 다운로드
                Length=metadata.size_bytes
            )
            
            return response['LogFileData'].encode('utf-8')
            
        except Exception as e:
            logger.error(f"청크 데이터 로드 중 오류: {str(e)}")
            raise

    def update_chunk_status(
        self,
        chunk_id: str,
        status: str,
        error: Optional[str] = None,
        processing_time: Optional[float] = None
    ) -> None:
        """청크 처리 상태 업데이트
        
        Args:
            chunk_id: 청크 ID
            status: 새로운 상태
            error: 에러 메시지 (선택)
            processing_time: 처리 소요 시간 (선택)
        """
        if chunk_id not in self.chunks:
            logger.warning(f"알 수 없는 청크 ID: {chunk_id}")
            return
            
        metadata = self.chunks[chunk_id]
        metadata.status = status
        metadata.last_processed_at = datetime.now()
        
        if error:
            metadata.last_error = error
            
        if processing_time:
            metadata.processing_time = processing_time
            self.stats.processing_time += processing_time
        
        # 통계 업데이트
        if status == "completed":
            self.stats.processed_chunks += 1
        elif status == "failed":
            self.stats.failed_chunks += 1

    def _should_check_memory(self) -> bool:
        """메모리 체크 필요 여부 확인"""
        return time.time() - self._last_memory_check >= self._memory_check_interval

    def _check_memory_usage(self) -> bool:
        """현재 메모리 사용량 체크
        
        Returns:
            bool: 메모리 사용량이 허용 범위 내인지 여부
        """
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            self._last_memory_check = time.time()
            
            return memory_mb < self.max_memory_usage
            
        except ImportError:
            logger.warning("psutil 모듈을 찾을 수 없음, 메모리 체크 생략")
            return True
        except Exception as e:
            logger.error(f"메모리 사용량 확인 중 오류: {str(e)}")
            return True

    def get_processing_stats(self) -> Dict[str, Any]:
        """처리 통계 반환"""
        return {
            'total_chunks': self.stats.total_chunks,
            'processed_chunks': self.stats.processed_chunks,
            'failed_chunks': self.stats.failed_chunks,
            'total_bytes': self.stats.total_bytes,
            'processing_time': self.stats.processing_time,
            'progress_percentage': self.stats.get_progress(),
            'average_chunk_processing_time': (
                self.stats.processing_time / self.stats.processed_chunks
                if self.stats.processed_chunks > 0 else 0
            )
        }

    def get_failed_chunks(self) -> List[ChunkMetadata]:
        """실패한 청크 목록 반환"""
        return [
            metadata for metadata in self.chunks.values()
            if metadata.status == "failed" and metadata.attempt_count < self.retry_limit
        ]

    def cleanup(self) -> None:
        """리소스 정리"""
        self.chunks.clear()
        logger.info("청크 매니저 리소스 정리 완료")