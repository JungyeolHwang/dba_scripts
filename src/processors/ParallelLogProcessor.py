"""병렬 로그 처리를 담당하는 모듈

이 모듈은 대용량 로그 파일을 청크 단위로 병렬 처리하는 기능을 제공합니다.
메모리 사용량을 모니터링하면서 동적으로 처리 속도를 조절합니다.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Full
from typing import List, Dict, Any, Generator, Optional
from dataclasses import dataclass, field
from threading import Lock
import time

from src.processors.log_processor import LogStreamProcessor
from src.utils.decorators import monitor_memory
from src.config.config import Config

logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """처리 메트릭을 관리하는 데이터 클래스
    
    Attributes:
        processed_chunks: 처리된 청크 수
        failed_chunks: 실패한 청크 수
        processing_time: 처리 시간
        memory_usage: 메모리 사용량 기록
    """
    processed_chunks: int = 0
    failed_chunks: int = 0
    processing_time: float = 0.0
    memory_usage: List[float] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)

    def increment_processed(self) -> None:
        """처리된 청크 수를 증가시킵니다."""
        with self._lock:
            self.processed_chunks += 1

    def increment_failed(self) -> None:
        """실패한 청크 수를 증가시킵니다."""
        with self._lock:
            self.failed_chunks += 1

    def add_memory_usage(self, usage: float) -> None:
        """메모리 사용량을 기록합니다."""
        with self._lock:
            self.memory_usage.append(usage)

class ParallelLogProcessor:
    """청크 단위 병렬 처리를 담당하는 클래스
    
    이 클래스는 로그 청크들을 병렬로 처리하고, 메모리 사용량에 따라
    동적으로 처리 속도를 조절합니다.
    
    Attributes:
        config: 애플리케이션 설정
        max_workers: 최대 작업자 스레드 수
        queue_size: 결과 큐 크기
        memory_threshold: 메모리 사용량 임계치 (MB)
        stream_processor: 로그 스트림 처리기
        metrics: 처리 메트릭
    """

    def __init__(
        self,
        config: Config,
        max_workers: int = 4,
        queue_size: int = 1000,
        memory_threshold: int = 512
    ):
        """초기화 메서드
        
        Args:
            config: 애플리케이션 설정
            max_workers: 최대 작업자 스레드 수
            queue_size: 결과 큐 크기
            memory_threshold: 메모리 사용량 임계치 (MB)
        """
        self.config = config
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.memory_threshold = memory_threshold
        self.stream_processor = LogStreamProcessor(config, None)
        self.metrics = ProcessingMetrics()
        self._result_queue: Queue = Queue(maxsize=queue_size)
        self._active: bool = True

    @monitor_memory
    def process_chunks(self, chunks: Generator[str, None, None]) -> Generator[Dict[str, Any], None, None]:
        """로그 청크들을 병렬로 처리합니다.
        
        Args:
            chunks: 처리할 로그 청크 제너레이터
            
        Yields:
            Dict[str, Any]: 처리된 쿼리 정보
            
        Raises:
            Exception: 처리 중 오류 발생 시
        """
        start_time = time.time()
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_chunk = {}
                
                # 청크 처리 작업 제출
                for chunk in chunks:
                    if not self._active:
                        logger.warning("처리가 중단되었습니다")
                        break
                        
                    # 메모리 사용량 체크 및 조절
                    self._adjust_processing_rate()
                    
                    future = executor.submit(self._process_single_chunk, chunk)
                    future_to_chunk[future] = chunk
                
                # 결과 수집
                for future in as_completed(future_to_chunk):
                    try:
                        for result in future.result():
                            yield result
                        self.metrics.increment_processed()
                    except Exception as e:
                        logger.error(f"청크 처리 실패: {str(e)}")
                        self.metrics.increment_failed()
                        continue
                        
        except Exception as e:
            logger.error(f"병렬 처리 중 오류 발생: {str(e)}")
            raise
        finally:
            self.metrics.processing_time = time.time() - start_time
            self._log_processing_summary()

    def _process_single_chunk(self, chunk: str) -> List[Dict[str, Any]]:
        """단일 청크를 처리합니다.
        
        Args:
            chunk: 처리할 로그 청크
            
        Returns:
            List[Dict[str, Any]]: 처리된 쿼리 정보 리스트
        """
        try:
            return list(self.stream_processor.process_streaming_content([chunk]))
        except Exception as e:
            logger.error(f"청크 처리 중 오류: {str(e)}")
            raise

    def _adjust_processing_rate(self) -> None:
        """메모리 사용량에 따라 처리 속도를 조절합니다."""
        try:
            import psutil
            current_process = psutil.Process()
            memory_mb = current_process.memory_info().rss / 1024 / 1024
            self.metrics.add_memory_usage(memory_mb)
            
            if memory_mb > self.memory_threshold:
                logger.warning(f"메모리 사용량 임계치 초과: {memory_mb:.2f}MB")
                time.sleep(1)  # 처리 속도 감소
        except ImportError:
            logger.warning("psutil을 사용할 수 없어 메모리 모니터링이 비활성화됩니다")

    def _log_processing_summary(self) -> None:
        """처리 통계를 로깅합니다."""
        logger.info(
            f"처리 완료 - "
            f"성공: {self.metrics.processed_chunks}, "
            f"실패: {self.metrics.failed_chunks}, "
            f"소요시간: {self.metrics.processing_time:.2f}초"
        )
        
        if self.metrics.memory_usage:
            avg_memory = sum(self.metrics.memory_usage) / len(self.metrics.memory_usage)
            max_memory = max(self.metrics.memory_usage)
            logger.info(f"메모리 사용량 - 평균: {avg_memory:.2f}MB, 최대: {max_memory:.2f}MB")

    def stop(self) -> None:
        """처리를 중단합니다."""
        self._active = False

    def get_metrics(self) -> Dict[str, Any]:
        """처리 메트릭을 반환합니다.
        
        Returns:
            Dict[str, Any]: 처리 메트릭 정보
        """
        return {
            'processed_chunks': self.metrics.processed_chunks,
            'failed_chunks': self.metrics.failed_chunks,
            'processing_time': self.metrics.processing_time,
            'average_memory_usage': (
                sum(self.metrics.memory_usage) / len(self.metrics.memory_usage)
                if self.metrics.memory_usage else 0
            ),
            'max_memory_usage': max(self.metrics.memory_usage) if self.metrics.memory_usage else 0
        }