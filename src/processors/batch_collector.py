"""배치 처리 결과를 수집하고 벌크 인덱싱을 관리하는 모듈

이 모듈은 처리된 쿼리 결과를 메모리 효율적으로 수집하고
Elasticsearch 벌크 인덱싱을 최적화하는 기능을 제공합니다.
"""

import time
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import deque

from src.elasticsearch.manager import ElasticsearchManager
from src.utils.decorators import monitor_memory

logger = logging.getLogger(__name__)

@dataclass
class BatchStats:
    """배치 처리 통계를 관리하는 데이터 클래스
    
    Attributes:
        total_documents: 총 처리된 문서 수
        current_batch_size: 현재 배치 크기
        total_batches: 총 처리된 배치 수
        failed_documents: 실패한 문서 수
        total_processing_time: 총 처리 시간
        avg_batch_size: 평균 배치 크기
        batch_size_history: 배치 크기 이력
    """
    total_documents: int = 0
    current_batch_size: int = 0
    total_batches: int = 0
    failed_documents: int = 0
    total_processing_time: float = 0
    avg_batch_size: float = 0
    batch_size_history: List[int] = field(default_factory=list)

    def update_batch_stats(self, batch_size: int, processing_time: float) -> None:
        """배치 통계를 업데이트합니다."""
        self.total_batches += 1
        self.total_documents += batch_size
        self.total_processing_time += processing_time
        self.batch_size_history.append(batch_size)
        self.avg_batch_size = self.total_documents / self.total_batches

class BatchCollector:
    """배치 수집 및 인덱싱 관리 클래스
    
    이 클래스는 처리된 쿼리 결과를 수집하고 최적화된 방식으로
    Elasticsearch에 인덱싱하는 역할을 담당합니다.
    
    Attributes:
        es_manager: Elasticsearch 관리자
        initial_batch_size: 초기 배치 크기
        max_batch_size: 최대 배치 크기
        min_batch_size: 최소 배치 크기
        batch_timeout: 배치 타임아웃(초)
        current_batch: 현재 배치 데이터
        stats: 배치 처리 통계
    """
    
    def __init__(
        self,
        es_manager: ElasticsearchManager,
        initial_batch_size: int = 1000,
        max_batch_size: int = 5000,
        min_batch_size: int = 100,
        batch_timeout: float = 30.0
    ):
        """초기화 메서드
        
        Args:
            es_manager: Elasticsearch 관리자
            initial_batch_size: 초기 배치 크기
            max_batch_size: 최대 배치 크기
            min_batch_size: 최소 배치 크기
            batch_timeout: 배치 타임아웃(초)
        """
        self.es_manager = es_manager
        self.initial_batch_size = initial_batch_size
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
        self.batch_timeout = batch_timeout
        
        self.current_batch: deque = deque(maxlen=max_batch_size)
        self.stats = BatchStats()
        self._last_flush_time = time.time()

    @monitor_memory
    def add_document(self, document: Dict[str, Any]) -> None:
        """문서를 현재 배치에 추가합니다.
        
        Args:
            document: 추가할 문서
        """
        self.current_batch.append(document)
        self.stats.current_batch_size = len(self.current_batch)
        
        # 배치 크기나 타임아웃 체크
        if self._should_flush():
            self.flush()

    def _should_flush(self) -> bool:
        """현재 배치를 플러시해야 하는지 확인합니다.
        
        Returns:
            bool: 플러시 필요 여부
        """
        current_time = time.time()
        timeout_reached = (current_time - self._last_flush_time) >= self.batch_timeout
        size_reached = len(self.current_batch) >= self._get_optimal_batch_size()
        
        return timeout_reached or size_reached

    def _get_optimal_batch_size(self) -> int:
        """현재 상황에 최적화된 배치 크기를 계산합니다.
        
        Returns:
            int: 최적 배치 크기
        """
        if not self.stats.batch_size_history:
            return self.initial_batch_size
            
        # 최근 성능을 기반으로 배치 크기 조정
        recent_batches = self.stats.batch_size_history[-5:]
        avg_recent_size = sum(recent_batches) / len(recent_batches)
        
        if self.stats.failed_documents > 0:
            # 실패가 있으면 배치 크기 감소
            optimal_size = int(avg_recent_size * 0.8)
        else:
            # 성공적이면 배치 크기 증가
            optimal_size = int(avg_recent_size * 1.2)
            
        # 범위 제한
        return max(self.min_batch_size, min(self.max_batch_size, optimal_size))

    @monitor_memory
    def flush(self, force: bool = False) -> Optional[Dict[str, Any]]:
        """현재 배치를 Elasticsearch에 인덱싱합니다.
        
        Args:
            force: 강제 플러시 여부
            
        Returns:
            Optional[Dict[str, Any]]: 인덱싱 결과
        """
        if not self.current_batch and not force:
            return None
            
        try:
            start_time = time.time()
            batch_size = len(self.current_batch)
            
            if batch_size > 0:
                # 벌크 인덱싱 수행
                documents = list(self.current_batch)
                index_name = self.es_manager.get_current_index_name()
                indexed = self.es_manager.bulk_index(documents, index_name)
                
                # 통계 업데이트
                processing_time = time.time() - start_time
                self.stats.update_batch_stats(batch_size, processing_time)
                
                if indexed < batch_size:
                    self.stats.failed_documents += (batch_size - indexed)
                    
                logger.info(
                    f"Batch flushed - Size: {batch_size}, "
                    f"Time: {processing_time:.2f}s, "
                    f"Indexed: {indexed}"
                )
                
            # 배치 초기화
            self.current_batch.clear()
            self._last_flush_time = time.time()
            
            return self._create_flush_result(batch_size)
            
        except Exception as e:
            logger.error(f"Batch flush failed: {str(e)}")
            self.stats.failed_documents += len(self.current_batch)
            raise

    def _create_flush_result(self, batch_size: int) -> Dict[str, Any]:
        """플러시 결과를 생성합니다.
        
        Args:
            batch_size: 처리된 배치 크기
            
        Returns:
            Dict[str, Any]: 플러시 결과
        """
        return {
            'flushed_documents': batch_size,
            'total_documents': self.stats.total_documents,
            'failed_documents': self.stats.failed_documents,
            'total_batches': self.stats.total_batches,
            'avg_batch_size': self.stats.avg_batch_size,
            'total_processing_time': self.stats.total_processing_time
        }

    def get_stats(self) -> Dict[str, Any]:
        """현재 배치 처리 통계를 반환합니다.
        
        Returns:
            Dict[str, Any]: 배치 처리 통계
        """
        return {
            'current_batch_size': self.stats.current_batch_size,
            'total_documents': self.stats.total_documents,
            'total_batches': self.stats.total_batches,
            'failed_documents': self.stats.failed_documents,
            'avg_batch_size': self.stats.avg_batch_size,
            'total_processing_time': self.stats.total_processing_time,
            'documents_per_second': (
                self.stats.total_documents / self.stats.total_processing_time
                if self.stats.total_processing_time > 0 else 0
            )
        }