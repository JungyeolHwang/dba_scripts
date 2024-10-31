"""Elasticsearch 연결 및 최적화된 인덱싱을 관리하는 모듈

이 모듈은 Elasticsearch와의 연결, 동적 배치 크기 조절,
자동 재시도, 성능 모니터링 기능을 제공합니다.
"""

import time
import logging
import statistics
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import (
    ConnectionError, 
    ConnectionTimeout,
    TransportError
)

from src.config.config import Config
from src.utils.decorators import monitor_memory

logger = logging.getLogger(__name__)

@dataclass
class IndexingMetrics:
    """인덱싱 성능 메트릭을 관리하는 데이터 클래스
    
    Attributes:
        successful_docs: 성공적으로 인덱싱된 문서 수
        failed_docs: 실패한 문서 수
        total_bytes: 처리된 총 바이트 수
        total_time: 총 소요 시간
        batch_sizes: 배치 크기 이력
        batch_times: 배치 처리 시간 이력
        error_counts: 에러 유형별 발생 횟수
    """
    successful_docs: int = 0
    failed_docs: int = 0
    total_bytes: int = 0
    total_time: float = 0.0
    batch_sizes: List[int] = field(default_factory=list)
    batch_times: List[float] = field(default_factory=list)
    error_counts: Dict[str, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    def add_batch_metric(self, size: int, duration: float, success: bool) -> None:
        """배치 처리 메트릭을 추가합니다."""
        self.batch_sizes.append(size)
        self.batch_times.append(duration)
        if success:
            self.successful_docs += size
        else:
            self.failed_docs += size
        self.total_time += duration

    def record_error(self, error_type: str) -> None:
        """에러 발생을 기록합니다."""
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def get_summary(self) -> Dict[str, Any]:
        """메트릭 요약을 생성합니다."""
        total_docs = self.successful_docs + self.failed_docs
        avg_batch_size = statistics.mean(self.batch_sizes) if self.batch_sizes else 0
        avg_batch_time = statistics.mean(self.batch_times) if self.batch_times else 0
        
        return {
            'total_documents': total_docs,
            'successful_documents': self.successful_docs,
            'failed_documents': self.failed_docs,
            'success_rate': (self.successful_docs / total_docs * 100) if total_docs > 0 else 0,
            'total_processing_time': self.total_time,
            'average_batch_size': avg_batch_size,
            'average_batch_time': avg_batch_time,
            'documents_per_second': total_docs / self.total_time if self.total_time > 0 else 0,
            'error_distribution': dict(self.error_counts)
        }

class ElasticsearchManager:
    """Elasticsearch 관리자 클래스
    
    이 클래스는 ES 연결과 인덱싱 작업을 최적화된 방식으로 관리합니다.
    
    Attributes:
        config: 애플리케이션 설정
        es_client: ES 클라이언트
        metrics: 인덱싱 메트릭
        current_batch_size: 현재 배치 크기
        _index_templates: 인덱스 템플릿
    """
    
    def __init__(
        self, 
        config: Config,
        initial_batch_size: int = 1000,
        min_batch_size: int = 100,
        max_batch_size: int = 5000,
        performance_window: int = 10
    ):
        """초기화 메서드"""
        self.config = config
        self.es_client: Optional[Elasticsearch] = None
        self.metrics = IndexingMetrics()
        
        self.initial_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.performance_window = performance_window
        
        self.current_batch_size = initial_batch_size
        self._index_templates = self._get_index_templates()
        
        self._setup_client()
        self._setup_index_template()

    def _setup_client(self) -> None:
        """ES 클라이언트를 설정합니다."""
        try:
            self.es_client = Elasticsearch(
                [self.config.es_host],
                timeout=self.config.es_timeout,
                retry_on_timeout=True,
                max_retries=self.config.es_max_retries
            )
            
            if not self.es_client.ping():
                raise ConnectionError("Elasticsearch 연결 실패")
                
            logger.info(f"Elasticsearch 연결 성공: {self.es_client.info()}")
            
        except Exception as e:
            logger.error(f"Elasticsearch 연결 실패: {str(e)}")
            raise

    def _get_index_templates(self) -> Dict[str, Any]:
        """인덱스 템플릿을 정의합니다."""
        return {
            "template": "slowquery-*",
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "refresh_interval": "30s",
                "index.mapping.total_fields.limit": 2000,
                "index.translog.durability": "async",
                "index.translog.sync_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "query": {"type": "text"},
                    "normalized_query": {"type": "text"},
                    "query_hash": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "query_time": {"type": "float"},
                    "lock_time": {"type": "float"},
                    "rows_examined": {"type": "long"},
                    "rows_sent": {"type": "long"},
                    "execution_count": {"type": "long"},
                    "timestamps": {"type": "date"},
                    "last_seen": {"type": "date"},
                    "max_query_time": {"type": "float"},
                    "instance_id": {"type": "keyword"}
                }
            }
        }

    def _setup_index_template(self) -> None:
        """인덱스 템플릿을 설정합니다."""
        try:
            if self.es_client.indices.exists_template(name="slowquery"):
                self.es_client.indices.delete_template(name="slowquery")
            
            self.es_client.indices.put_template(
                name="slowquery",
                body=self._index_templates
            )
            logger.info("인덱스 템플릿 설정 완료")
            
        except Exception as e:
            logger.error(f"인덱스 템플릿 설정 실패: {str(e)}")
            raise

    @monitor_memory
    def bulk_index(
        self, 
        documents: List[Dict[str, Any]], 
        index_name: str,
        chunk_size: Optional[int] = None
    ) -> Tuple[int, int]:
        """문서들을 최적화된 방식으로 벌크 인덱싱합니다.
        
        Args:
            documents: 인덱싱할 문서들
            index_name: 대상 인덱스
            chunk_size: 청크 크기 (선택)
            
        Returns:
            Tuple[int, int]: (성공, 실패) 문서 수
        """
        if not documents:
            return 0, 0

        batch_size = chunk_size or self.current_batch_size
        total_success = 0
        total_failed = 0
        
        try:
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                success, failed = self._process_batch(batch, index_name)
                
                total_success += success
                total_failed += failed
                
            self._adjust_batch_size()
            return total_success, total_failed
            
        except Exception as e:
            logger.error(f"벌크 인덱싱 실패: {str(e)}")
            self.metrics.record_error(type(e).__name__)
            return total_success, total_failed

    def _process_batch(
        self, 
        batch: List[Dict[str, Any]], 
        index_name: str
    ) -> Tuple[int, int]:
        """단일 배치를 처리합니다."""
        start_time = time.time()
        retry_count = 0
        max_retries = self.config.es_max_retries
        
        while retry_count <= max_retries:
            try:
                success, failed = helpers.bulk(
                    self.es_client,
                    batch,
                    index=index_name,
                    chunk_size=len(batch),
                    request_timeout=30,
                    raise_on_error=False
                )
                
                duration = time.time() - start_time
                self.metrics.add_batch_metric(
                    len(batch), 
                    duration,
                    success == len(batch)
                )
                
                return success, failed
                
            except TransportError as e:
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = min(2 ** retry_count, 30)
                    logger.warning(
                        f"배치 처리 실패 (시도 {retry_count}/{max_retries}), "
                        f"{wait_time}초 후 재시도: {str(e)}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"최대 재시도 횟수 초과: {str(e)}")
                    self.metrics.record_error("TransportError")
                    return 0, len(batch)
                    
            except Exception as e:
                logger.error(f"예상치 못한 에러: {str(e)}")
                self.metrics.record_error(type(e).__name__)
                return 0, len(batch)

    def _adjust_batch_size(self) -> None:
        """성능 메트릭을 기반으로 배치 크기를 조절합니다."""
        if len(self.metrics.batch_times) < self.performance_window:
            return
            
        recent_times = self.metrics.batch_times[-self.performance_window:]
        avg_time = statistics.mean(recent_times)
        
        if avg_time < 1.0 and self.current_batch_size < self.max_batch_size:
            # 처리가 빠르면 배치 크기 증가
            self.current_batch_size = min(
                int(self.current_batch_size * 1.2),
                self.max_batch_size
            )
        elif avg_time > 3.0 and self.current_batch_size > self.min_batch_size:
            # 처리가 느리면 배치 크기 감소
            self.current_batch_size = max(
                int(self.current_batch_size * 0.8),
                self.min_batch_size
            )

    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """인덱스 통계를 조회합니다."""
        try:
            if not self.es_client.indices.exists(index=index_name):
                return {}
                
            stats = self.es_client.indices.stats(index=index_name)
            return {
                'document_count': stats['_all']['total']['docs']['count'],
                'store_size_bytes': stats['_all']['total']['store']['size_in_bytes'],
                'indexing_stats': {
                    'index_total': stats['_all']['total']['indexing']['index_total'],
                    'index_time_ms': stats['_all']['total']['indexing']['index_time_in_millis'],
                    'index_failed': stats['_all']['total']['indexing']['index_failed']
                }
            }
        except Exception as e:
            logger.error(f"인덱스 통계 조회 실패: {str(e)}")
            return {}

    def get_performance_metrics(self) -> Dict[str, Any]:
        """성능 메트릭을 반환합니다."""
        return {
            **self.metrics.get_summary(),
            'current_batch_size': self.current_batch_size
        }

    def cleanup_old_documents(self, index_name: str, days: int = 30) -> int:
        """오래된 문서를 정리합니다."""
        try:
            response = self.es_client.delete_by_query(
                index=index_name,
                body={
                    "query": {
                        "range": {
                            "last_seen": {
                                "lte": f"now-{days}d"
                            }
                        }
                    }
                }
            )
            return response.get('deleted', 0)
            
        except Exception as e:
            logger.error(f"문서 정리 실패: {str(e)}")
            return 0

    def close(self) -> None:
        """리소스를 정리합니다."""
        if self.es_client:
            self.es_client.close()
            logger.info("Elasticsearch 연결 종료")