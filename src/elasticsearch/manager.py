"""Elasticsearch 연결 및 인덱싱을 관리하는 모듈

이 모듈은 Elasticsearch와의 연결, 문서 인덱싱,
인덱스 관리 기능을 제공합니다.
"""

import json
import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import (
    ConnectionError, 
    ConnectionTimeout, 
    RequestError,
    TransportError
)

from src.config.config import Config
from src.utils.decorators import monitor_memory

logger = logging.getLogger(__name__)

@dataclass
class IndexingStats:
    """인덱싱 통계를 관리하는 데이터 클래스
    
    Attributes:
        successful_docs (int): 성공적으로 인덱싱된 문서 수
        failed_docs (int): 인덱싱 실패한 문서 수
        total_bytes (int): 처리된 총 바이트 수
        retries (int): 재시도 횟수
        batch_stats (List[Dict]): 배치별 처리 통계
    """
    successful_docs: int = 0
    failed_docs: int = 0
    total_bytes: int = 0
    retries: int = 0
    batch_stats: List[Dict] = field(default_factory=list)

    def add_batch_stat(self, success: int, failed: int, duration: float) -> None:
        """배치 처리 통계를 추가합니다."""
        self.batch_stats.append({
            'timestamp': time.time(),
            'successful': success,
            'failed': failed,
            'duration_seconds': duration
        })

    def get_summary(self) -> Dict[str, Any]:
        """통계 요약을 반환합니다."""
        return {
            'successful_documents': self.successful_docs,
            'failed_documents': self.failed_docs,
            'total_bytes_processed': self.total_bytes,
            'retry_count': self.retries,
            'total_batches': len(self.batch_stats),
            'average_batch_success_rate': (
                self.successful_docs / (self.successful_docs + self.failed_docs)
                if (self.successful_docs + self.failed_docs) > 0 else 0
            )
        }

class ElasticsearchManager:
    """Elasticsearch 관리자 클래스
    
    이 클래스는 Elasticsearch 연결과 작업을 관리합니다.
    
    Attributes:
        config (Config): 애플리케이션 설정
        es_client (Elasticsearch): ES 클라이언트
        _stats (IndexingStats): 인덱싱 통계
        _index_mappings (Dict): 인덱스 매핑 설정
    """
    
    def __init__(self, config: Config):
        """초기화 메서드
        
        Args:
            config: 애플리케이션 설정
        """
        self.config = config
        self.es_client: Optional[Elasticsearch] = None
        self._stats = IndexingStats()
        self._index_mappings = self._get_default_mappings()
        self._setup_client()

    def _setup_client(self) -> None:
        """ES 클라이언트를 설정합니다."""
        if not self.config.es_host:
            logger.error("ES_HOST가 설정되지 않았습니다")
            raise ValueError("ES_HOST configuration is required")
            
        try:
            logger.info(f"Elasticsearch 연결 시도: {self.config.es_host}")
            
            self.es_client = Elasticsearch(
                [self.config.es_host],
                timeout=self.config.es_connection_timeout,
                retry_on_timeout=True,
                max_retries=self.config.es_retry_count
            )
            
            # 연결 테스트
            if not self.es_client.ping():
                raise ConnectionError("Elasticsearch 연결 테스트 실패")
                
            logger.info(f"Elasticsearch 연결 성공: {self.es_client.info()}")
            
        except Exception as e:
            logger.error(f"Elasticsearch 연결 실패: {str(e)}")
            raise

    @staticmethod
    def _get_default_mappings() -> Dict[str, Any]:
        """기본 인덱스 매핑을 반환합니다."""
        return {
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
                "number_of_replicas": 1,
                "index.mapping.total_fields.limit": 2000,
                "index.refresh_interval": "5s"
            }
        }

    @monitor_memory
    def bulk_index(self, documents: List[Dict[str, Any]], index_name: str) -> int:
        """문서들을 벌크 인덱싱합니다.
        
        Args:
            documents: 인덱싱할 문서들
            index_name: 대상 인덱스 이름
            
        Returns:
            int: 성공적으로 인덱싱된 문서 수
            
        Raises:
            Exception: 인덱싱 실패 시
        """
        if not self.es_client:
            logger.error("ES 클라이언트가 초기화되지 않았습니다")
            return 0
            
        if not documents:
            logger.warning("인덱싱할 문서가 없습니다")
            return 0
            
        try:
            # 인덱스 존재 확인 및 생성
            self._ensure_index_exists(index_name)
            
            # 벌크 인덱싱 시작
            logger.info(f"{len(documents)}개 문서 벌크 인덱싱 시작")
            start_time = time.time()
            
            success, failed = helpers.bulk(
                self.es_client,
                documents,
                stats_only=True,
                refresh=True,
                request_timeout=30,
                raise_on_error=False
            )
            
            duration = time.time() - start_time
            
            # 통계 업데이트
            self._stats.successful_docs += success
            self._stats.failed_docs += failed
            self._stats.add_batch_stat(success, failed, duration)
            
            logger.info(f"벌크 인덱싱 완료 - 성공: {success}, 실패: {failed}, 소요시간: {duration:.2f}초")
            
            if failed > 0:
                logger.warning(f"{failed}개 문서 인덱싱 실패")
            
            return success
            
        except helpers.BulkIndexError as e:
            logger.error(f"벌크 인덱싱 중 오류 발생: {str(e)}")
            self._handle_bulk_errors(e.errors)
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {str(e)}")
            raise

    def _ensure_index_exists(self, index_name: str) -> None:
        """인덱스 존재를 확인하고, 없으면 생성합니다.
        
        Args:
            index_name: 인덱스 이름
        """
        try:
            if not self.es_client.indices.exists(index=index_name):
                logger.info(f"인덱스 생성: {index_name}")
                self.es_client.indices.create(
                    index=index_name,
                    body=self._index_mappings
                )
                logger.info(f"인덱스 생성 완료: {index_name}")
        except Exception as e:
            logger.error(f"인덱스 생성 중 오류 발생: {str(e)}")
            raise

    def _handle_bulk_errors(self, errors: List[Dict[str, Any]]) -> None:
        """벌크 인덱싱 오류를 처리합니다.
        
        Args:
            errors: 오류 목록
        """
        error_types = {}
        for error in errors:
            error_type = error.get('index', {}).get('error', {}).get('type', 'unknown')
            error_types[error_type] = error_types.get(error_type, 0) + 1
            
        logger.error(f"벌크 인덱싱 오류 요약: {json.dumps(error_types, indent=2)}")

    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """인덱스 통계를 조회합니다.
        
        Args:
            index_name: 인덱스 이름
            
        Returns:
            Dict[str, Any]: 인덱스 통계
        """
        try:
            if not self.es_client.indices.exists(index=index_name):
                return {}
                
            stats = self.es_client.indices.stats(index=index_name)
            return {
                'doc_count': stats['_all']['total']['docs']['count'],
                'store_size_bytes': stats['_all']['total']['store']['size_in_bytes'],
                'indexing_stats': stats['_all']['total']['indexing']
            }
        except Exception as e:
            logger.error(f"인덱스 통계 조회 중 오류 발생: {str(e)}")
            return {}

    def get_indexing_stats(self) -> Dict[str, Any]:
        """인덱싱 통계를 반환합니다."""
        return self._stats.get_summary()

    def cleanup_old_documents(self, index_name: str, days: int = 30) -> int:
        """오래된 문서를 정리합니다.
        
        Args:
            index_name: 인덱스 이름
            days: 보관할 기간(일)
            
        Returns:
            int: 삭제된 문서 수
        """
        try:
            if not self.es_client.indices.exists(index=index_name):
                return 0

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
            
            deleted = response.get('deleted', 0)
            logger.info(f"{deleted}개 문서 정리 완료")
            return deleted
            
        except Exception as e:
            logger.error(f"문서 정리 중 오류 발생: {str(e)}")
            return 0

    def close(self) -> None:
        """ES 클라이언트 연결을 종료합니다."""
        if self.es_client:
            try:
                self.es_client.close()
                logger.info("ES 클라이언트 연결 종료")
            except Exception as e:
                logger.error(f"ES 클라이언트 종료 중 오류 발생: {str(e)}")