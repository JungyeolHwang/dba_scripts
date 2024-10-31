"""슬로우 쿼리 로그 처리의 메인 프로세서

이 모듈은 RDS 슬로우 쿼리 로그를 처리하고 Elasticsearch에 인덱싱하는 
전체 프로세스를 조율합니다.
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from src.config.config import Config
from src.processors.query_normalizer import QueryNormalizer
from src.processors.log_processor import LogStreamProcessor
from src.elasticsearch.manager import ElasticsearchManager
from src.utils.decorators import monitor_memory

logger = logging.getLogger(__name__)

@dataclass
class ProcessingStats:
    """처리 통계를 관리하는 데이터 클래스"""
    processed: int = 0
    indexed: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    batch_stats: List[Dict] = field(default_factory=list)

    def add_batch_stat(self, batch_size: int, duration: float) -> None:
        """배치 처리 통계를 추가합니다."""
        self.batch_stats.append({
            'timestamp': datetime.now().isoformat(),
            'batch_size': batch_size,
            'duration_seconds': duration,
            'success_count': self.indexed,
            'error_count': self.errors
        })

    def get_summary(self) -> Dict[str, Any]:
        """처리 통계 요약을 반환합니다."""
        return {
            'processed_queries': self.processed,
            'indexed_documents': self.indexed,
            'error_count': self.errors,
            'total_duration': time.time() - self.start_time,
            'batches_processed': len(self.batch_stats),
            'average_batch_duration': (
                sum(s['duration_seconds'] for s in self.batch_stats) 
                / len(self.batch_stats) if self.batch_stats else 0
            )
        }
       
class SlowQueryLogProcessor:
    """슬로우 쿼리 로그 처리기
    
    이 클래스는 슬로우 쿼리 로그의 전체 처리 프로세스를 관리합니다.
    로그 스트리밍, 쿼리 정규화, Elasticsearch 인덱싱을 조율합니다.
    
    Attributes:
        config (Config): 애플리케이션 설정
        query_normalizer (QueryNormalizer): SQL 쿼리 정규화기
        es_manager (ElasticsearchManager): Elasticsearch 관리자
        stream_processor (LogStreamProcessor): 로그 스트림 처리기
        _batch (List[Dict]): 현재 처리 중인 배치
        _stats (ProcessingStats): 처리 통계
    """
    
    def __init__(
        self, 
        config: Config,
        query_normalizer: Optional[QueryNormalizer] = None,
        es_manager: Optional[ElasticsearchManager] = None,
        stream_processor: Optional[LogStreamProcessor] = None
    ):
        """초기화 메서드
        
        Args:
            config: 애플리케이션 설정
            query_normalizer: 쿼리 정규화기 (선택)
            es_manager: Elasticsearch 관리자 (선택)
            stream_processor: 로그 스트림 처리기 (선택)
        """
        self.config = config
        self.query_normalizer = query_normalizer or QueryNormalizer()
        self.es_manager = es_manager or ElasticsearchManager(config)
        self.stream_processor = stream_processor or LogStreamProcessor(config, self.query_normalizer)
        
        self._batch: List[Dict] = []
        self._stats = ProcessingStats()

    @monitor_memory
    def process_logs(self) -> Dict[str, Any]:
        """로그 처리를 실행합니다.
        
        Returns:
            Dict[str, Any]: 처리 통계
            
        Raises:
            Exception: 처리 중 오류 발생 시
        """
        try:
            logger.info("로그 처리 시작")
            start_time = time.time()
            
            log_stream = self.stream_processor.stream_log_files()
            
            for query_data in self.stream_processor.process_streaming_content(log_stream):
                self._stats.processed += 1
                
                try:
                    es_doc = self._prepare_es_document(query_data)
                    self._batch.append(es_doc)
                    
                    if len(self._batch) >= self.config.batch_size:
                        self._process_batch()
                        
                except Exception as e:
                    logger.error(f"문서 준비 중 오류 발생: {str(e)}")
                    self._stats.errors += 1
                    continue
            
            # 마지막 배치 처리
            if self._batch:
                self._process_batch()
                
            duration = time.time() - start_time
            logger.info(f"로그 처리 완료 (소요 시간: {duration:.2f}초)")
            
            return self._stats.get_summary()
            
        except Exception as e:
            logger.error(f"로그 처리 중 오류 발생: {str(e)}")
            raise
        finally:
            self._cleanup()

    def _prepare_es_document(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """Elasticsearch 문서를 준비합니다.
        
        Args:
            query_data: 쿼리 데이터
            
        Returns:
            Dict[str, Any]: ES 문서
        """
        try:
            normalized_query = self.query_normalizer.normalize_query(query_data['query'])
            query_hash = self.query_normalizer.generate_hash(query_data['query'])
            
            current_timestamp = query_data['timestamp']
            index_name = self.config.get_es_index_name()
            
            document = {
                '_op_type': 'update',
                '_index': index_name,
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
                    'instance_id': self.config.instance_id
                },
                'retry_on_conflict': 3
            }
            
            logger.debug(f"ES 문서 준비 완료: {query_hash}")
            return document
            
        except Exception as e:
            logger.error(f"ES 문서 준비 중 오류 발생: {str(e)}")
            raise

    def _process_batch(self) -> None:
        """현재 배치를 처리합니다."""
        if not self._batch:
            return
            
        try:
            logger.info(f"배치 처리 시작 (크기: {len(self._batch)})")
            batch_start = time.time()
            
            index_name = self.config.get_es_index_name()
            indexed_count = self.es_manager.bulk_index(self._batch, index_name)
            
            # indexed_count가 tuple인 경우 첫 번째 값(성공 횟수) 사용
            if isinstance(indexed_count, tuple):
                success_count, failed_count = indexed_count
                self._stats.indexed += success_count
                self._stats.errors += failed_count
            else:
                self._stats.indexed += indexed_count
            
            duration = time.time() - batch_start
            self._stats.add_batch_stat(len(self._batch), duration)
            
            logger.info(
                f"배치 처리 완료 - 인덱싱: {self._stats.indexed}, "
                f"실패: {self._stats.errors}, "
                f"소요 시간: {duration:.2f}초"
            )
            
        except Exception as e:
            logger.error(f"배치 처리 중 오류 발생: {str(e)}")
            self._stats.errors += len(self._batch)
            raise
        finally:
            self._batch = []

    def _cleanup(self) -> None:
        """리소스를 정리합니다."""
        self._batch = []
        stats_summary = self._stats.get_summary()
        logger.info(f"처리 통계: {json.dumps(stats_summary, indent=2)}")

    def get_processing_stats(self) -> Dict[str, Any]:
        """현재 처리 통계를 반환합니다."""
        return self._stats.get_summary()

    async def process_logs_async(self) -> Dict[str, Any]:
        """비동기 로그 처리를 위한 메서드 (향후 구현)"""
        raise NotImplementedError("Async processing not yet implemented")