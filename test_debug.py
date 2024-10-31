"""디버그 및 테스트를 위한 스크립트

이 스크립트는 슬로우 쿼리 로그 처리 시스템의 각 컴포넌트를 
독립적으로 테스트할 수 있도록 합니다.

주요 중단점(breakpoint) 위치는 'BREAKPOINT:' 주석으로 표시되어 있습니다.
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Generator, Any
from dataclasses import dataclass

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mock 데이터 및 유틸리티
SAMPLE_SLOW_QUERIES = [
    """
# Time: 2024-03-15T10:00:01.123456Z
# User@Host: testuser[testuser] @ localhost []
# Query_time: 2.000000  Lock_time: 0.000123 Rows_sent: 1  Rows_examined: 1000000
SET timestamp=1647338401;
SELECT * FROM large_table WHERE id > 1000 AND complex_condition = 'test';
    """,
    """
# Time: 2024-03-15T10:00:02.123456Z
# User@Host: admin[admin] @ remote_host []
# Query_time: 1.500000  Lock_time: 0.000456 Rows_sent: 100  Rows_examined: 500000
use test_db;
SELECT t1.*, t2.name 
FROM table1 t1 
JOIN table2 t2 ON t1.id = t2.id 
WHERE t1.status = 'active';
    """
]

@dataclass
class MockConfig:
    instance_id: str = "test-instance"
    region: str = "us-east-1"
    min_query_time: float = 0.1
    es_host: str = "localhost:9200"
    es_index_prefix: str = "test-slowlog"
    batch_size: int = 100
    max_buffer_size_mb: int = 10
    chunk_size: int = 1000
    es_timeout: int = 30
    es_max_retries: int = 3
    
    def get_es_index_name(self) -> str:
        return f"{self.es_index_prefix}-{self.instance_id}"

class MockElasticsearchManager:
    def __init__(self):
        self.documents: List[Dict] = []
        
    def bulk_index(self, documents: List[Dict], index_name: str) -> tuple[int, int]:
        # BREAKPOINT: ES 인덱싱 시작 지점
        # - documents 내용 확인
        # - index_name 확인
        self.documents.extend(documents)
        return len(documents), 0

class MockLogStream:
    def __init__(self, sample_queries: List[str]):
        self.queries = sample_queries
        
    def download_db_log_file_portion(self, **kwargs) -> Dict[str, Any]:
        # BREAKPOINT: 로그 파일 청크 다운로드 시작 지점
        # - kwargs 파라미터 확인
        # - 반환되는 데이터 확인
        if not self.queries:
            return {"LogFileData": "", "AdditionalDataPending": False}
        return {
            "LogFileData": self.queries.pop(0),
            "AdditionalDataPending": bool(self.queries)
        }

def create_test_log_file():
    """테스트용 슬로우 쿼리 로그 파일을 생성합니다."""
    # BREAKPOINT: 테스트 파일 생성 시작 지점
    # - 생성되는 파일 내용 확인
    log_content = "\n".join(SAMPLE_SLOW_QUERIES)
    with open("test_slow_query.log", "w") as f:
        f.write(log_content)
    return "test_slow_query.log"

def test_query_normalizer():
    """QueryNormalizer 테스트"""
    from src.processors.query_normalizer import QueryNormalizer
    
    logger.info("=== QueryNormalizer 테스트 시작 ===")
    
    normalizer = QueryNormalizer()
    
    test_queries = [
        "SELECT * FROM users WHERE id = 123",
        "SELECT name, age FROM users WHERE id IN (1, 2, 3)",
        """
        SELECT t1.*, t2.name 
        FROM table1 t1 
        JOIN table2 t2 ON t1.id = t2.id 
        WHERE t1.status = 'active'
        """
    ]
    
    for query in test_queries:
        # BREAKPOINT: 각 쿼리 정규화 시작 지점
        # - 원본 쿼리 확인
        # - 정규화 과정 추적
        logger.info("\n원본 쿼리:")
        logger.info(query)
        
        normalized = normalizer.normalize_query(query)
        # BREAKPOINT: 정규화 결과 확인 지점
        # - 정규화된 쿼리 구조 확인
        # - 변환된 부분 확인
        logger.info("\n정규화된 쿼리:")
        logger.info(normalized)
        
        query_hash = normalizer.generate_hash(query)
        # BREAKPOINT: 해시 생성 결과 확인 지점
        # - 해시 값 확인
        # - 동일 쿼리의 해시 일관성 확인
        logger.info(f"\n쿼리 해시: {query_hash}")
        logger.info("-" * 50)
    
    logger.info(f"캐시 통계: {normalizer.get_cache_stats()}")

def test_chunk_processing():
    """청크 처리 테스트"""
    from src.processors.chunk_manager import LogChunkManager
    
    logger.info("=== 청크 처리 테스트 시작 ===")
    
    test_file = create_test_log_file()
    file_size = os.path.getsize(test_file)
    
    # BREAKPOINT: 청크 매니저 초기화 지점
    # - 설정값 확인
    # - 초기 상태 확인
    chunk_manager = LogChunkManager(
        instance_id="test-instance",
        region="us-east-1",
        max_chunk_size=1024,  # 1KB
        max_memory_usage=100   # 100MB
    )
    
    log_file = {
        "LogFileName": test_file,
        "Size": file_size
    }
    
    # BREAKPOINT: 청크 준비 시작 지점
    # - 파일 정보 확인
    # - 청크 분할 계획 확인
    chunk_manager.prepare_chunks(log_file)
    
    logger.info(f"청크 준비 완료: {chunk_manager.get_processing_stats()}")
    
    # 청크 처리
    for chunk_id, chunk_data in chunk_manager.get_next_chunk():
        # BREAKPOINT: 각 청크 처리 시작 지점
        # - 청크 데이터 확인
        # - 메모리 사용량 확인
        logger.info(f"\n처리 중인 청크: {chunk_id}")
        logger.info(f"청크 크기: {len(chunk_data)} bytes")
        chunk_manager.update_chunk_status(chunk_id, "completed")
    
    # BREAKPOINT: 청크 처리 완료 지점
    # - 최종 처리 통계 확인
    # - 메모리 사용량 확인
    logger.info(f"\n최종 처리 통계: {chunk_manager.get_processing_stats()}")
    
    os.remove(test_file)

def test_log_processor():
    """LogStreamProcessor 테스트"""
    from src.processors.log_processor import LogStreamProcessor
    from src.processors.query_normalizer import QueryNormalizer
    
    logger.info("=== LogStreamProcessor 테스트 시작 ===")
    
    # BREAKPOINT: 프로세서 초기화 지점
    # - 설정 확인
    # - 초기 상태 확인
    config = MockConfig()
    normalizer = QueryNormalizer()
    processor = LogStreamProcessor(config, normalizer)
    
    test_content = SAMPLE_SLOW_QUERIES[0]
    
    logger.info("\n테스트 컨텐츠:")
    logger.info(test_content)
    
    # BREAKPOINT: 쿼리 처리 시작 지점
    # - 입력 컨텐츠 확인
    # - 파싱 과정 추적
    results = list(processor.process_chunk("test_chunk", test_content))
    
    # BREAKPOINT: 처리 결과 확인 지점
    # - 파싱된 결과 구조 확인
    # - 메타데이터 추출 확인
    logger.info("\n처리 결과:")
    for result in results:
        logger.info(f"\n쿼리 시간: {result['query_time']}")
        logger.info(f"검사된 행 수: {result['rows_examined']}")
        logger.info(f"쿼리: {result['query'][:100]}...")

def test_full_pipeline():
    """전체 파이프라인 테스트"""
    from src.processors.slow_query import SlowQueryLogProcessor
    from src.processors.query_normalizer import QueryNormalizer
    
    logger.info("=== 전체 파이프라인 테스트 시작 ===")
    
    # BREAKPOINT: 파이프라인 초기화 지점
    # - 전체 구성요소 초기화 확인
    # - 설정값 확인
    config = MockConfig()
    normalizer = QueryNormalizer()
    es_manager = MockElasticsearchManager()
    
    processor = SlowQueryLogProcessor(
        config=config,
        query_normalizer=normalizer,
        es_manager=es_manager
    )
    
    # BREAKPOINT: 전체 처리 시작 지점
    # - 처리 흐름 추적
    # - 각 단계별 데이터 변환 확인
    results = processor.process_logs()
    
    # BREAKPOINT: 최종 결과 확인 지점
    # - 처리 통계 확인
    # - 성능 메트릭 확인
    logger.info("\n처리 결과:")
    logger.info(f"처리된 쿼리 수: {results['processed_queries']}")
    logger.info(f"인덱싱된 문서 수: {results['indexed_documents']}")
    logger.info(f"에러 수: {results['error_count']}")
    logger.info(f"총 소요 시간: {results['total_duration']:.2f}초")

def main():
    """모든 테스트를 실행합니다."""
    try:
        # BREAKPOINT: 전체 테스트 시작 지점
        # - 테스트 환경 설정 확인
        # - 초기 상태 확인
        
        test_query_normalizer()
        print("\n" + "="*50 + "\n")
        
        test_chunk_processing()
        print("\n" + "="*50 + "\n")
        
        test_log_processor()
        print("\n" + "="*50 + "\n")
        
        test_full_pipeline()
        
    except Exception as e:
        # BREAKPOINT: 에러 발생 지점
        # - 예외 정보 확인
        # - 스택 트레이스 확인
        logger.error(f"테스트 중 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()