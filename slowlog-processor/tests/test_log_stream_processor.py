import pytest
import os
from src.lambda_slowlog_processor import Config, LogStreamProcessor, QueryNormalizer

# 테스트용 로그 파일 생성을 위한 fixture
@pytest.fixture
def sample_log_file(tmp_path):
    """테스트용 샘플 슬로우 쿼리 로그 파일 생성"""
    log_content = """
# Time: 2024-03-15T10:00:01.123456Z
# User@Host: testuser[testuser] @ localhost []
# Query_time: 2.000000  Lock_time: 0.000000 Rows_sent: 1000  Rows_examined: 10000
# Schema: testdb  Last_errno: 0  Killed: 0
# Query_time: 2.000000  Lock_time: 0.000000  Rows_sent: 1000  Rows_examined: 10000
SELECT * FROM users WHERE id > 1000;

# Time: 2024-03-15T10:00:02.123456Z
# User@Host: testuser[testuser] @ localhost []
# Query_time: 3.000000  Lock_time: 0.000000 Rows_sent: 2000  Rows_examined: 20000
SELECT u.*, p.* FROM users u JOIN profiles p ON u.id = p.user_id WHERE u.age > 25;
"""
    log_file = tmp_path / "slowquery.log"
    log_file.write_text(log_content)
    return log_file

@pytest.mark.unit
class TestLogStreamProcessor:
    def test_process_valid_log(self):
        """단일 로그 항목 처리 단위 테스트"""
        config = Config(instance_id='test')
        processor = LogStreamProcessor(config, QueryNormalizer())
        
        valid_log = """
# Time: 2024-03-15T10:00:01.123456Z
# User@Host: testuser[testuser] @ localhost []
# Query_time: 2.000000  Lock_time: 0.000000 Rows_sent: 1000  Rows_examined: 10000
SELECT * FROM users WHERE id > 1000;
        """
        
        def stream_generator():
            yield valid_log
            
        results = list(processor.process_streaming_content(stream_generator()))
        assert len(results) == 1
        assert results[0]['query_time'] == 2.0
        assert results[0]['rows_examined'] == 10000

    def test_process_invalid_log(self):
        """잘못된 형식의 로그 처리 테스트"""
        config = Config(instance_id='test')
        processor = LogStreamProcessor(config, QueryNormalizer())
        
        invalid_log = "Invalid log format\nNo proper headers\n"
        
        def stream_generator():
            yield invalid_log
            
        results = list(processor.process_streaming_content(stream_generator()))
        assert len(results) == 0

@pytest.mark.integration
class TestLogStreamProcessorIntegration:
    def test_file_processing(self, sample_log_file):
        """전체 로그 파일 처리 통합 테스트"""
        config = Config(
            instance_id='test',
            min_query_time=0.1  # 낮은 임계값 설정으로 모든 쿼리 포함
        )
        processor = LogStreamProcessor(config, QueryNormalizer())
        
        # 파일 내용을 스트림으로 변환
        def file_stream():
            with open(sample_log_file, 'r') as f:
                yield f.read()
        
        # 전체 로그 파일 처리
        results = list(processor.process_streaming_content(file_stream()))
        
        # 검증
        assert len(results) == 2  # 샘플 로그의 쿼리 수
        
        # 첫 번째 쿼리 검증
        assert results[0]['query_time'] == 2.0
        assert results[0]['rows_examined'] == 10000
        assert "SELECT * FROM users" in results[0]['query']
        
        # 두 번째 쿼리 검증
        assert results[1]['query_time'] == 3.0
        assert results[1]['rows_examined'] == 20000
        assert "JOIN profiles" in results[1]['query']

    def test_large_file_processing(self, tmp_path):
        """대용량 로그 파일 처리 통합 테스트"""
        # 큰 로그 파일 생성
        large_log_file = tmp_path / "large_slowquery.log"
        with open(large_log_file, 'w') as f:
            for i in range(100):  # 100개의 쿼리 로그 생성
                f.write(f"""
# Time: 2024-03-15T10:00:{i:02d}.123456Z
# User@Host: testuser[testuser] @ localhost []
# Query_time: {i/10 + 1}.000000  Lock_time: 0.000000 Rows_sent: {1000 * (i+1)}  Rows_examined: {10000 * (i+1)}
SELECT * FROM large_table_{i} WHERE id > {i*1000};

""")

        config = Config(
            instance_id='test',
            min_query_time=0.1,
            max_buffer_size_mb=1  # 작은 버퍼 크기로 설정하여 청크 처리 테스트
        )
        processor = LogStreamProcessor(config, QueryNormalizer())

        # 파일 스트림 생성
        def file_stream():
            with open(large_log_file, 'r') as f:
                yield f.read()

        # 처리 및 검증
        results = list(processor.process_streaming_content(file_stream()))
        assert len(results) == 100
        
        # 처리된 쿼리들의 특성 검증
        for i, result in enumerate(results):
            assert result['query_time'] == (i/10 + 1)
            assert result['rows_examined'] == 10000 * (i+1)
            assert f"large_table_{i}" in result['query']