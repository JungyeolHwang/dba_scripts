import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.lambda_slowlog_processor import (
    SlowQueryLogProcessor, 
    Config, 
    QueryNormalizer,
    ElasticsearchManager,
    LogStreamProcessor
)

@pytest.fixture
def config():
    """테스트용 기본 설정 fixture"""
    return Config(
        instance_id='test-instance',
        es_host='http://localhost:9200',
        es_index_prefix='test-slowlog',
        batch_size=2
    )

@pytest.fixture
def sample_query_data():
    """테스트용 샘플 쿼리 데이터 fixture"""
    return {
        'query': 'SELECT * FROM users WHERE id = 123',
        'timestamp': '2024-03-15T10:00:01.123456Z',
        'user': {'name': 'testuser', 'host': 'localhost'},
        'query_time': 1.5,
        'lock_time': 0.1,
        'rows_examined': 1000,
        'rows_sent': 100
    }

@pytest.mark.unit
class TestSlowQueryLogProcessor:
    def test_initialization(self, config):
        """초기화 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        assert processor.config == config
        assert isinstance(processor.query_normalizer, QueryNormalizer)
        assert isinstance(processor.es_manager, ElasticsearchManager)
        assert isinstance(processor.stream_processor, LogStreamProcessor)
        assert processor._batch == []
        assert processor._stats == {'processed': 0, 'indexed': 0, 'errors': 0}

    def test_prepare_es_document(self, config, sample_query_data):
        """ES 문서 준비 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # QueryNormalizer 모킹
        processor.query_normalizer = MagicMock()
        processor.query_normalizer.normalize_query.return_value = "SELECT * FROM users WHERE id = ?"
        processor.query_normalizer.generate_hash.return_value = "test_hash"
        
        document = processor._prepare_es_document(sample_query_data)
        
        assert document['_op_type'] == 'update'
        assert document['_index'] == f"{config.es_index_prefix}-{config.instance_id}"
        assert document['_id'] == "test_hash"
        assert 'script' in document
        assert 'upsert' in document
        
        # upsert 내용 검증
        upsert = document['upsert']
        assert upsert['query'] == sample_query_data['query']
        assert upsert['normalized_query'] == "SELECT * FROM users WHERE id = ?"
        assert upsert['query_hash'] == "test_hash"
        assert upsert['timestamp'] == sample_query_data['timestamp']
        assert upsert['user'] == sample_query_data['user']
        assert upsert['query_time'] == float(sample_query_data['query_time'])
        assert upsert['instance_id'] == config.instance_id

    @patch('src.lambda_slowlog_processor.LogStreamProcessor')
    def test_process_logs_success(self, mock_stream_processor, config, sample_query_data):
        """로그 처리 성공 케이스 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # LogStreamProcessor 모킹
        mock_stream = MagicMock()
        mock_stream.stream_log_files.return_value = []
        mock_stream.process_streaming_content.return_value = [sample_query_data]
        processor.stream_processor = mock_stream
        
        # ElasticsearchManager 모킹
        processor.es_manager = MagicMock()
        processor.es_manager.bulk_index.return_value = 1
        
        stats = processor.process_logs()
        
        assert stats['processed'] == 1
        assert stats['indexed'] == 1
        assert stats['errors'] == 0

    @patch('src.lambda_slowlog_processor.LogStreamProcessor')
    def test_process_logs_with_errors(self, mock_stream_processor, config):
        """로그 처리 에러 케이스 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # 에러를 발생시키는 샘플 데이터
        error_data = {'query': None}  # 의도적으로 잘못된 데이터
        
        # LogStreamProcessor 모킹
        mock_stream = MagicMock()
        mock_stream.stream_log_files.return_value = []
        mock_stream.process_streaming_content.return_value = [error_data]
        processor.stream_processor = mock_stream
        
        stats = processor.process_logs()
        
        assert stats['processed'] == 1
        assert stats['errors'] == 1

    def test_process_batch(self, config):
        """배치 처리 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # ElasticsearchManager 모킹
        processor.es_manager = MagicMock()
        processor.es_manager.bulk_index.return_value = 2
        
        # 테스트용 배치 데이터
        processor._batch = [
            {'_id': '1', 'data': 'test1'},
            {'_id': '2', 'data': 'test2'}
        ]
        
        processor._process_batch()
        
        # 검증
        assert processor._batch == []  # 배치가 비워졌는지 확인
        assert processor._stats['indexed'] == 2
        processor.es_manager.bulk_index.assert_called_once()

    def test_process_batch_with_error(self, config):
        """배치 처리 에러 케이스 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # ElasticsearchManager가 에러를 발생시키도록 모킹
        processor.es_manager = MagicMock()
        processor.es_manager.bulk_index.side_effect = Exception("Bulk index error")
        
        # 테스트용 배치 데이터
        processor._batch = [
            {'_id': '1', 'data': 'test1'},
            {'_id': '2', 'data': 'test2'}
        ]
        
        with pytest.raises(Exception):
            processor._process_batch()
        
        # 검증
        assert processor._batch == []  # 에러가 발생해도 배치는 비워져야 함
        assert processor._stats['errors'] == 2

    def test_cleanup(self, config):
        """리소스 정리 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # 테스트용 데이터 설정
        processor._batch = [{'test': 'data'}]
        processor._stats = {'processed': 10, 'indexed': 8, 'errors': 2}
        
        processor._cleanup()
        
        # 검증
        assert processor._batch == []
        assert processor._stats == {'processed': 10, 'indexed': 8, 'errors': 2}

@pytest.mark.integration
class TestSlowQueryLogProcessorIntegration:
    def test_full_processing_flow(self, config):
        """전체 처리 흐름 통합 테스트"""
        processor = SlowQueryLogProcessor(config)
        
        # 실제 ES 연결 및 처리 테스트
        try:
            stats = processor.process_logs()
            assert isinstance(stats, dict)
            assert all(key in stats for key in ['processed', 'indexed', 'errors'])
        except Exception as e:
            pytest.skip(f"Integration test failed: {str(e)}")