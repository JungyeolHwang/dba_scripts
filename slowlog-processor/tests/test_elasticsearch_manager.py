from unittest.mock import MagicMock
import pytest
from src.lambda_slowlog_processor import Config, ElasticsearchManager

@pytest.mark.unit
class TestElasticsearchManager:
    def test_bulk_index(self):
        """벌크 인덱싱 단위 테스트 (mock 사용)"""
        config = Config(instance_id='test', es_host='http://localhost:9200')
        manager = ElasticsearchManager(config)
        
        # ES 클라이언트 모킹
        manager.es_client = MagicMock()
        manager.es_client.bulk = MagicMock(return_value={'errors': False})
        
        documents = [{
            '_op_type': 'update',
            '_index': 'test-index',
            '_id': 'test-id',
            'script': {
                'source': 'ctx._source.count += 1',
                'lang': 'painless'
            },
            'upsert': {'count': 1}
        }]
        
        result = manager.bulk_index(documents, 'test-index')
        assert result == 1

@pytest.mark.integration
class TestElasticsearchManagerIntegration:
    @pytest.fixture(scope="class")
    def es_config(self):
        """ES 테스트 설정"""
        return Config(
            instance_id='test',
            es_host='http://localhost:9200'
        )
    
    def test_real_connection(self, es_config):
        """실제 ES 서버 연결 테스트"""
        manager = ElasticsearchManager(es_config)
        assert manager.es_client is not None
        assert manager.es_client.ping()
    
    def test_real_index_creation(self, es_config):
        """실제 ES 인덱스 생성 및 문서 색인 테스트"""
        manager = ElasticsearchManager(es_config)
        test_index = f"test-index-{pytest.timestamp}"
        
        documents = [{
            '_op_type': 'index',
            '_index': test_index,
            '_id': '1',
            '_source': {'test': 'data'}
        }]
        
        result = manager.bulk_index(documents, test_index)
        assert result > 0
        
        # 인덱스 존재 확인
        assert manager.es_client.indices.exists(index=test_index)