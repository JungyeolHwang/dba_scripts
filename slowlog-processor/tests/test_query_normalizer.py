import pytest
from src.lambda_slowlog_processor import QueryNormalizer

@pytest.mark.unit  # 쿼리 정규화 단위 테스트
class TestQueryNormalizer:
    def test_basic_normalization(self):
        """기본적인 쿼리 정규화 테스트"""
        normalizer = QueryNormalizer()
        
        # 숫자 정규화
        query = "SELECT * FROM users WHERE id = 123"
        assert normalizer.normalize_query(query) == "SELECT * FROM users WHERE id = ?"

        # 문자열 정규화
        query = "SELECT * FROM users WHERE name = 'John'"
        assert normalizer.normalize_query(query) == "SELECT * FROM users WHERE name = ?"

    def test_complex_queries(self):
        """복잡한 쿼리 정규화 테스트"""
        normalizer = QueryNormalizer()
        
        # IN 절
        query = "SELECT * FROM users WHERE id IN (1,2,3,4)"
        assert normalizer.normalize_query(query) == "SELECT * FROM users WHERE id IN (...)"
        
        # 여러 조건
        query = """SELECT u.*, p.name 
                  FROM users u 
                  JOIN profiles p ON u.id = p.user_id 
                  WHERE u.age > 20 AND p.city = 'Seoul'"""
        normalized = normalizer.normalize_query(query)
        assert "age > ?" in normalized
        assert "city = ?" in normalized

    def test_query_hash_consistency(self):
        """쿼리 해시 일관성 테스트"""
        normalizer = QueryNormalizer()
        
        # 동일한 의미의 다른 형태 쿼리
        query1 = "SELECT * FROM users WHERE id = 123"
        query2 = "SELECT *   FROM   users   WHERE   id   =   123"
        
        hash1 = normalizer.generate_hash(query1)
        hash2 = normalizer.generate_hash(query2)
        assert hash1 == hash2