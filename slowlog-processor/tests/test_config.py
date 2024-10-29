import pytest
from src.lambda_slowlog_processor import Config

@pytest.mark.unit  # 설정 관련 단위 테스트
class TestConfig:
    def test_config_initialization(self):
        """기본 설정 초기화 테스트"""
        config = Config(instance_id='test-instance')
        assert config.instance_id == 'test-instance'
        assert config.region == 'ap-northeast-2'  # default value
        assert config.min_query_time == 0.1  # default value

    def test_config_from_event(self):
        """이벤트로부터 설정 생성 테스트"""
        event = {
            'instance_id': 'test-instance',
            'region': 'us-east-1',
            'min_query_time': '0.5'
        }
        config = Config.from_event(event)
        assert config.instance_id == 'test-instance'
        assert config.region == 'us-east-1'
        assert config.min_query_time == 0.5

    def test_config_validation(self):
        """설정 유효성 검증 테스트"""
        with pytest.raises(ValueError):
            config = Config(instance_id='')
            config.validate()

        with pytest.raises(ValueError):
            config = Config(instance_id='test', min_query_time=-1)
            config.validate()