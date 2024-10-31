"""애플리케이션 설정을 관리하는 모듈

이 모듈은 Lambda 함수의 설정을 관리하며, 환경 변수와 이벤트 파라미터로부터
설정을 로드하고 검증하는 기능을 제공합니다.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
import os
import logging

logger = logging.getLogger(__name__)

# 기본 설정값 정의
DEFAULT_CONFIG = {
    "region": "ap-northeast-2",
    "min_query_time": 0.1,
    "es_index_prefix": "mysql-slowlog",
    "es_connection_timeout": 30,
    "es_retry_count": 3,
    "batch_size": 1000,
    "max_buffer_size_mb": 10,
    "chunk_size": 1000,
    "es_timeout": 60,
    "es_max_retries": 5
}

@dataclass
class Config:
    """어플리케이션 설정 클래스
    
    Attributes:
        instance_id (str): RDS 인스턴스 식별자
        region (str): AWS 리전
        min_query_time (float): 처리할 최소 쿼리 실행 시간 (초)
        es_host (str): Elasticsearch 호스트 주소
        es_index_prefix (str): Elasticsearch 인덱스 접두사
        es_connection_timeout (int): Elasticsearch 연결 타임아웃 (초)
        es_retry_count (int): Elasticsearch 재시도 횟수
        batch_size (int): 배치 처리 크기
        max_buffer_size_mb (int): 최대 버퍼 크기 (MB)
        chunk_size (int): 청크 처리 크기
    """
    
    instance_id: str
    region: str = DEFAULT_CONFIG["region"]
    min_query_time: float = DEFAULT_CONFIG["min_query_time"]
    es_host: str = ""
    es_index_prefix: str = DEFAULT_CONFIG["es_index_prefix"]
    es_connection_timeout: int = DEFAULT_CONFIG["es_connection_timeout"]
    es_retry_count: int = DEFAULT_CONFIG["es_retry_count"]
    batch_size: int = DEFAULT_CONFIG["batch_size"]
    max_buffer_size_mb: int = DEFAULT_CONFIG["max_buffer_size_mb"]
    chunk_size: int = DEFAULT_CONFIG["chunk_size"]
    es_timeout: int = DEFAULT_CONFIG["es_timeout"]
    es_max_retries: int = DEFAULT_CONFIG["es_max_retries"]

    @classmethod
    def from_event(cls, event: Dict[str, Any]) -> 'Config':
        """Lambda 이벤트로부터 설정 객체를 생성합니다."""
        try:
            config = cls(
                instance_id=cls._get_config_value('instance_id', event),
                region=cls._get_config_value('region', event, DEFAULT_CONFIG["region"]),
                min_query_time=float(cls._get_config_value('min_query_time', event, DEFAULT_CONFIG["min_query_time"])),
                es_host=cls._get_config_value('es_host', event, ""),
                es_index_prefix=cls._get_config_value('es_index_prefix', event, DEFAULT_CONFIG["es_index_prefix"]),
                es_connection_timeout=int(cls._get_config_value('es_connection_timeout', event, DEFAULT_CONFIG["es_connection_timeout"])),
                es_retry_count=int(cls._get_config_value('es_retry_count', event, DEFAULT_CONFIG["es_retry_count"])),
                batch_size=int(cls._get_config_value('batch_size', event, DEFAULT_CONFIG["batch_size"])),
                max_buffer_size_mb=int(cls._get_config_value('max_buffer_size_mb', event, DEFAULT_CONFIG["max_buffer_size_mb"])),
                chunk_size=int(cls._get_config_value('chunk_size', event, DEFAULT_CONFIG["chunk_size"])),
                es_timeout=int(cls._get_config_value('es_timeout', event, DEFAULT_CONFIG["es_timeout"])),  # 추가된 속성
                es_max_retries=int(cls._get_config_value('es_max_retries', event, DEFAULT_CONFIG["es_max_retries"]))  # 추가된 속성
            )
            return config
        except Exception as e:
            logger.error(f"설정 생성 중 오류 발생: {str(e)}")
            raise ValueError(f"Invalid configuration: {str(e)}")

    @staticmethod
    def _get_config_value(key: str, event: Dict[str, Any], default: Any = None) -> Any:
        """설정값을 이벤트 또는 환경 변수에서 가져옵니다.
        
        Args:
            key: 설정 키
            event: Lambda 이벤트 데이터
            default: 기본값
            
        Returns:
            Any: 설정값
        """
        # 이벤트에서 값 확인
        value = event.get(key)
        if value is not None:
            return value
            
        # 환경 변수에서 값 확인
        env_key = key.upper()
        value = os.environ.get(env_key)
        if value is not None:
            return value
            
        # 기본값 반환
        if default is not None:
            return default
            
        # 필수 값이 없는 경우
        raise ValueError(f"Required configuration '{key}' is missing")

    def validate(self) -> None:
        """설정값의 유효성을 검증합니다.
        
        Raises:
            ValueError: 유효하지 않은 설정값이 있는 경우
            
        Example:
            >>> config = Config(instance_id="test")
            >>> config.validate()  # OK
            >>> config = Config(instance_id="", min_query_time=-1)
            >>> config.validate()  # Raises ValueError
        """
        validations = [
            (bool(self.instance_id), "instance_id is required"),
            (self.min_query_time > 0, "min_query_time must be positive"),
            (self.batch_size > 0, "batch_size must be positive"),
            (self.max_buffer_size_mb > 0, "max_buffer_size_mb must be positive"),
            (self.es_connection_timeout > 0, "es_connection_timeout must be positive"),
            (self.es_retry_count >= 0, "es_retry_count must be non-negative"),
            (self.chunk_size > 0, "chunk_size must be positive")
        ]
        
        for condition, message in validations:
            if not condition:
                logger.error(f"설정 검증 실패: {message}")
                raise ValueError(message)

    def get_es_index_name(self) -> str:
        """Elasticsearch 인덱스 이름을 생성합니다.
        
        Returns:
            str: 인덱스 이름
        """
        return f"{self.es_index_prefix}-{self.instance_id}"

    def __str__(self) -> str:
        """설정 객체의 문자열 표현을 반환합니다."""
        return (
            f"Config(instance_id={self.instance_id}, "
            f"region={self.region}, "
            f"min_query_time={self.min_query_time}, "
            f"es_host={'[REDACTED]' if self.es_host else 'None'}, "
            f"es_index_prefix={self.es_index_prefix})"
        )