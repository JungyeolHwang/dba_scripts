"""대용량 로그 처리를 위한 세부 설정 모듈

이 모듈은 대용량 로그 처리에 필요한 세부 설정들을 관리합니다.
기본 Config 클래스와 통합하여 사용됩니다.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import logging
import json

from src.config.config import Config

logger = logging.getLogger(__name__)

@dataclass
class ProcessingConfig:
    """처리 관련 세부 설정 클래스
    
    Attributes:
        chunk_size: 청크 크기 (바이트)
        max_workers: 최대 작업자 스레드 수
        memory_threshold_mb: 메모리 사용량 임계치 (MB)
        batch_size: ES 벌크 인덱싱 배치 크기
        max_batch_size: 최대 배치 크기
        min_batch_size: 최소 배치 크기
        batch_timeout: 배치 타임아웃 (초)
        retry_limit: 실패한 청크 재시도 횟수
        buffer_size_mb: 로그 처리 버퍼 크기 (MB)
    """
    
    # 청크 관련 설정
    chunk_size: int = 5 * 1024 * 1024  # 5MB
    max_workers: int = 4
    memory_threshold_mb: int = 512
    
    # 배치 관련 설정
    batch_size: int = 1000
    max_batch_size: int = 5000
    min_batch_size: int = 100
    batch_timeout: float = 30.0
    
    # 재시도 및 버퍼 설정
    retry_limit: int = 3
    buffer_size_mb: int = 10
    
    # 동적 조정 설정
    enable_auto_scaling: bool = True
    scaling_factor: float = 1.2
    scaling_cooldown: float = 60.0
    
    # 모니터링 설정
    monitoring_interval: float = 5.0
    metrics_enabled: bool = True
    
    # 기본 Config 참조
    base_config: Optional[Config] = None
    
    def __post_init__(self):
        """설정값 검증 및 로깅"""
        self.validate()
        self._log_config()
    
    def validate(self) -> None:
        """설정값을 검증합니다.
        
        Raises:
            ValueError: 잘못된 설정값이 있는 경우
        """
        validations = [
            (self.chunk_size > 0, "chunk_size must be positive"),
            (self.max_workers > 0, "max_workers must be positive"),
            (self.memory_threshold_mb > 100, "memory_threshold_mb must be at least 100MB"),
            (self.batch_size > 0, "batch_size must be positive"),
            (self.min_batch_size <= self.batch_size <= self.max_batch_size,
             "batch_size must be between min and max batch size"),
            (self.batch_timeout > 0, "batch_timeout must be positive"),
            (self.retry_limit >= 0, "retry_limit cannot be negative"),
            (self.buffer_size_mb > 0, "buffer_size_mb must be positive"),
            (self.scaling_factor > 1.0, "scaling_factor must be greater than 1.0"),
            (self.scaling_cooldown > 0, "scaling_cooldown must be positive"),
            (self.monitoring_interval > 0, "monitoring_interval must be positive")
        ]
        
        for condition, message in validations:
            if not condition:
                raise ValueError(f"Invalid configuration: {message}")
    
    def _log_config(self) -> None:
        """현재 설정을 로깅합니다."""
        logger.info("Processing configuration initialized:")
        for key, value in self.to_dict().items():
            logger.info(f"  {key}: {value}")
    
    def to_dict(self) -> Dict[str, Any]:
        """설정을 딕셔너리로 변환합니다."""
        return {
            'chunk_settings': {
                'chunk_size_mb': self.chunk_size / (1024 * 1024),
                'max_workers': self.max_workers,
                'memory_threshold_mb': self.memory_threshold_mb
            },
            'batch_settings': {
                'batch_size': self.batch_size,
                'max_batch_size': self.max_batch_size,
                'min_batch_size': self.min_batch_size,
                'batch_timeout': self.batch_timeout
            },
            'retry_settings': {
                'retry_limit': self.retry_limit,
                'buffer_size_mb': self.buffer_size_mb
            },
            'scaling_settings': {
                'enable_auto_scaling': self.enable_auto_scaling,
                'scaling_factor': self.scaling_factor,
                'scaling_cooldown': self.scaling_cooldown
            },
            'monitoring_settings': {
                'monitoring_interval': self.monitoring_interval,
                'metrics_enabled': self.metrics_enabled
            }
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], base_config: Optional[Config] = None) -> 'ProcessingConfig':
        """딕셔너리로부터 설정을 생성합니다.
        
        Args:
            config_dict: 설정 딕셔너리
            base_config: 기본 Config 인스턴스
            
        Returns:
            ProcessingConfig: 생성된 설정 객체
        """
        # 중첩된 딕셔너리를 플랫하게 변환
        flat_config = {}
        for section in config_dict.values():
            flat_config.update(section)
            
        # MB 단위로 제공된 값을 바이트로 변환
        if 'chunk_size_mb' in flat_config:
            flat_config['chunk_size'] = int(flat_config.pop('chunk_size_mb') * 1024 * 1024)
            
        return cls(**flat_config, base_config=base_config)
    
    def adjust_for_memory(self, current_memory_mb: float) -> None:
        """현재 메모리 사용량에 따라 설정을 조정합니다.
        
        Args:
            current_memory_mb: 현재 메모리 사용량 (MB)
        """
        if not self.enable_auto_scaling:
            return
            
        memory_usage_ratio = current_memory_mb / self.memory_threshold_mb
        
        if memory_usage_ratio > 0.8:  # 메모리 사용량이 임계치의 80% 초과
            # 배치 크기와 작업자 수 감소
            self.batch_size = max(self.min_batch_size, 
                                int(self.batch_size / self.scaling_factor))
            self.max_workers = max(1, self.max_workers - 1)
            logger.warning(
                f"High memory usage ({current_memory_mb:.1f}MB). "
                f"Adjusted batch_size={self.batch_size}, "
                f"max_workers={self.max_workers}"
            )
        elif memory_usage_ratio < 0.5:  # 메모리 사용량이 임계치의 50% 미만
            # 배치 크기와 작업자 수 증가
            self.batch_size = min(self.max_batch_size, 
                                int(self.batch_size * self.scaling_factor))
            self.max_workers = min(8, self.max_workers + 1)
            logger.info(
                f"Low memory usage ({current_memory_mb:.1f}MB). "
                f"Adjusted batch_size={self.batch_size}, "
                f"max_workers={self.max_workers}"
            )
    
    def to_json(self) -> str:
        """설정을 JSON 문자열로 변환합니다."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str, base_config: Optional[Config] = None) -> 'ProcessingConfig':
        """JSON 문자열로부터 설정을 생성합니다.
        
        Args:
            json_str: JSON 설정 문자열
            base_config: 기본 Config 인스턴스
            
        Returns:
            ProcessingConfig: 생성된 설정 객체
        """
        config_dict = json.loads(json_str)
        return cls.from_dict(config_dict, base_config)

    @classmethod
    def create_default(cls, base_config: Config) -> 'ProcessingConfig':
        """기본 설정으로 인스턴스를 생성합니다.
        
        Args:
            base_config: 기본 Config 인스턴스
            
        Returns:
            ProcessingConfig: 생성된 설정 객체
        """
        return cls(base_config=base_config)