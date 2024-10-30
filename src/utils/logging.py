"""로깅 설정과 유틸리티를 제공하는 모듈

이 모듈은 애플리케이션 전반의 로깅 설정과
커스텀 로깅 기능을 제공합니다.
"""

import functools
import logging
import logging.handlers
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path

class JsonFormatter(logging.Formatter):
    """JSON 형식의 로그 포매터
    
    로그 메시지를 구조화된 JSON 형식으로 포매팅합니다.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 JSON 형식으로 포매팅합니다."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'function': record.funcName,
            'line': record.lineno
        }
        
        if hasattr(record, 'extras'):
            log_data.update(record.extras)
            
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
            
        return json.dumps(log_data)

def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    json_format: bool = False,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> None:
    """애플리케이션 로깅을 설정합니다.
    
    Args:
        level: 로깅 레벨
        log_file: 로그 파일 경로 (선택)
        json_format: JSON 형식 사용 여부
        max_bytes: 로그 파일 최대 크기
        backup_count: 보관할 백업 파일 수
        
    Example:
        >>> setup_logging(
        ...     level=logging.DEBUG,
        ...     log_file="app.log",
        ...     json_format=True
        ... )
    """
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    # 포매터 설정
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 파일 핸들러 추가 (지정된 경우)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

class LogContext:
    """컨텍스트 매니저를 통한 임시 로깅 설정
    
    Example:
        >>> with LogContext(level=logging.DEBUG):
        ...     # 디버그 레벨로 임시 로깅
        ...     logger.debug("Detailed info")
    """
    
    def __init__(self, level: int):
        self.level = level
        self.previous_level = None
        self.root_logger = logging.getLogger()
        
    def __enter__(self) -> None:
        self.previous_level = self.root_logger.level
        self.root_logger.setLevel(self.level)
        
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.previous_level is not None:
            self.root_logger.setLevel(self.previous_level)

def get_logger(name: str) -> logging.Logger:
    """모듈별 로거를 생성합니다.
    
    Args:
        name: 로거 이름
        
    Returns:
        logging.Logger: 설정된 로거
    """
    return logging.getLogger(name)

def log_with_context(**context: Any) -> callable:
    """컨텍스트 정보를 포함하여 로깅하는 데코레이터
    
    Example:
        >>> @log_with_context(module="data_processor", version="1.0")
        ... def process_data():
        ...     logger.info("Processing data")  # 컨텍스트 정보 포함
    """
    def decorator(func: callable) -> callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = logging.getLogger(func.__module__)
            extra = {'extras': context}
            
            with logging.LoggerAdapter(logger, extra):
                return func(*args, **kwargs)
        return wrapper
    return decorator