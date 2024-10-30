"""성능 모니터링과 유틸리티 데코레이터를 제공하는 모듈

이 모듈은 함수/메서드의 실행을 모니터링하고 메트릭을 수집하는
데코레이터들을 제공합니다.
"""

import time
import logging
import functools
import tracemalloc
from typing import Any, Callable, TypeVar, cast
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

# 제네릭 타입 정의
F = TypeVar('F', bound=Callable[..., Any])

@dataclass
class ExecutionMetrics:
    """함수 실행 메트릭을 저장하는 데이터 클래스
    
    Attributes:
        function_name (str): 함수 이름
        start_time (datetime): 실행 시작 시간
        execution_time (float): 실행 소요 시간(초)
        memory_start (int): 시작 시 메모리 사용량
        memory_peak (int): 최대 메모리 사용량
        success (bool): 성공 여부
        error (str): 에러 메시지 (실패 시)
    """
    function_name: str
    start_time: datetime
    execution_time: float
    memory_start: int
    memory_peak: int
    success: bool
    error: str = ""

def monitor_memory(func: F) -> F:
    """함수의 메모리 사용량과 실행 시간을 모니터링하는 데코레이터
    
    Args:
        func: 모니터링할 함수
        
    Returns:
        F: 래핑된 함수
        
    Example:
        >>> @monitor_memory
        ... def process_data(data: List[str]) -> None:
        ...     # 처리 로직
        ...     pass
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # 메모리 추적 시작
        tracemalloc.start()
        start_time = time.time()
        start_memory = tracemalloc.get_traced_memory()[0]
        
        metrics = ExecutionMetrics(
            function_name=func.__name__,
            start_time=datetime.now(),
            execution_time=0.0,
            memory_start=start_memory,
            memory_peak=0,
            success=False
        )
        
        try:
            # 함수 실행
            result = func(*args, **kwargs)
            metrics.success = True
            return result
            
        except Exception as e:
            metrics.error = str(e)
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
            
        finally:
            # 메트릭 수집
            metrics.execution_time = time.time() - start_time
            _, metrics.memory_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            # 메트릭 로깅
            logger.info(
                "Function Execution Metrics: "
                f"name={metrics.function_name}, "
                f"duration={metrics.execution_time:.2f}s, "
                f"memory_increase={(metrics.memory_peak - metrics.memory_start) / 1024 / 1024:.2f}MB, "
                f"success={metrics.success}"
            )
    
    return cast(F, wrapper)

def retry(max_attempts: int = 3, delay: float = 1.0):
    """실패 시 재시도하는 데코레이터
    
    Args:
        max_attempts: 최대 시도 횟수
        delay: 재시도 간 대기 시간(초)
        
    Returns:
        Callable: 데코레이터 함수
        
    Example:
        >>> @retry(max_attempts=3, delay=1.0)
        ... def unstable_operation() -> None:
        ...     # 불안정한 작업
        ...     pass
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    if attempt > 0:
                        logger.info(f"Retry attempt {attempt + 1}/{max_attempts} for {func.__name__}")
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {str(e)}"
                            f"Waiting {delay} seconds before retry..."
                        )
                        time.sleep(delay)
                    
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception
            
        return cast(F, wrapper)
    return decorator

def log_execution_time(func: F) -> F:
    """함수의 실행 시간을 로깅하는 데코레이터
    
    Args:
        func: 로깅할 함수
        
    Returns:
        F: 래핑된 함수
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            execution_time = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {execution_time:.2f} seconds")
            
    return cast(F, wrapper)