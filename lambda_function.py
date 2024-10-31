"""AWS Lambda 핸들러 모듈

이 모듈은 AWS Lambda의 진입점으로서, 
슬로우 쿼리 로그 처리의 전체 프로세스를 조율합니다.
"""

import json
import time
import logging
from typing import Dict, Any
from datetime import datetime

from src.config.config import Config
from src.processors.slow_query import SlowQueryLogProcessor
from src.utils.logging import setup_logging
from src.utils.decorators import monitor_memory, log_with_context

# 로깅 설정
setup_logging(
    level=logging.INFO,
    json_format=True
)

logger = logging.getLogger(__name__)

class LambdaResponse:
    """Lambda 응답을 생성하는 유틸리티 클래스"""
    
    @staticmethod
    def success(body: Dict[str, Any]) -> Dict[str, Any]:
        """성공 응답을 생성합니다.
        
        Args:
            body: 응답 본문
            
        Returns:
            Dict[str, Any]: Lambda 응답 형식
        """
        return {
            'statusCode': 200,
            'body': json.dumps(body),
            'headers': {
                'Content-Type': 'application/json',
                'X-Processed-At': datetime.now().isoformat()
            }
        }
    
    @staticmethod
    def error(status_code: int, message: str, error_details: Dict[str, Any] = None) -> Dict[str, Any]:
        """에러 응답을 생성합니다.
        
        Args:
            status_code: HTTP 상태 코드
            message: 에러 메시지
            error_details: 추가 에러 정보
            
        Returns:
            Dict[str, Any]: Lambda 응답 형식
        """
        body = {
            'error': message,
            'timestamp': datetime.now().isoformat()
        }
        
        if error_details:
            body['details'] = error_details
            
        return {
            'statusCode': status_code,
            'body': json.dumps(body),
            'headers': {
                'Content-Type': 'application/json'
            }
        }

def validate_event(event: Dict[str, Any]) -> None:
    """이벤트 데이터를 검증합니다.
    
    Args:
        event: Lambda 이벤트 데이터
        
    Raises:
        ValueError: 필수 파라미터가 없거나 유효하지 않은 경우
    """
    required_params = []  # 필수 파라미터 정의
    missing_params = [param for param in required_params if param not in event]
    
    if missing_params:
        raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
    # 추가적인 검증 로직이 필요한 경우 여기에 구현

@monitor_memory
@log_with_context(service="slowlog-processor")
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 핸들러 함수
    
    Args:
        event: Lambda 이벤트 데이터
        context: Lambda 컨텍스트
        
    Returns:
        Dict[str, Any]: Lambda 응답
        
    Example 이벤트:
        {
            "instance_id": "database-1",
            "region": "ap-northeast-2",
            "min_query_time": "0.5",
            "es_host": "https://es-endpoint.region.es.amazonaws.com",
            "es_index_prefix": "mysql-slowlog"
        }
    """
    execution_start = time.time()
    logger.info("Lambda execution started", extra={"event": event})
    
    try:
        # 이벤트 검증
        validate_event(event)
        
        # 설정 초기화
        config = Config.from_event(event)
        config.validate()
        logger.info("Configuration initialized", extra={"config": str(config)})
        
        # 프로세서 초기화 및 실행
        processor = SlowQueryLogProcessor(config)
        results = processor.process_logs()
        
        # 실행 통계 수집
        execution_time = time.time() - execution_start
        execution_stats = {
            'start_time': datetime.fromtimestamp(execution_start).isoformat(),
            'duration_seconds': execution_time,
            'memory_used_mb': context.memory_limit_in_mb if context else None,
            'function_version': context.function_version if context else None
        }
        
        # 응답 생성
        response_body = {
            'message': 'Successfully processed slow query logs',
            'stats': results,
            'execution_stats': execution_stats
        }
        
        logger.info(
            "Lambda execution completed successfully",
            extra={'execution_stats': execution_stats}
        )
        
        return LambdaResponse.success(response_body)
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        return LambdaResponse.error(
            400,
            "Invalid configuration",
            {'error_type': 'ValidationError', 'details': str(ve)}
        )
        
    except Exception as e:
        logger.error(
            f"Lambda execution failed: {str(e)}",
            exc_info=True,
            extra={'event': event}
        )
        return LambdaResponse.error(
            500,
            "Internal server error",
            {
                'error_type': type(e).__name__,
                'details': str(e)
            }
        )

def local_handler(event: Dict[str, Any]) -> None:
    """로컬 테스트를 위한 핸들러
    
    Args:
        event: 테스트 이벤트 데이터
        
    Example:
        >>> test_event = {
        ...     "instance_id": "test-db",
        ...     "region": "ap-northeast-2"
        ... }
        >>> local_handler(test_event)
    """
    class MockContext:
        """Lambda 컨텍스트를 모방하는 클래스"""
        memory_limit_in_mb = 128
        function_version = '$LATEST'
        
    try:
        response = lambda_handler(event, MockContext())
        print(json.dumps(response, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    # 로컬 테스트 이벤트
    test_event = {
        "instance_id": "test-instance",
        "region": "ap-northeast-2",
        "min_query_time": "0.1",
        "es_host": "http://localhost:9200"
    }
    
    local_handler(test_event)