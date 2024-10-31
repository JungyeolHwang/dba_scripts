"""처리 진행상황을 추적하는 모듈

이 모듈은 대용량 로그 처리의 진행상황을 추적하고,
처리 시간을 예측하며 실패한 청크의 재처리를 관리합니다.
"""

import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger(__name__)

@dataclass
class ChunkProgress:
    """청크 처리 진행상황을 관리하는 데이터 클래스
    
    Attributes:
        chunk_id: 청크 식별자
        size_bytes: 청크 크기
        start_time: 처리 시작 시간
        end_time: 처리 완료 시간
        status: 처리 상태
        attempt_count: 처리 시도 횟수
        error: 에러 메시지
    """
    chunk_id: str
    size_bytes: int
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, processing, completed, failed
    attempt_count: int = 0
    error: Optional[str] = None

    def get_processing_time(self) -> Optional[float]:
        """처리 소요 시간을 반환합니다."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

class ProgressTracker:
    """처리 진행상황 추적 클래스
    
    이 클래스는 청크 단위의 처리 진행상황을 추적하고,
    예상 완료 시간을 계산하며 실패한 청크를 관리합니다.
    
    Attributes:
        total_size_bytes: 전체 처리할 데이터 크기
        chunk_progress: 청크별 진행상황
        processing_history: 최근 처리 이력
        start_time: 전체 처리 시작 시간
        history_window: 처리 속도 계산을 위한 이력 윈도우 크기
    """
    
    def __init__(
        self,
        total_size_bytes: int,
        history_window: int = 100
    ):
        """초기화 메서드
        
        Args:
            total_size_bytes: 전체 처리할 데이터 크기
            history_window: 처리 속도 계산을 위한 이력 윈도우 크기
        """
        self.total_size_bytes = total_size_bytes
        self.chunk_progress: Dict[str, ChunkProgress] = {}
        self.processing_history: deque = deque(maxlen=history_window)
        self.start_time = datetime.now()
        self.history_window = history_window

    def register_chunk(self, chunk_id: str, size_bytes: int) -> None:
        """새로운 청크를 등록합니다.
        
        Args:
            chunk_id: 청크 식별자
            size_bytes: 청크 크기
        """
        if chunk_id not in self.chunk_progress:
            self.chunk_progress[chunk_id] = ChunkProgress(
                chunk_id=chunk_id,
                size_bytes=size_bytes
            )

    def start_chunk_processing(self, chunk_id: str) -> None:
        """청크 처리 시작을 기록합니다."""
        if chunk_id in self.chunk_progress:
            progress = self.chunk_progress[chunk_id]
            progress.start_time = datetime.now()
            progress.status = "processing"
            progress.attempt_count += 1

    def complete_chunk_processing(self, chunk_id: str) -> None:
        """청크 처리 완료를 기록합니다."""
        if chunk_id in self.chunk_progress:
            progress = self.chunk_progress[chunk_id]
            progress.end_time = datetime.now()
            progress.status = "completed"
            
            # 처리 이력 업데이트
            processing_time = progress.get_processing_time()
            if processing_time:
                self.processing_history.append({
                    'chunk_id': chunk_id,
                    'size_bytes': progress.size_bytes,
                    'processing_time': processing_time
                })

    def fail_chunk_processing(self, chunk_id: str, error: str) -> None:
        """청크 처리 실패를 기록합니다.
        
        Args:
            chunk_id: 청크 식별자
            error: 에러 메시지
        """
        if chunk_id in self.chunk_progress:
            progress = self.chunk_progress[chunk_id]
            progress.end_time = datetime.now()
            progress.status = "failed"
            progress.error = error

    def get_failed_chunks(self, max_attempts: int = 3) -> List[str]:
        """재시도 가능한 실패한 청크 목록을 반환합니다.
        
        Args:
            max_attempts: 최대 시도 횟수
            
        Returns:
            List[str]: 재시도 가능한 청크 ID 목록
        """
        return [
            chunk_id for chunk_id, progress in self.chunk_progress.items()
            if progress.status == "failed" and progress.attempt_count < max_attempts
        ]

    def get_processing_rate(self) -> float:
        """현재 처리 속도를 계산합니다 (bytes/sec).
        
        Returns:
            float: 처리 속도
        """
        if not self.processing_history:
            return 0.0
            
        recent_history = list(self.processing_history)
        total_bytes = sum(h['size_bytes'] for h in recent_history)
        total_time = sum(h['processing_time'] for h in recent_history)
        
        return total_bytes / total_time if total_time > 0 else 0.0

    def estimate_completion_time(self) -> Optional[datetime]:
        """예상 완료 시간을 계산합니다.
        
        Returns:
            Optional[datetime]: 예상 완료 시간
        """
        processing_rate = self.get_processing_rate()
        if processing_rate <= 0:
            return None
            
        # 처리된 바이트 계산
        processed_bytes = sum(
            progress.size_bytes
            for progress in self.chunk_progress.values()
            if progress.status == "completed"
        )
        
        remaining_bytes = self.total_size_bytes - processed_bytes
        if remaining_bytes <= 0:
            return datetime.now()
            
        remaining_seconds = remaining_bytes / processing_rate
        return datetime.now() + timedelta(seconds=remaining_seconds)

    def get_progress_summary(self) -> Dict[str, Any]:
        """현재 진행상황 요약을 반환합니다.
        
        Returns:
            Dict[str, Any]: 진행상황 요약
        """
        total_chunks = len(self.chunk_progress)
        completed_chunks = sum(1 for p in self.chunk_progress.values() if p.status == "completed")
        failed_chunks = sum(1 for p in self.chunk_progress.values() if p.status == "failed")
        processing_chunks = sum(1 for p in self.chunk_progress.values() if p.status == "processing")
        
        processed_bytes = sum(
            p.size_bytes for p in self.chunk_progress.values()
            if p.status == "completed"
        )
        
        progress_percentage = (processed_bytes / self.total_size_bytes * 100) if self.total_size_bytes > 0 else 0
        
        return {
            'total_chunks': total_chunks,
            'completed_chunks': completed_chunks,
            'failed_chunks': failed_chunks,
            'processing_chunks': processing_chunks,
            'processed_bytes': processed_bytes,
            'total_bytes': self.total_size_bytes,
            'progress_percentage': round(progress_percentage, 2),
            'processing_rate_bps': round(self.get_processing_rate(), 2),
            'estimated_completion': self.estimate_completion_time(),
            'elapsed_time': (datetime.now() - self.start_time).total_seconds(),
            'failed_chunk_details': [
                {
                    'chunk_id': chunk_id,
                    'attempts': progress.attempt_count,
                    'error': progress.error
                }
                for chunk_id, progress in self.chunk_progress.items()
                if progress.status == "failed"
            ]
        }

    def log_progress(self) -> None:
        """현재 진행상황을 로깅합니다."""
        summary = self.get_progress_summary()
        estimated_completion = summary['estimated_completion']
        
        logger.info(
            f"Processing Progress: "
            f"{summary['progress_percentage']}% complete - "
            f"{summary['completed_chunks']}/{summary['total_chunks']} chunks, "
            f"Rate: {summary['processing_rate_bps']:.1f} bytes/sec, "
            f"ETA: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S') if estimated_completion else 'Unknown'}"
        )
        
        if summary['failed_chunks'] > 0:
            logger.warning(
                f"Failed chunks: {summary['failed_chunks']}, "
                f"Latest errors: " + 
                ", ".join(f"{d['chunk_id']}: {d['error']}" 
                         for d in summary['failed_chunk_details'][:3])
            )