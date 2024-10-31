"""메모리 사용량을 추적하고 제어하는 모듈

이 모듈은 프로세스의 메모리 사용량을 실시간으로 모니터링하고,
임계치 기반으로 처리를 제어하며 메모리 최적화 지표를 수집합니다.
"""

import os
import time
import logging
import psutil
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
from threading import Thread, Event, Lock
import weakref

logger = logging.getLogger(__name__)

@dataclass
class MemorySnapshot:
    """메모리 사용량 스냅샷을 저장하는 데이터 클래스
    
    Attributes:
        timestamp: 측정 시간
        rss_bytes: 실제 사용 메모리
        vms_bytes: 가상 메모리
        shared_bytes: 공유 메모리
        proc_count: 프로세스 수
        thread_count: 스레드 수
        percent: 메모리 사용률
    """
    timestamp: datetime
    rss_bytes: int
    vms_bytes: int
    shared_bytes: int
    proc_count: int
    thread_count: int
    percent: float

    @property
    def rss_mb(self) -> float:
        """RSS 메모리를 MB 단위로 반환"""
        return self.rss_bytes / (1024 * 1024)

    @property
    def vms_mb(self) -> float:
        """가상 메모리를 MB 단위로 반환"""
        return self.vms_bytes / (1024 * 1024)

class MemoryTracker:
    """메모리 사용량 추적 및 제어 클래스
    
    이 클래스는 프로세스의 메모리 사용량을 모니터링하고,
    임계치에 따른 처리 제어와 메모리 최적화를 제공합니다.
    
    Attributes:
        threshold_mb: 메모리 임계치 (MB)
        critical_threshold_mb: 크리티컬 임계치 (MB)
        monitoring_interval: 모니터링 주기 (초)
        history_size: 이력 저장 크기
        snapshots: 메모리 스냅샷 이력
        callbacks: 임계치 도달 시 콜백 함수들
        _stop_event: 모니터링 중지 이벤트
    """
    
    def __init__(
        self,
        threshold_mb: float = 512,
        critical_threshold_mb: float = 768,
        monitoring_interval: float = 1.0,
        history_size: int = 3600,
        auto_start: bool = True
    ):
        """초기화 메서드
        
        Args:
            threshold_mb: 경고 임계치 (MB)
            critical_threshold_mb: 크리티컬 임계치 (MB)
            monitoring_interval: 모니터링 주기 (초)
            history_size: 이력 저장 크기
            auto_start: 자동 시작 여부
        """
        self.threshold_mb = threshold_mb
        self.critical_threshold_mb = critical_threshold_mb
        self.monitoring_interval = monitoring_interval
        
        self.snapshots: deque = deque(maxlen=history_size)
        self.callbacks: Dict[str, Callable] = {}
        
        self._stop_event = Event()
        self._snapshot_lock = Lock()
        self._callback_lock = Lock()
        self._monitor_thread: Optional[Thread] = None
        
        # 프로세스 정보 초기화
        self.process = psutil.Process(os.getpid())
        self._initial_memory = self._get_current_memory()
        
        if auto_start:
            self.start_monitoring()

    def start_monitoring(self) -> None:
        """메모리 모니터링을 시작합니다."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("모니터링이 이미 실행 중입니다")
            return
            
        self._stop_event.clear()
        self._monitor_thread = Thread(
            target=self._monitoring_loop,
            name="MemoryMonitor",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("메모리 모니터링이 시작되었습니다")

    def stop_monitoring(self) -> None:
        """메모리 모니터링을 중지합니다."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        logger.info("메모리 모니터링이 중지되었습니다")

    def _monitoring_loop(self) -> None:
        """메모리 모니터링 루프"""
        while not self._stop_event.is_set():
            try:
                snapshot = self._take_snapshot()
                self._check_thresholds(snapshot)
                time.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"모니터링 중 오류 발생: {str(e)}")
                time.sleep(1)  # 오류 발생 시 잠시 대기

    def _take_snapshot(self) -> MemorySnapshot:
        """현재 메모리 상태의 스냅샷을 생성합니다."""
        memory_info = self.process.memory_info()
        with self._snapshot_lock:
            snapshot = MemorySnapshot(
                timestamp=datetime.now(),
                rss_bytes=memory_info.rss,
                vms_bytes=memory_info.vms,
                shared_bytes=getattr(memory_info, 'shared', 0),
                proc_count=len(self.process.children()),
                thread_count=self.process.num_threads(),
                percent=self.process.memory_percent()
            )
            self.snapshots.append(snapshot)
            return snapshot

    def _check_thresholds(self, snapshot: MemorySnapshot) -> None:
        """메모리 임계치를 확인하고 필요한 조치를 수행합니다."""
        if snapshot.rss_mb >= self.critical_threshold_mb:
            self._handle_critical_threshold(snapshot)
        elif snapshot.rss_mb >= self.threshold_mb:
            self._handle_warning_threshold(snapshot)

    def _handle_warning_threshold(self, snapshot: MemorySnapshot) -> None:
        """경고 임계치 도달 시 처리"""
        logger.warning(
            f"메모리 사용량 경고 수준: {snapshot.rss_mb:.1f}MB "
            f"(임계치: {self.threshold_mb}MB)"
        )
        self._execute_callbacks('warning', snapshot)

    def _handle_critical_threshold(self, snapshot: MemorySnapshot) -> None:
        """크리티컬 임계치 도달 시 처리"""
        logger.error(
            f"메모리 사용량 위험 수준: {snapshot.rss_mb:.1f}MB "
            f"(임계치: {self.critical_threshold_mb}MB)"
        )
        self._execute_callbacks('critical', snapshot)

    def register_callback(
        self,
        name: str,
        callback: Callable[[MemorySnapshot], None],
        threshold_type: str = 'warning'
    ) -> None:
        """임계치 도달 시 실행할 콜백 함수를 등록합니다.
        
        Args:
            name: 콜백 이름
            callback: 콜백 함수
            threshold_type: 임계치 타입 ('warning' 또는 'critical')
        """
        with self._callback_lock:
            self.callbacks[f"{threshold_type}_{name}"] = weakref.proxy(callback)
        logger.debug(f"콜백 등록됨: {name} ({threshold_type})")

    def unregister_callback(self, name: str, threshold_type: str = 'warning') -> None:
        """등록된 콜백을 제거합니다."""
        with self._callback_lock:
            self.callbacks.pop(f"{threshold_type}_{name}", None)

    def _execute_callbacks(self, threshold_type: str, snapshot: MemorySnapshot) -> None:
        """해당 임계치의 모든 콜백을 실행합니다."""
        with self._callback_lock:
            for name, callback in list(self.callbacks.items()):
                if name.startswith(threshold_type):
                    try:
                        callback(snapshot)
                    except Exception as e:
                        logger.error(f"콜백 실행 중 오류 ({name}): {str(e)}")

    def get_current_memory(self) -> Dict[str, float]:
        """현재 메모리 사용량을 반환합니다."""
        return self._get_current_memory()

    def _get_current_memory(self) -> Dict[str, float]:
        """현재 메모리 사용량을 측정합니다."""
        memory_info = self.process.memory_info()
        return {
            'rss_mb': memory_info.rss / (1024 * 1024),
            'vms_mb': memory_info.vms / (1024 * 1024),
            'percent': self.process.memory_percent()
        }

    def get_memory_growth(self) -> Dict[str, float]:
        """초기 상태 대비 메모리 증가량을 반환합니다."""
        current = self._get_current_memory()
        return {
            'rss_mb_growth': current['rss_mb'] - self._initial_memory['rss_mb'],
            'vms_mb_growth': current['vms_mb'] - self._initial_memory['vms_mb'],
            'percent_growth': current['percent'] - self._initial_memory['percent']
        }

    def get_memory_trend(self, minutes: int = 5) -> Dict[str, Any]:
        """지정된 기간의 메모리 사용 추세를 분석합니다.
        
        Args:
            minutes: 분석할 기간 (분)
            
        Returns:
            Dict[str, Any]: 메모리 사용 추세 정보
        """
        with self._snapshot_lock:
            if not self.snapshots:
                return {}
                
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            relevant_snapshots = [
                s for s in self.snapshots
                if s.timestamp >= cutoff_time
            ]
            
            if not relevant_snapshots:
                return {}
                
            first, last = relevant_snapshots[0], relevant_snapshots[-1]
            time_diff = (last.timestamp - first.timestamp).total_seconds() / 60
            
            if time_diff <= 0:
                return {}
                
            return {
                'duration_minutes': time_diff,
                'memory_growth_mb': last.rss_mb - first.rss_mb,
                'growth_rate_mb_per_minute': (last.rss_mb - first.rss_mb) / time_diff,
                'average_memory_mb': sum(s.rss_mb for s in relevant_snapshots) / len(relevant_snapshots),
                'peak_memory_mb': max(s.rss_mb for s in relevant_snapshots),
                'current_memory_mb': last.rss_mb
            }

    def get_summary(self) -> Dict[str, Any]:
        """메모리 사용량 요약을 반환합니다."""
        current = self._get_current_memory()
        growth = self.get_memory_growth()
        trend = self.get_memory_trend()
        
        return {
            'current_usage': current,
            'memory_growth': growth,
            'trend': trend,
            'thresholds': {
                'warning_mb': self.threshold_mb,
                'critical_mb': self.critical_threshold_mb
            },
            'status': 'critical' if current['rss_mb'] >= self.critical_threshold_mb
                      else 'warning' if current['rss_mb'] >= self.threshold_mb
                      else 'normal'
        }

    def __enter__(self) -> 'MemoryTracker':
        """컨텍스트 매니저 진입"""
        self.start_monitoring()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """컨텍스트 매니저 종료"""
        self.stop_monitoring()