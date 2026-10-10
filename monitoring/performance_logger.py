#!/usr/bin/env python3
"""
파이프라인 성능 모니터링 로거

각 작업의 처리 시간, 성공/실패율, 처리량을 실시간으로 기록
"""

import time
import json
import psycopg2
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, Any, Optional
import threading
import os

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

class PerformanceLogger:
    def __init__(self):
        self.setup_metrics_table()
        self.lock = threading.Lock()

    def setup_metrics_table(self):
        """성능 메트릭 테이블 생성"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS performance_metrics (
                            id SERIAL PRIMARY KEY,
                            timestamp TIMESTAMP DEFAULT NOW(),
                            stage VARCHAR(50) NOT NULL,
                            operation VARCHAR(100) NOT NULL,
                            duration_seconds FLOAT NOT NULL,
                            success BOOLEAN NOT NULL,
                            paper_id INTEGER,
                            error_message TEXT,
                            metadata JSONB,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    """)

                    # 인덱스 생성
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_performance_timestamp
                        ON performance_metrics(timestamp)
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_performance_stage
                        ON performance_metrics(stage)
                    """)

                    conn.commit()
                    print("✅ 성능 메트릭 테이블 설정 완료")

        except Exception as e:
            print(f"❌ 메트릭 테이블 설정 실패: {e}")

    def log_performance(self, stage: str, operation: str, duration: float,
                       success: bool, paper_id: Optional[int] = None,
                       error_message: Optional[str] = None,
                       metadata: Optional[Dict] = None):
        """성능 메트릭 로깅"""
        try:
            with self.lock:
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO performance_metrics
                            (stage, operation, duration_seconds, success, paper_id, error_message, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (stage, operation, duration, success, paper_id, error_message,
                              json.dumps(metadata) if metadata else None))
                        conn.commit()

        except Exception as e:
            print(f"❌ 성능 로깅 실패: {e}")

    @contextmanager
    def measure_operation(self, stage: str, operation: str,
                         paper_id: Optional[int] = None,
                         metadata: Optional[Dict] = None):
        """작업 시간 측정 컨텍스트 매니저"""
        start_time = time.time()
        success = False
        error_message = None

        try:
            yield
            success = True
        except Exception as e:
            error_message = str(e)
            raise
        finally:
            duration = time.time() - start_time
            self.log_performance(stage, operation, duration, success,
                               paper_id, error_message, metadata)

    def get_recent_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """최근 메트릭 조회"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    # 스테이지별 평균 처리 시간
                    cursor.execute("""
                        SELECT
                            stage,
                            operation,
                            COUNT(*) as total_operations,
                            AVG(duration_seconds) as avg_duration,
                            SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                            COUNT(*) - SUM(CASE WHEN success THEN 1 ELSE 0 END) as failure_count,
                            MAX(duration_seconds) as max_duration,
                            MIN(duration_seconds) as min_duration
                        FROM performance_metrics
                        WHERE timestamp >= NOW() - INTERVAL '%s hours'
                        GROUP BY stage, operation
                        ORDER BY stage, operation
                    """, (hours,))

                    metrics = cursor.fetchall()

                    result = {
                        'summary': {},
                        'detailed': []
                    }

                    stage_totals = {}

                    for metric in metrics:
                        stage, operation, total, avg_duration, success, failure, max_dur, min_dur = metric

                        success_rate = (success / total * 100) if total > 0 else 0
                        throughput = 3600 / avg_duration if avg_duration > 0 else 0

                        detailed_metric = {
                            'stage': stage,
                            'operation': operation,
                            'total_operations': total,
                            'avg_duration': round(avg_duration, 3),
                            'success_rate': round(success_rate, 1),
                            'throughput_per_hour': round(throughput, 0),
                            'success_count': success,
                            'failure_count': failure,
                            'max_duration': round(max_dur, 3),
                            'min_duration': round(min_dur, 3)
                        }

                        result['detailed'].append(detailed_metric)

                        # 스테이지별 집계
                        if stage not in stage_totals:
                            stage_totals[stage] = {
                                'total_operations': 0,
                                'total_duration': 0,
                                'success_count': 0,
                                'failure_count': 0
                            }

                        stage_totals[stage]['total_operations'] += total
                        stage_totals[stage]['total_duration'] += avg_duration * total
                        stage_totals[stage]['success_count'] += success
                        stage_totals[stage]['failure_count'] += failure

                    # 스테이지별 요약 계산
                    for stage, totals in stage_totals.items():
                        if totals['total_operations'] > 0:
                            avg_duration = totals['total_duration'] / totals['total_operations']
                            success_rate = totals['success_count'] / totals['total_operations'] * 100
                            throughput = 3600 / avg_duration if avg_duration > 0 else 0

                            result['summary'][stage] = {
                                'avg_duration': round(avg_duration, 3),
                                'success_rate': round(success_rate, 1),
                                'throughput_per_hour': round(throughput, 0),
                                'total_operations': totals['total_operations']
                            }

                    return result

        except Exception as e:
            print(f"❌ 메트릭 조회 실패: {e}")
            return {}

    def get_pipeline_bottlenecks(self, hours: int = 24) -> Dict[str, Any]:
        """파이프라인 병목 분석"""
        try:
            metrics = self.get_recent_metrics(hours)
            summary = metrics.get('summary', {})

            bottlenecks = []
            stages = ['openalex', 'pdf_grobid', 'embedding', 'similarity']

            max_duration = 0
            slowest_stage = None

            for stage in stages:
                if stage in summary:
                    duration = summary[stage]['avg_duration']
                    if duration > max_duration:
                        max_duration = duration
                        slowest_stage = stage

                    bottlenecks.append({
                        'stage': stage,
                        'avg_duration': duration,
                        'throughput': summary[stage]['throughput_per_hour'],
                        'success_rate': summary[stage]['success_rate'],
                        'is_bottleneck': False
                    })

            # 병목 표시
            for bottleneck in bottlenecks:
                if bottleneck['stage'] == slowest_stage:
                    bottleneck['is_bottleneck'] = True

            return {
                'bottlenecks': bottlenecks,
                'slowest_stage': slowest_stage,
                'max_duration': max_duration,
                'analysis_period_hours': hours
            }

        except Exception as e:
            print(f"❌ 병목 분석 실패: {e}")
            return {}

# 전역 로거 인스턴스
_performance_logger = None

def get_performance_logger():
    """성능 로거 싱글톤 인스턴스 반환"""
    global _performance_logger
    if _performance_logger is None:
        _performance_logger = PerformanceLogger()
    return _performance_logger

# 편의 함수들
def log_openalex_operation(operation: str, duration: float, success: bool,
                          paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """OpenAlex 작업 로깅"""
    logger = get_performance_logger()
    logger.log_performance('openalex', operation, duration, success, paper_id, None, metadata)

def log_pdf_grobid_operation(operation: str, duration: float, success: bool,
                            paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """PDF/GROBID 작업 로깅"""
    logger = get_performance_logger()
    logger.log_performance('pdf_grobid', operation, duration, success, paper_id, None, metadata)

def log_embedding_operation(operation: str, duration: float, success: bool,
                           paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """임베딩 작업 로깅"""
    logger = get_performance_logger()
    logger.log_performance('embedding', operation, duration, success, paper_id, None, metadata)

def log_similarity_operation(operation: str, duration: float, success: bool,
                            paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """유사도 작업 로깅"""
    logger = get_performance_logger()
    logger.log_performance('similarity', operation, duration, success, paper_id, None, metadata)

@contextmanager
def measure_openalex(operation: str, paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """OpenAlex 작업 측정"""
    logger = get_performance_logger()
    with logger.measure_operation('openalex', operation, paper_id, metadata):
        yield

@contextmanager
def measure_pdf_grobid(operation: str, paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """PDF/GROBID 작업 측정"""
    logger = get_performance_logger()
    with logger.measure_operation('pdf_grobid', operation, paper_id, metadata):
        yield

@contextmanager
def measure_embedding(operation: str, paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """임베딩 작업 측정"""
    logger = get_performance_logger()
    with logger.measure_operation('embedding', operation, paper_id, metadata):
        yield

@contextmanager
def measure_similarity(operation: str, paper_id: Optional[int] = None, metadata: Optional[Dict] = None):
    """유사도 작업 측정"""
    logger = get_performance_logger()
    with logger.measure_operation('similarity', operation, paper_id, metadata):
        yield

if __name__ == "__main__":
    # 테스트
    logger = PerformanceLogger()

    # 샘플 데이터 로깅
    logger.log_performance('openalex', 'metadata_collection', 1.5, True, 1001, None, {'query': 'test'})
    logger.log_performance('embedding', 'generate_embedding', 2.3, True, 1001, None, {'model': 'all-MiniLM-L6-v2'})

    # 메트릭 조회
    metrics = logger.get_recent_metrics(24)
    print("📊 성능 메트릭:")
    print(json.dumps(metrics, indent=2, ensure_ascii=False))

    # 병목 분석
    bottlenecks = logger.get_pipeline_bottlenecks(24)
    print("\n🚨 병목 분석:")
    print(json.dumps(bottlenecks, indent=2, ensure_ascii=False))