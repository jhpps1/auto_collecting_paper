#!/usr/bin/env python3
"""
실시간 파이프라인 모니터링 대시보드

웹 기반 실시간 성능 모니터링 인터페이스
"""

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import json
from datetime import datetime, timedelta
import sys
import os

# 상위 디렉토리의 모듈 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from monitoring.performance_logger import PerformanceLogger

app = FastAPI(title="Pipeline Monitoring Dashboard")

# 템플릿 설정
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# 정적 파일 설정
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 성능 로거 인스턴스
logger = PerformanceLogger()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """메인 대시보드 페이지"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/metrics")
async def get_metrics(hours: int = 24):
    """최근 메트릭 데이터 API"""
    try:
        metrics = logger.get_recent_metrics(hours)
        return JSONResponse(content=metrics)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/bottlenecks")
async def get_bottlenecks(hours: int = 24):
    """병목 분석 API"""
    try:
        bottlenecks = logger.get_pipeline_bottlenecks(hours)
        return JSONResponse(content=bottlenecks)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/realtime-stats")
async def get_realtime_stats():
    """실시간 통계 API"""
    try:
        # 최근 1시간 데이터
        recent_metrics = logger.get_recent_metrics(1)

        # 최근 24시간 데이터
        daily_metrics = logger.get_recent_metrics(24)

        # 현재 처리 속도 계산
        current_throughput = {}
        if 'summary' in recent_metrics:
            for stage, data in recent_metrics['summary'].items():
                current_throughput[stage] = data.get('throughput_per_hour', 0)

        return JSONResponse(content={
            "current_throughput": current_throughput,
            "recent_metrics": recent_metrics,
            "daily_metrics": daily_metrics,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/stage-details/{stage}")
async def get_stage_details(stage: str, hours: int = 24):
    """특정 스테이지 상세 정보"""
    try:
        metrics = logger.get_recent_metrics(hours)
        stage_data = []

        if 'detailed' in metrics:
            stage_data = [m for m in metrics['detailed'] if m['stage'] == stage]

        return JSONResponse(content={
            "stage": stage,
            "operations": stage_data,
            "period_hours": hours
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    print("🖥️ 파이프라인 모니터링 대시보드 시작")
    print("   📊 대시보드: http://localhost:8001")
    print("   🔌 API: http://localhost:8001/api/metrics")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )