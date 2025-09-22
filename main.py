#!/usr/bin/env python3
"""
논문 추천 시스템 FastAPI 메인 애플리케이션

이 시스템은 다음 기술 스택을 사용합니다:
- PostgreSQL + pgvector: 논문 메타데이터 및 임베딩
- HBase: 논문 유사도 관계 저장
- OpenSearch: 실시간 논문 검색
- Redis: 캐싱 및 세션 관리
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
from loguru import logger

# 로컬 모듈 imports (향후 구현 예정)
# from api.routes import papers, recommendations, search, users
# from core.config import settings
# from core.database import get_db_connections
# from services.recommendation_engine import RecommendationEngine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행할 작업들"""

    # 시작 시 초기화 작업
    logger.info("🚀 논문 추천 시스템 시작 중...")

    # OpenSearch 및 임베딩 클라이언트 초기화
    from core.opensearch_client import init_opensearch, close_opensearch
    from core.embedding_client import init_embedding, close_embedding

    try:
        # OpenSearch 초기화
        opensearch_success = await init_opensearch()
        if opensearch_success:
            logger.info("✅ OpenSearch 초기화 완료")
        else:
            logger.warning("⚠️ OpenSearch 초기화 실패 (서비스는 계속 실행)")

        # 임베딩 모델 초기화
        embedding_success = await init_embedding()
        if embedding_success:
            logger.info("✅ 임베딩 모델 초기화 완료")
        else:
            logger.warning("⚠️ 임베딩 모델 초기화 실패 (서비스는 계속 실행)")

        logger.info("✅ 하이브리드 검색 시스템 초기화 완료")

    except Exception as e:
        logger.error(f"❌ 초기화 중 오류 발생: {e}")
        logger.info("서비스는 제한된 기능으로 실행됩니다")

    yield  # 애플리케이션 실행

    # 종료 시 정리 작업
    logger.info("🛑 시스템 종료 중...")
    try:
        await close_opensearch()
        await close_embedding()
        logger.info("✅ 시스템 정리 완료")
    except Exception as e:
        logger.error(f"❌ 종료 중 오류 발생: {e}")


# FastAPI 앱 생성
app = FastAPI(
    title="논문 추천 시스템 API",
    description="""
    AI 기반 논문 추천 시스템
    
    ## 주요 기능
    
    * **논문 검색**: OpenSearch 기반 전문 검색
    * **개인화 추천**: 사용자 기반 논문 추천 
    * **유사도 분석**: 벡터 기반 논문 유사도 계산
    * **사용자 관리**: 사용자 선호도 및 이력 관리
    
    ## 기술 스택
    
    * **PostgreSQL + pgvector**: 논문 메타데이터 저장
    * **HBase**: 대용량 유사도 관계 저장
    * **OpenSearch**: 실시간 검색 엔진
    * **Redis**: 고성능 캐싱
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS 설정 - 프로덕션 환경 고려
import os

# 환경변수로 허용할 오리진 설정 (기본값: 개발환경)
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000,http://localhost:3001,http://localhost:8080,http://127.0.0.1:3000,http://j13d203.p.ssafy.io"
).split(",")

# DEBUG 모드일 때만 모든 오리진 허용
if os.getenv("DEBUG_MODE", "false").lower() == "true":
    ALLOWED_ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


# 헬스체크 엔드포인트
@app.get("/")
async def root():
    """시스템 상태 확인"""
    return {
        "message": "논문 추천 시스템 API",
        "version": "1.0.0",
        "status": "running",
        "docs_url": "/docs"
    }


@app.get("/health")
async def health_check():
    """상세 헬스체크 - 모든 서비스 상태 확인"""
    from datetime import datetime, timezone
    import psycopg2
    import redis
    import happybase
    import httpx
    
    services_status = {
        "api": "healthy",
        "postgresql": "checking...",
        "hbase": "checking...", 
        "opensearch": "checking...",
        "redis": "checking..."
    }
    
    # PostgreSQL 연결 확인
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="papers_db",
            user="postgres",
            password="postgres123"
        )
        conn.close()
        services_status["postgresql"] = "healthy"
    except Exception as e:
        services_status["postgresql"] = f"unhealthy: {str(e)[:50]}"
    
    # Redis 연결 확인
    try:
        r = redis.Redis(host='localhost', port=6379, password='redis', decode_responses=True)
        r.ping()
        services_status["redis"] = "healthy"
    except Exception as e:
        services_status["redis"] = f"unhealthy: {str(e)[:50]}"
    
    # HBase 연결 확인
    try:
        connection = happybase.Connection('localhost', port=9090)
        connection.open()
        connection.tables()
        connection.close()
        services_status["hbase"] = "healthy"
    except Exception as e:
        services_status["hbase"] = f"unhealthy: {str(e)[:50]}"
    
    # OpenSearch 연결 확인
    try:
        opensearch_host = os.getenv("OPENSEARCH_HOST", "localhost")
        opensearch_port = os.getenv("OPENSEARCH_PORT", "9200")
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{opensearch_host}:{opensearch_port}/_cluster/health", timeout=5.0)
            if response.status_code == 200:
                services_status["opensearch"] = "healthy"
            else:
                services_status["opensearch"] = f"unhealthy: status {response.status_code}"
    except Exception as e:
        services_status["opensearch"] = f"unhealthy: {str(e)[:50]}"
    
    # 모든 서비스가 정상인지 확인
    all_healthy = all(status == "healthy" for status in services_status.values())
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "services": services_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# API 라우터 등록
from api.routes import search, papers, pdf_analysis

app.include_router(search.router, prefix="/api/v1/search", tags=["search"])
app.include_router(papers.router, prefix="/api/v1/papers", tags=["papers"])
app.include_router(pdf_analysis.router, prefix="/api/v1/pdf-analysis", tags=["pdf-analysis"])

# 향후 구현 예정
# app.include_router(recommendations.router, prefix="/api/v1/recommendations", tags=["recommendations"])
# app.include_router(users.router, prefix="/api/v1/users", tags=["users"])


if __name__ == "__main__":
    # 개발 서버 실행
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )