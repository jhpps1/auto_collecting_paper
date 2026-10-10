#!/usr/bin/env python3
"""
간단한 모니터링 대시보드 (빠른 응답)
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import os
import psycopg2
import json
from datetime import datetime

app = FastAPI(title="Simple Pipeline Monitor")

# DB 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

@app.get("/", response_class=HTMLResponse)
async def simple_dashboard():
    """간단한 대시보드"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>파이프라인 모니터링</title>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial; padding: 20px; background: #f5f5f5; }
            .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .metric { display: inline-block; margin: 10px; padding: 10px; background: #007bff; color: white; border-radius: 5px; }
            .status { font-size: 18px; margin: 10px 0; }
            .running { color: #28a745; }
            .warning { color: #ffc107; }
            .error { color: #dc3545; }
        </style>
        <script>
            async function loadStats() {
                try {
                    const response = await fetch('/api/quick-stats');
                    const data = await response.json();

                    document.getElementById('papers').textContent = data.total_papers;
                    document.getElementById('embeddings').textContent = data.papers_with_embeddings;
                    document.getElementById('pdf').textContent = data.papers_with_pdf;
                    document.getElementById('grobid').textContent = data.grobid_completed;
                    document.getElementById('coverage').textContent = data.embedding_coverage.toFixed(1) + '%';
                    document.getElementById('updated').textContent = new Date().toLocaleString();

                    // 상태 표시
                    const status = data.embedding_coverage > 90 ? 'running' :
                                  data.embedding_coverage > 50 ? 'warning' : 'error';
                    document.getElementById('status').className = 'status ' + status;
                    document.getElementById('status').textContent =
                        status === 'running' ? '✅ 정상 동작' :
                        status === 'warning' ? '⚠️ 처리 중' : '❌ 처리 필요';

                } catch (error) {
                    document.getElementById('status').className = 'status error';
                    document.getElementById('status').textContent = '❌ 연결 오류';
                }
            }

            setInterval(loadStats, 5000); // 5초마다 업데이트
            loadStats(); // 즉시 로드
        </script>
    </head>
    <body>
        <h1>🚀 논문 처리 파이프라인 모니터링</h1>

        <div class="card">
            <div id="status" class="status">로딩 중...</div>
        </div>

        <div class="card">
            <h3>📊 현재 상태</h3>
            <div class="metric">총 논문: <span id="papers">-</span>개</div>
            <div class="metric">PDF 보유: <span id="pdf">-</span>개</div>
            <div class="metric">GROBID 완료: <span id="grobid">-</span>개</div>
            <div class="metric">임베딩 생성: <span id="embeddings">-</span>개</div>
            <div class="metric">처리율: <span id="coverage">-</span></div>
        </div>

        <div class="card">
            <small>마지막 업데이트: <span id="updated">-</span></small>
        </div>

        <div class="card">
            <h3>🔗 링크</h3>
            <a href="/api/quick-stats" target="_blank">📊 통계 API</a> |
            <a href="http://172.18.169.203:8000/docs" target="_blank">🔍 검색 API</a> |
            <a href="http://172.18.169.203:16010" target="_blank">🗃️ HBase UI</a>
        </div>
    </body>
    </html>
    """

@app.get("/api/quick-stats")
async def quick_stats():
    """빠른 통계 조회"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # 빠른 카운트 쿼리들
                stats = {}

                cursor.execute("SELECT COUNT(*) FROM papers")
                stats['total_papers'] = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL")
                stats['papers_with_embeddings'] = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM papers WHERE pdf_url IS NOT NULL")
                stats['papers_with_pdf'] = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM papers WHERE grobid_status = 'completed'")
                stats['grobid_completed'] = cursor.fetchone()[0]

                stats['embedding_coverage'] = (
                    stats['papers_with_embeddings'] / stats['total_papers'] * 100
                ) if stats['total_papers'] > 0 else 0

                stats['timestamp'] = datetime.now().isoformat()

                return JSONResponse(content=stats)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/health")
async def health():
    """헬스 체크"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    print("🖥️ 간단한 모니터링 대시보드 시작")
    print("   📊 접속: http://172.18.169.203:8002")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        reload=False
    )