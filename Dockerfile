FROM python:3.9-slim

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# requirements.txt 복사 및 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# PDF 다운로드 디렉토리 생성
RUN mkdir -p /app/pdfs /app/grobid_output

# 실행 권한 설정
RUN chmod +x *.py

# 기본 환경변수 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 기본 명령어 (무한 수집 모드)
CMD ["python3", "infinite_pipeline.py"]