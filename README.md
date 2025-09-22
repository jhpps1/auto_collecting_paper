# 논문 수집 및 임베딩 파이프라인

OpenAlex API를 통한 논문 메타데이터 수집, PDF 다운로드, GROBID 텍스트 추출, 임베딩 생성까지의 완전한 파이프라인을 Docker Compose로 구성한 프로젝트입니다.

## 🚀 빠른 시작

### 필수 요구사항

- Docker
- Docker Compose
- 8GB 이상의 RAM (GROBID 및 임베딩 모델 실행용)

### 실행 방법

1. **저장소 클론**
   ```bash
   git clone <repository-url>
   cd paper-collection-pipeline
   ```

2. **Docker Compose로 전체 파이프라인 실행**
   ```bash
   docker compose up -d
   ```

   이 명령어로 다음 서비스들이 자동으로 시작됩니다:
   - PostgreSQL (pgvector 확장 포함)
   - GROBID 서비스
   - 논문 수집 파이프라인

3. **로그 확인**
   ```bash
   # 전체 로그 확인
   docker compose logs -f

   # 파이프라인만 확인
   docker compose logs -f pipeline
   ```

4. **중지**
   ```bash
   docker compose down
   ```

## 📊 시스템 구성

### 서비스 구성

| 서비스 | 포트 | 설명 |
|--------|------|------|
| `postgres` | 5432 | PostgreSQL + pgvector (논문 데이터 저장) |
| `grobid` | 8070 | GROBID 텍스트 추출 서비스 |
| `pipeline` | - | 논문 수집 파이프라인 (백그라운드 실행) |

### 데이터베이스 스키마

- **papers**: 논문 메타데이터, PDF URL, 전문 텍스트, 임베딩
- **authors**: 저자 정보
- **paper_authors**: 논문-저자 관계 (다대다)

## 🔄 파이프라인 단계

1. **OpenAlex API 수집**: 컴퓨터 과학 분야 논문 메타데이터 수집
2. **PDF 다운로드**: 오픈 액세스 논문의 PDF 파일 다운로드
3. **GROBID 처리**: PDF에서 구조화된 텍스트 추출
4. **임베딩 생성**: Sentence Transformers로 768차원 벡터 생성

## ⚙️ 환경 변수

Docker Compose에서 자동으로 설정되는 환경 변수들:

```yaml
# PostgreSQL 설정
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=papers_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123

# GROBID 설정
GROBID_URL=http://grobid:8070
```

## 📈 모니터링

### 데이터베이스 직접 접근

```bash
# PostgreSQL 컨테이너에 접속
docker compose exec postgres psql -U postgres -d papers_db

# 논문 수 확인
SELECT COUNT(*) FROM papers;

# 임베딩 생성된 논문 수 확인
SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL;
```

### 파이프라인 상태 확인

```bash
# 파이프라인 로그 실시간 확인
docker compose logs -f pipeline

# 현재 실행 중인 서비스 확인
docker compose ps
```

## 🛠️ 커스터마이징

### 검색 키워드 수정

`pipeline_runner.py` 파일의 `keywords` 리스트를 수정하여 수집할 논문 주제를 변경할 수 있습니다.

### 배치 크기 조정

`pipeline_runner.py`에서 `target_papers_per_batch` 값을 조정하여 한 번에 수집할 논문 수를 변경할 수 있습니다.

### 모델 변경

`embedding_generator.py`에서 사용하는 Sentence Transformers 모델을 변경할 수 있습니다.

## 🐛 문제 해결

### 메모리 부족 오류
- Docker Desktop의 메모리 할당을 8GB 이상으로 늘려주세요.

### GROBID 서비스 연결 실패
```bash
# GROBID 서비스 상태 확인
curl http://localhost:8070/api/isalive

# 서비스 재시작
docker compose restart grobid
```

### 데이터베이스 연결 오류
```bash
# PostgreSQL 상태 확인
docker compose exec postgres pg_isready -U postgres

# 데이터베이스 로그 확인
docker compose logs postgres
```

## 📁 데이터 영속성

- PostgreSQL 데이터: `postgres_data` Docker 볼륨에 저장
- PDF 파일: `./pdfs` 디렉토리에 저장
- GROBID 출력: `./grobid_output` 디렉토리에 저장

## 🔧 개발자 가이드

### 로컬 개발 환경 설정

```bash
# Python 가상환경 생성
python3 -m venv venv
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정 후 개별 실행
export POSTGRES_HOST=localhost
python3 pipeline_runner.py --single
```

### 단계별 실행

```bash
# 1. 메타데이터만 수집
python3 full_openalex_collector.py --query "machine learning" --count 10

# 2. PDF 처리만 실행
python3 pdf_grobid_processor.py

# 3. 임베딩만 생성
python3 embedding_generator.py
```

## 📝 라이센스

이 프로젝트는 MIT 라이센스 하에 배포됩니다.