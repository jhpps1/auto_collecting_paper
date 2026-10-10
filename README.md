# 논문 수집·처리·임베딩

OpenAlex에서 논문 메타데이터를 모아 PostgreSQL `paperdb`에 넣고, 제목과 초록으로 384차원 임베딩을 저장하는 배치 작업이다. 조회 API는 별도 저장소 [RSP](https://github.com/jhpps1/RSP)다. 두 프로세스는 서로를 호출하지 않고 같은 `papers` 테이블만 공유한다.

## 무엇을 실행하나

| 작업 | 파일 | 요청 경로에 있나 |
| --- | --- | --- |
| OpenAlex 수집 | `full_openalex_collector.py` | 아니오. 배치 |
| 임베딩 | `embedding_generator.py` | 아니오. 배치 |
| PDF 본문 추출 | `pdf_grobid_processor.py` + GROBID | 아니오. 배치 |
| 유사도 행렬 | `pyspark_similarity.py`, `simple_similarity.py` | 아니오. Postgres 임베딩을 읽어 HBase에 쓴다 |
| 검색 색인 | `reindex_papers_to_opensearch.py` | 아니오. Postgres를 읽어 OpenSearch에 넣는다 |

이 compose는 GROBID와 수집 명령만 올린다. PostgreSQL, HBase, OpenSearch, PySpark 클러스터는 올리지 않는다. 조회는 RSP의 `POST /search`가 `papers.embedding`을 읽는다.

`full_openalex_collector.py` 안의 concept 주석은 분야를 나눠 돌릴 때의 실행 메모다. 기본으로 주석이 풀려 있는 값은 Computer Science(`C41008148`)다.

## 공유 데이터베이스

RSP compose가 만드는 DB와 같다.

- DB `paperdb`, 사용자 `admin`
- 테이블 `papers`
- 임베딩 모델 `all-MiniLM-L6-v2`, `VECTOR(384)`
- PDF 링크는 OpenAlex `open_access.oa_url`만 저장한다

예전 수집기 기본값은 DB `papers_db`, 임베딩 `all-mpnet-base-v2`(768차원)였다. 그 벡터는 조회 서버의 384차원 컬럼에 들어가지 않는다.

저자·개념·저널용 관계 테이블이 없으면 그 쓰기는 건너뛰고 `papers` 행은 남긴다.

## 실행

RSP를 먼저 띄운다. 그 compose는 호스트 5433에 Postgres를 연다. LAGO가 5432를 쓰는 경우를 피한 값이다.

```bash
cp .env.example .env
```

메타데이터만 소수 수집한 뒤 임베딩한다.

```bash
python full_openalex_collector.py --query "machine learning" --count 10
python embedding_generator.py
```

Docker로 수집만 할 때:

```bash
docker compose up --build
```

`pipeline` 서비스의 기본 명령은 `full_openalex_collector.py --count 10`이고, 끝나면 컨테이너가 멈춘다. Postgres는 이 파일 안에 없다. 기본 호스트는 `host.docker.internal`, 포트는 `5433`이다. 같은 Docker 네트워크의 `postgres` 서비스에 붙이면 `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432`로 덮어쓴다.

`infinite_pipeline.py`는 수집과 임베딩을 반복하고, 논문이 10건 이상이면 `simple_similarity.py`로 HBase REST(`localhost:8080`)에 유사도를 쓰려 한다. 이 compose는 HBase를 시작하지 않으므로 그 스크립트는 기본 명령이 아니다.

GROBID만 확인할 때:

```bash
docker compose up grobid
curl http://localhost:8070/api/isalive
```

PDF가 있는 행에 본문을 붙일 때 `python pdf_grobid_processor.py`. 결과는 `papers.full_text`, `papers.grobid_data`에 들어간다. 조회 검색은 이 컬럼을 쓰지 않고 `embedding`을 쓴다.

## 환경 변수

`.env.example`을 본다.

```
POSTGRES_HOST POSTGRES_PORT POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
EMBEDDING_MODEL=all-MiniLM-L6-v2
GROBID_URL=http://grobid:8070
```

## 배치 작업이 기대하는 것

- `pyspark_similarity.py`: PySpark로 Postgres 임베딩의 코사인 유사도를 계산하고 HBase REST에 저장
- `simple_similarity.py`: 같은 일을 numpy로 계산
- `reindex_papers_to_opensearch.py`: Postgres 행을 OpenSearch `papers` 인덱스에 넣음. 기본 주소 `http://localhost:9200`

처리량 수치는 적혀 있지 않다. 클러스터가 이 compose에 없기 때문이다.
