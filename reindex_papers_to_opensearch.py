#!/usr/bin/env python3
"""
PostgreSQL에서 OpenSearch로 논문 데이터 재색인 스크립트

PostgreSQL의 모든 논문 데이터(임베딩 포함)를 OpenSearch에 색인합니다.
OpenSearch document ID = PostgreSQL papers.id 로 설정하여 ID 매칭 문제를 해결합니다.
"""

import psycopg2
import psycopg2.extras
import requests
import json
from typing import List, Dict, Any
import time
from datetime import datetime

# 설정
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'papers_db',
    'user': 'postgres',
    'password': 'postgres123'
}

OPENSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "papers"

class PaperReindexer:
    def __init__(self):
        self.postgres_conn = None
        self.total_papers = 0
        self.indexed_papers = 0
        self.failed_papers = 0

    def connect_postgres(self):
        """PostgreSQL 연결"""
        try:
            self.postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
            print("✅ PostgreSQL 연결 성공")
            return True
        except Exception as e:
            print(f"❌ PostgreSQL 연결 실패: {e}")
            return False

    def fetch_papers_batch(self, offset: int, batch_size: int = 50) -> List[Dict[str, Any]]:
        """PostgreSQL에서 논문 데이터를 배치로 가져오기"""
        cursor = self.postgres_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        sql = """
        SELECT
            p.id,
            p.openalex_paper_id,
            p.title,
            p.abstract_text,
            p.doi,
            p.citation_count,
            p.pdf_url,
            p.is_open_access,
            p.publication_date,
            p.keywords,
            p.publisher,
            p.type,
            p.reliability_score,
            p.total_score,
            p.embedding,

            -- 저널 정보
            j.name as journal_name,
            j.impact_factor,
            j.h_index as journal_h_index,
            j.jif_quartile,
            j.issn_l,
            j.is_oa as journal_is_oa,

            -- 저자 정보 (서브쿼리로 중복 제거)
            COALESCE(authors_data.authors, '[]'::json) as authors,

            -- 개념 정보 (서브쿼리로 중복 제거)
            COALESCE(concepts_data.concepts, '[]'::json) as concepts

        FROM papers p
        LEFT JOIN journals j ON p.journal_id = j.id

        -- 저자 정보 서브쿼리
        LEFT JOIN (
            SELECT
                pa.paper_id,
                json_agg(
                    json_build_object(
                        'name', a.name,
                        'orcid', a.orcid,
                        'affiliation', a.affiliation,
                        'h_index', a.h_index,
                        'citation_count', a.citation_count
                    ) ORDER BY pa.author_order
                ) as authors
            FROM paper_authors pa
            LEFT JOIN authors a ON pa.author_id = a.id
            WHERE a.id IS NOT NULL
            GROUP BY pa.paper_id
        ) authors_data ON p.id = authors_data.paper_id

        -- 개념 정보 서브쿼리
        LEFT JOIN (
            SELECT
                pc.paper_id,
                json_agg(
                    json_build_object(
                        'name', c.name,
                        'relevance_score', pc.relevance_score,
                        'level', c.level
                    ) ORDER BY pc.relevance_score DESC
                ) as concepts
            FROM paper_concepts pc
            LEFT JOIN concepts c ON pc.concept_id = c.id
            WHERE c.id IS NOT NULL
            GROUP BY pc.paper_id
        ) concepts_data ON p.id = concepts_data.paper_id

        WHERE p.embedding IS NOT NULL  -- 임베딩이 있는 논문만
        ORDER BY p.id
        LIMIT %s OFFSET %s
        """

        cursor.execute(sql, (batch_size, offset))
        return cursor.fetchall()

    def convert_paper_to_opensearch_doc(self, paper: Dict[str, Any]) -> Dict[str, Any]:
        """PostgreSQL 논문 데이터를 OpenSearch 문서로 변환"""

        # 임베딩 변환 (pgvector array -> Python list)
        embedding_list = None
        if paper['embedding']:
            # pgvector는 문자열로 저장되므로 파싱 필요
            embedding_str = str(paper['embedding'])
            if embedding_str.startswith('[') and embedding_str.endswith(']'):
                embedding_list = [float(x) for x in embedding_str[1:-1].split(',')]

        # OpenSearch 문서 구조
        doc = {
            "title": paper['title'],
            "abstract_text": paper['abstract_text'],
            "doi": paper['doi'],
            "citation_count": paper['citation_count'] or 0,
            "pdf_url": paper['pdf_url'],
            "is_open_access": paper['is_open_access'],
            "publication_date": paper['publication_date'].isoformat() if paper['publication_date'] else None,
            "keywords": paper['keywords'] or [],
            "publisher": paper['publisher'],
            "type": paper['type'],
            "reliability_score": float(paper['reliability_score']) if paper['reliability_score'] else None,
            "total_score": float(paper['total_score']) if paper['total_score'] else None,

            "metadata": {
                "openalex_paper_id": paper['openalex_paper_id']
            },

            "journal_info": {
                "name": paper['journal_name'],
                "impact_factor": float(paper['impact_factor']) if paper['impact_factor'] else None,
                "h_index": paper['journal_h_index'],
                "jif_quartile": paper['jif_quartile'],
                "issn_l": paper['issn_l'],
                "is_oa": paper['journal_is_oa']
            },

            "author_details": paper['authors'] if paper['authors'] else [],
            "concept_details": paper['concepts'] if paper['concepts'] else [],

            "embeddings": {
                "full_text_embedding": embedding_list
            } if embedding_list else {}
        }

        return doc

    def index_paper_to_opensearch(self, paper_id: int, doc: Dict[str, Any]) -> bool:
        """OpenSearch에 논문 문서 색인"""
        try:
            url = f"{OPENSEARCH_URL}/{INDEX_NAME}/_doc/{paper_id}"
            response = requests.put(url, json=doc, headers={'Content-Type': 'application/json'})

            if response.status_code in [200, 201]:
                return True
            else:
                print(f"❌ 색인 실패 (Paper ID: {paper_id}): {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"❌ 색인 오류 (Paper ID: {paper_id}): {e}")
            return False

    def reindex_all_papers(self, batch_size: int = 50):
        """모든 논문을 재색인"""
        if not self.connect_postgres():
            return

        # 총 논문 수 확인
        cursor = self.postgres_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL")
        self.total_papers = cursor.fetchone()[0]

        print(f"📊 총 {self.total_papers}개 논문을 재색인합니다...")

        offset = 0
        start_time = time.time()

        while offset < self.total_papers:
            print(f"\n📥 배치 처리 중: {offset + 1} ~ {min(offset + batch_size, self.total_papers)}")

            # 배치 데이터 가져오기
            papers_batch = self.fetch_papers_batch(offset, batch_size)

            if not papers_batch:
                break

            # 각 논문 색인
            batch_success = 0
            for paper in papers_batch:
                paper_id = paper['id']

                try:
                    doc = self.convert_paper_to_opensearch_doc(paper)

                    if self.index_paper_to_opensearch(paper_id, doc):
                        self.indexed_papers += 1
                        batch_success += 1
                        if batch_success % 10 == 0:
                            print(f"  ✅ {batch_success}개 완료")
                    else:
                        self.failed_papers += 1

                except Exception as e:
                    print(f"❌ 문서 변환 실패 (Paper ID: {paper_id}): {e}")
                    self.failed_papers += 1

            print(f"📊 배치 완료: 성공 {batch_success}/{len(papers_batch)}개")
            offset += batch_size

            # 진행률 출력
            progress = (self.indexed_papers + self.failed_papers) / self.total_papers * 100
            print(f"🔄 전체 진행률: {progress:.1f}% ({self.indexed_papers + self.failed_papers}/{self.total_papers})")

        # 최종 결과
        elapsed_time = time.time() - start_time
        print(f"\n🎉 재색인 완료!")
        print(f"✅ 성공: {self.indexed_papers}개")
        print(f"❌ 실패: {self.failed_papers}개")
        print(f"⏱️ 총 소요시간: {elapsed_time:.1f}초")

        # OpenSearch 인덱스 통계 확인
        self.check_opensearch_stats()

    def check_opensearch_stats(self):
        """OpenSearch 인덱스 통계 확인"""
        try:
            response = requests.get(f"{OPENSEARCH_URL}/{INDEX_NAME}/_count")
            if response.status_code == 200:
                count_data = response.json()
                print(f"📈 OpenSearch 인덱스 문서 수: {count_data['count']}개")

            # 임베딩 데이터 확인
            search_query = {
                "size": 1,
                "_source": ["title", "embeddings.full_text_embedding"],
                "query": {"exists": {"field": "embeddings.full_text_embedding"}}
            }

            response = requests.post(
                f"{OPENSEARCH_URL}/{INDEX_NAME}/_search",
                json=search_query,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                search_data = response.json()
                hits = search_data.get('hits', {}).get('hits', [])
                if hits:
                    embedding = hits[0]['_source']['embeddings']['full_text_embedding']
                    print(f"📊 임베딩 차원: {len(embedding)}차원")
                    print(f"📝 임베딩 샘플: {embedding[:5]}...")
                else:
                    print("⚠️ 임베딩 데이터가 있는 문서를 찾을 수 없습니다")

        except Exception as e:
            print(f"❌ OpenSearch 통계 확인 실패: {e}")

    def close(self):
        """리소스 정리"""
        if self.postgres_conn:
            self.postgres_conn.close()


if __name__ == "__main__":
    print("🔄 PostgreSQL → OpenSearch 논문 재색인 시작")
    print(f"📅 시작 시간: {datetime.now()}")

    reindexer = PaperReindexer()

    try:
        reindexer.reindex_all_papers(batch_size=20)  # 배치 크기 20개로 설정
    finally:
        reindexer.close()

    print(f"📅 종료 시간: {datetime.now()}")