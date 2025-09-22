#!/usr/bin/env python3
"""
논문 임베딩 생성 스크립트

GROBID로 처리된 논문 텍스트를 바탕으로 Sentence Transformers를 사용하여
768차원 임베딩을 생성하고 PostgreSQL pgvector에 저장
"""

import psycopg2
import psycopg2.extras
import numpy as np
import json
from datetime import datetime
from sentence_transformers import SentenceTransformer
import torch
from typing import List, Dict, Any
import sys
import os

# 모니터링 모듈 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from monitoring.performance_logger import measure_embedding, get_performance_logger

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'papers_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres123')
}

class EmbeddingGenerator:
    def __init__(self, model_name='all-mpnet-base-v2'):
        """
        임베딩 생성기 초기화

        Args:
            model_name: Sentence Transformers 모델명
        """
        print(f"🤖 임베딩 모델 로딩: {model_name}")

        # GPU 사용 가능 여부 확인
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        print(f"🔧 디바이스: {device}")

        # Sentence Transformers 모델 로드
        self.model = SentenceTransformer(model_name, device=device)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()

        print(f"✅ 모델 로딩 완료 - 임베딩 차원: {self.embedding_dim}")

    def extract_embedding_text(self, paper_data: Dict[str, Any]) -> str:
        """
        논문 데이터에서 임베딩 생성용 텍스트 추출

        Args:
            paper_data: 논문 정보 (title, abstract_text, full_text, grobid_data)

        Returns:
            결합된 텍스트 (제목 + 초록 + 전체 텍스트)
        """
        text_parts = []

        # 1. 제목
        if paper_data.get('title'):
            text_parts.append(f"Title: {paper_data['title']}")

        # 2. 초록 (우선순위: GROBID 추출 > 원본)
        abstract = ""
        grobid_data = paper_data.get('grobid_data')
        if grobid_data:
            try:
                if isinstance(grobid_data, str):
                    grobid_data = json.loads(grobid_data)
                abstract = grobid_data.get('abstract', '')
            except (json.JSONDecodeError, TypeError):
                pass

        if not abstract and paper_data.get('abstract_text'):
            abstract = paper_data['abstract_text']

        if abstract:
            text_parts.append(f"Abstract: {abstract}")

        # 3. 전체 텍스트 (GROBID에서 추출된 구조화된 텍스트 사용)
        full_text = ""
        if grobid_data and isinstance(grobid_data, dict):
            # GROBID 섹션별 텍스트 결합
            sections = grobid_data.get('sections', [])
            if sections:
                section_texts = []
                for section in sections:
                    if section.get('content'):
                        if section.get('title'):
                            section_texts.append(f"{section['title']}: {section['content']}")
                        else:
                            section_texts.append(section['content'])
                full_text = " ".join(section_texts)
            elif grobid_data.get('full_text'):
                full_text = grobid_data['full_text']
        elif paper_data.get('full_text'):
            full_text = paper_data['full_text']

        if full_text:
            # 텍스트가 너무 긴 경우 제한 (임베딩 모델 제한 고려)
            max_length = 8000  # 토큰 제한을 고려한 문자 수
            if len(full_text) > max_length:
                full_text = full_text[:max_length] + "..."
            text_parts.append(f"Content: {full_text}")

        # 모든 텍스트 결합
        combined_text = " ".join(text_parts)

        if not combined_text.strip():
            return None

        return combined_text

    def generate_embedding(self, text: str) -> np.ndarray:
        """
        텍스트에서 임베딩 벡터 생성

        Args:
            text: 입력 텍스트

        Returns:
            768차원 임베딩 벡터
        """
        try:
            embedding = self.model.encode(text, convert_to_numpy=True)
            return embedding.astype(np.float32)
        except Exception as e:
            print(f"  ❌ 임베딩 생성 실패: {e}")
            return None

    def save_embedding_to_db(self, paper_id: int, embedding: np.ndarray) -> bool:
        """
        생성된 임베딩을 PostgreSQL에 저장

        Args:
            paper_id: 논문 ID
            embedding: 임베딩 벡터

        Returns:
            저장 성공 여부
        """
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:

                    # pgvector 확장을 위한 임베딩 포맷 변환
                    embedding_str = '[' + ','.join(map(str, embedding)) + ']'

                    cursor.execute("""
                        UPDATE papers
                        SET embedding = %s::vector,
                            embedding_model = 'all-mpnet-base-v2',
                            embedding_generated_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                    """, (embedding_str, paper_id))

                    return True

        except Exception as e:
            print(f"  ❌ 임베딩 저장 실패: {e}")
            return False

    def process_papers_for_embeddings(self):
        """
        모든 텍스트가 있는 논문에 대해 임베딩 생성
        """
        try:
            # 텍스트가 있고 임베딩이 없는 논문들 조회
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    cursor.execute("""
                        SELECT id, title, abstract_text, full_text, grobid_data
                        FROM papers
                        WHERE (full_text IS NOT NULL OR grobid_data IS NOT NULL)
                        AND embedding IS NULL
                        ORDER BY id
                    """)

                    papers = cursor.fetchall()
                    print(f"🚀 임베딩 생성 대상: {len(papers)}개 논문")

            if not papers:
                print("✅ 모든 논문에 이미 임베딩이 있습니다.")
                return

            success_count = 0
            failed_count = 0

            for i, paper in enumerate(papers, 1):
                print(f"\n📄 논문 {i}/{len(papers)} 처리 중...")
                print(f"  ID: {paper['id']}")
                print(f"  제목: {paper['title'][:60]}...")

                try:
                    # 1. 임베딩용 텍스트 추출
                    embedding_text = self.extract_embedding_text(dict(paper))

                    if not embedding_text:
                        print(f"  ⚠️ 임베딩할 텍스트가 없음")
                        failed_count += 1
                        continue

                    print(f"  📝 텍스트 길이: {len(embedding_text)} 문자")

                    # 2. 임베딩 생성
                    embedding = self.generate_embedding(embedding_text)

                    if embedding is None:
                        failed_count += 1
                        continue

                    print(f"  🤖 임베딩 생성 완료: {embedding.shape}")

                    # 3. 데이터베이스에 저장
                    success = self.save_embedding_to_db(paper['id'], embedding)

                    if success:
                        success_count += 1
                        print(f"  ✅ 임베딩 저장 완료")
                    else:
                        failed_count += 1

                except Exception as e:
                    print(f"  ❌ 논문 처리 실패: {e}")
                    failed_count += 1
                    continue

            print(f"\n🎉 임베딩 생성 완료:")
            print(f"  성공: {success_count}개")
            print(f"  실패: {failed_count}개")

            # 최종 통계 출력
            self.print_embedding_status()

        except Exception as e:
            print(f"❌ 임베딩 생성 프로세스 실패: {e}")

    def print_embedding_status(self):
        """임베딩 상태 통계 출력"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(*) as total_papers,
                            COUNT(CASE WHEN embedding IS NOT NULL THEN 1 END) as with_embedding,
                            COUNT(CASE WHEN full_text IS NOT NULL OR grobid_data IS NOT NULL THEN 1 END) as with_text,
                            COUNT(CASE WHEN grobid_status = 'completed' THEN 1 END) as grobid_completed
                        FROM papers
                    """)

                    stats = cursor.fetchone()

                    print(f"\n📊 임베딩 현황:")
                    print(f"  전체 논문: {stats[0]}개")
                    print(f"  텍스트 있음: {stats[2]}개")
                    print(f"  GROBID 완료: {stats[3]}개")
                    print(f"  임베딩 있음: {stats[1]}개")

                    # 임베딩 커버리지 계산
                    if stats[2] > 0:
                        coverage = (stats[1] / stats[2]) * 100
                        print(f"  임베딩 커버리지: {coverage:.1f}%")

        except Exception as e:
            print(f"❌ 상태 조회 실패: {e}")

    def test_similarity_search(self, query: str, top_k: int = 5):
        """
        임베딩 기반 유사도 검색 테스트

        Args:
            query: 검색 쿼리
            top_k: 상위 K개 결과
        """
        try:
            print(f"\n🔍 유사도 검색 테스트: '{query}'")

            # 쿼리 임베딩 생성
            query_embedding = self.generate_embedding(query)
            if query_embedding is None:
                print("❌ 쿼리 임베딩 생성 실패")
                return

            query_embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'

            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # 코사인 유사도를 사용한 검색
                    cursor.execute("""
                        SELECT
                            id, title,
                            1 - (embedding <=> %s::vector) as similarity
                        FROM papers
                        WHERE embedding IS NOT NULL
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                    """, (query_embedding_str, query_embedding_str, top_k))

                    results = cursor.fetchall()

                    print(f"📋 검색 결과 (상위 {len(results)}개):")
                    for i, result in enumerate(results, 1):
                        print(f"  {i}. [ID:{result['id']}] {result['title'][:80]}...")
                        print(f"     유사도: {result['similarity']:.4f}")

        except Exception as e:
            print(f"❌ 유사도 검색 테스트 실패: {e}")

def main():
    """메인 실행 함수"""
    generator = EmbeddingGenerator()

    # 임베딩 생성
    generator.process_papers_for_embeddings()

    # 테스트 검색
    generator.test_similarity_search("deep learning neural networks", top_k=3)
    generator.test_similarity_search("computer vision image classification", top_k=3)

if __name__ == "__main__":
    main()