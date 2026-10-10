#!/usr/bin/env python3
"""
간단한 논문 유사도 계산 (PySpark 대신 numpy 사용)

PostgreSQL에서 임베딩 벡터를 읽어와 모든 논문 간의 코사인 유사도를 계산하고
Top-K 결과를 HBase REST API에 저장
"""

import json
import os
import numpy as np
from typing import List, Tuple, Dict, Any
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
import base64

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

# HBase REST API 연결 설정
HBASE_CONFIG = {
    'host': 'localhost',
    'port': 8080,
    'rest_url': 'http://localhost:8080'
}

class HBaseRestClient:
    """HBase REST API 클라이언트"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def create_table(self, table_name: str, column_families: list):
        """테이블 생성"""
        try:
            # 테이블 존재 확인
            response = self.session.get(f"{self.base_url}/{table_name}/schema")
            if response.status_code == 200:
                print(f"✅ 테이블 '{table_name}' 이미 존재")
                return True

            # 테이블 생성 스키마
            schema = {
                "name": table_name,
                "ColumnSchema": [
                    {"name": cf} for cf in column_families
                ]
            }

            response = self.session.put(
                f"{self.base_url}/{table_name}/schema",
                json=schema
            )

            if response.status_code in [200, 201]:
                print(f"✅ 테이블 '{table_name}' 생성 완료")
                return True
            else:
                print(f"❌ 테이블 생성 실패: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"❌ 테이블 생성 오류: {e}")
            return False

    def put_row(self, table_name: str, row_key: str, data: dict):
        """행 데이터 저장"""
        try:
            # HBase REST API 형식으로 데이터 변환
            cells = []
            for column, value in data.items():
                # base64 인코딩
                cells.append({
                    "column": base64.b64encode(column.encode('utf-8')).decode('utf-8'),
                    "$": base64.b64encode(str(value).encode('utf-8')).decode('utf-8')
                })

            row_data = {
                "Row": [{
                    "key": base64.b64encode(row_key.encode('utf-8')).decode('utf-8'),
                    "Cell": cells
                }]
            }

            response = self.session.put(
                f"{self.base_url}/{table_name}/{row_key}",
                json=row_data
            )

            if response.status_code not in [200, 201]:
                print(f"❌ HBase PUT 실패: {response.status_code} - {response.text}")
                return False

            return True

        except Exception as e:
            print(f"❌ 행 저장 오류: {e}")
            return False

    def get_row(self, table_name: str, row_key: str):
        """행 데이터 조회"""
        try:
            response = self.session.get(f"{self.base_url}/{table_name}/{row_key}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"❌ 행 조회 오류: {e}")
            return None

class SimpleSimilarityCalculator:
    def __init__(self, top_k: int = 30):
        """
        간단한 유사도 계산기 초기화

        Args:
            top_k: 각 논문당 유사한 논문 상위 K개
        """
        self.top_k = top_k

    def load_embeddings_from_postgres(self) -> List[Dict[str, Any]]:
        """
        PostgreSQL에서 임베딩 데이터 로드

        Returns:
            논문 ID와 임베딩 벡터 리스트
        """
        try:
            print("📥 PostgreSQL에서 임베딩 데이터 로딩...")

            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    cursor.execute("""
                        SELECT
                            id,
                            title,
                            embedding,
                            embedding_model,
                            embedding_generated_at
                        FROM papers
                        WHERE embedding IS NOT NULL
                        ORDER BY id
                    """)

                    papers = cursor.fetchall()

            print(f"✅ {len(papers)}개 논문의 임베딩 로딩 완료")

            # 임베딩 벡터 파싱
            embeddings_data = []
            for paper in papers:
                try:
                    # pgvector 형식의 벡터를 numpy array로 변환
                    embedding_str = paper['embedding']
                    if embedding_str.startswith('[') and embedding_str.endswith(']'):
                        embedding_str = embedding_str[1:-1]

                    embedding_values = [float(x.strip()) for x in embedding_str.split(',')]
                    embedding_array = np.array(embedding_values, dtype=np.float32)

                    embeddings_data.append({
                        'paper_id': paper['id'],
                        'title': paper['title'],
                        'embedding': embedding_array,
                        'embedding_model': paper['embedding_model']
                    })

                except Exception as e:
                    print(f"⚠️ 논문 ID {paper['id']} 임베딩 파싱 실패: {e}")
                    continue

            print(f"📊 파싱 완료: {len(embeddings_data)}개 벡터")
            return embeddings_data

        except Exception as e:
            print(f"❌ 임베딩 데이터 로딩 실패: {e}")
            return []

    def calculate_cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """
        두 벡터 간의 코사인 유사도 계산

        Args:
            vec1: 첫 번째 벡터
            vec2: 두 번째 벡터

        Returns:
            코사인 유사도 (0~1)
        """
        try:
            # 코사인 유사도 계산
            dot_product = np.dot(vec1, vec2)
            norm_a = np.linalg.norm(vec1)
            norm_b = np.linalg.norm(vec2)

            if norm_a == 0 or norm_b == 0:
                return 0.0

            similarity = dot_product / (norm_a * norm_b)
            return float(similarity)

        except Exception as e:
            print(f"유사도 계산 오류: {e}")
            return 0.0

    def calculate_all_similarities(self, embeddings_data: List[Dict[str, Any]]) -> Dict[int, List[Dict]]:
        """
        모든 논문 간의 유사도 계산

        Args:
            embeddings_data: 임베딩 데이터 리스트

        Returns:
            논문별 상위 K개 유사 논문
        """
        try:
            print("🔢 논문 간 유사도 계산 중...")

            n_papers = len(embeddings_data)
            print(f"📊 계산할 논문 쌍 수: {n_papers * (n_papers - 1) // 2}개")

            # 모든 유사도 계산
            all_similarities = {}

            for i, paper_a in enumerate(embeddings_data):
                similarities = []

                for j, paper_b in enumerate(embeddings_data):
                    if i != j:  # 자기 자신 제외
                        similarity = self.calculate_cosine_similarity(
                            paper_a['embedding'],
                            paper_b['embedding']
                        )

                        similarities.append({
                            'target_paper_id': paper_b['paper_id'],
                            'target_title': paper_b['title'],
                            'similarity': similarity
                        })

                # 유사도 기준으로 정렬하고 상위 K개 선택
                similarities.sort(key=lambda x: x['similarity'], reverse=True)
                top_similarities = similarities[:self.top_k]

                # 랭킹 추가
                for rank, sim in enumerate(top_similarities, 1):
                    sim['rank'] = rank

                all_similarities[paper_a['paper_id']] = {
                    'source_title': paper_a['title'],
                    'similarities': top_similarities
                }

                print(f"  📄 논문 {i+1}/{n_papers} 완료: {paper_a['title'][:50]}...")

            print(f"✅ 유사도 계산 완료: {len(all_similarities)}개 논문")
            return all_similarities

        except Exception as e:
            print(f"❌ 유사도 계산 실패: {e}")
            return {}

    def setup_hbase_table(self):
        """HBase 테이블 설정"""
        try:
            print("🔧 HBase REST API 연결 및 테이블 설정...")

            hbase_client = HBaseRestClient(HBASE_CONFIG['rest_url'])
            table_name = 'paper_similarities'

            # 테이블 생성 (존재하지 않는 경우)
            column_families = ['similar', 'meta']

            success = hbase_client.create_table(table_name, column_families)
            if success:
                return hbase_client, table_name
            else:
                return None, None

        except Exception as e:
            print(f"❌ HBase 설정 실패: {e}")
            return None, None

    def save_similarities_to_hbase(self, paper_similarities: Dict[int, Dict], hbase_client_table):
        """
        유사도 결과를 HBase에 저장

        Args:
            paper_similarities: 논문별 유사도 결과
            hbase_client_table: (HBase 클라이언트, 테이블명) 튜플
        """
        try:
            print("💾 HBase에 유사도 결과 저장 중...")

            hbase_client, table_name = hbase_client_table
            saved_count = 0

            for source_paper_id, data in paper_similarities.items():
                try:
                    # Row Key 생성 (paper_id 기반)
                    row_key = f"paper_{source_paper_id:08d}"

                    # HBase에 저장할 데이터 준비
                    hbase_data = {}

                    # 메타데이터
                    hbase_data['meta:source_title'] = data['source_title']
                    hbase_data['meta:total_similarities'] = str(len(data['similarities']))
                    hbase_data['meta:calculated_at'] = datetime.now().isoformat()
                    hbase_data['meta:model'] = os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')

                    # 유사도 정보 (Top-K)
                    for sim in data['similarities']:
                        rank = sim['rank']
                        prefix = f"similar:rank_{rank:02d}"

                        hbase_data[f"{prefix}_paper_id"] = str(sim['target_paper_id'])
                        hbase_data[f"{prefix}_title"] = sim['target_title']
                        hbase_data[f"{prefix}_score"] = f"{sim['similarity']:.6f}"

                    # 전체 유사도 데이터를 JSON으로도 저장
                    similarities_json = json.dumps({
                        'source_paper_id': source_paper_id,
                        'similarities': data['similarities']
                    }, ensure_ascii=False)
                    hbase_data['similar:full_data'] = similarities_json

                    # HBase에 저장
                    print(f"🔄 논문 ID {source_paper_id} 저장 시도... (행 개수: {len(hbase_data)})")
                    success = hbase_client.put_row(table_name, row_key, hbase_data)
                    print(f"   저장 결과: {success}")
                    if success:
                        saved_count += 1

                    if saved_count % 5 == 0:
                        print(f"  💾 {saved_count}개 논문 저장 완료...")

                except Exception as e:
                    print(f"⚠️ 논문 ID {source_paper_id} 저장 실패: {e}")
                    continue

            print(f"✅ HBase 저장 완료: {saved_count}개 논문")

        except Exception as e:
            print(f"❌ HBase 저장 실패: {e}")

    def verify_hbase_data(self, hbase_client_table, sample_paper_id: int = None):
        """
        HBase에 저장된 데이터 검증

        Args:
            hbase_client_table: (HBase 클라이언트, 테이블명) 튜플
            sample_paper_id: 검증할 샘플 논문 ID
        """
        try:
            print("🔍 HBase 데이터 검증 중...")

            hbase_client, table_name = hbase_client_table

            # 샘플 데이터 조회
            if sample_paper_id:
                row_key = f"paper_{sample_paper_id:08d}"
                row_data = hbase_client.get_row(table_name, row_key)

                if row_data:
                    print(f"📋 샘플 논문 ID {sample_paper_id} 데이터:")
                    print(f"  ✅ 데이터가 성공적으로 저장됨")
                else:
                    print(f"⚠️ 샘플 논문 ID {sample_paper_id} 데이터 없음")
            else:
                print("📊 HBase 연결 테스트 완료")

        except Exception as e:
            print(f"❌ HBase 데이터 검증 실패: {e}")

    def run_similarity_calculation(self):
        """전체 유사도 계산 프로세스 실행"""
        try:
            print("🚀 논문 유사도 계산 시작...")

            # 1. PostgreSQL에서 임베딩 로드
            embeddings_data = self.load_embeddings_from_postgres()
            if not embeddings_data:
                print("❌ 임베딩 데이터가 없습니다")
                return

            # 2. 유사도 계산
            paper_similarities = self.calculate_all_similarities(embeddings_data)
            if not paper_similarities:
                return

            # 3. HBase 테이블 설정
            hbase_client_table = self.setup_hbase_table()
            if hbase_client_table[0] is None:
                return

            # 4. HBase에 저장
            self.save_similarities_to_hbase(paper_similarities, hbase_client_table)

            # 5. 검증
            sample_paper_id = list(paper_similarities.keys())[0] if paper_similarities else None
            self.verify_hbase_data(hbase_client_table, sample_paper_id)

            print("🎉 논문 유사도 계산 및 저장 완료!")

            # 결과 미리보기
            self.print_similarity_preview(paper_similarities)

        except Exception as e:
            print(f"❌ 유사도 계산 프로세스 실패: {e}")

    def print_similarity_preview(self, paper_similarities: Dict[int, Dict]):
        """유사도 결과 미리보기"""
        try:
            print("\n📋 유사도 계산 결과 미리보기:")

            for i, (paper_id, data) in enumerate(list(paper_similarities.items())[:3]):
                print(f"\n📄 논문 {i+1}: {data['source_title'][:60]}...")
                print(f"   ID: {paper_id}")
                print(f"   상위 {min(3, len(data['similarities']))}개 유사 논문:")

                for sim in data['similarities'][:3]:
                    print(f"     {sim['rank']}. [ID:{sim['target_paper_id']}] {sim['target_title'][:50]}...")
                    print(f"        유사도: {sim['similarity']:.4f}")

        except Exception as e:
            print(f"❌ 미리보기 출력 실패: {e}")

def main():
    """메인 실행 함수"""
    # Top-30 유사 논문 계산
    calculator = SimpleSimilarityCalculator(top_k=30)
    calculator.run_similarity_calculation()

if __name__ == "__main__":
    main()