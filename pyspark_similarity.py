#!/usr/bin/env python3
"""
PySpark를 사용한 대규모 논문 유사도 계산

PostgreSQL에서 임베딩 벡터를 읽어와 모든 논문 간의 코사인 유사도를 계산하고
Top-K 결과를 HBase에 저장
"""

import os
import sys
import json
import numpy as np
from typing import List, Tuple, Dict, Any
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
import base64

class HBaseRestClient:
    """HBase REST API 클라이언트"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def create_table(self, table_name: str, column_families: dict):
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
                    {"name": cf, "COMPRESSION": "NONE"}
                    for cf in column_families.keys()
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
            import base64
            # HBase REST API 형식으로 데이터 변환
            cells = []
            for column, value in data.items():
                if isinstance(column, bytes):
                    column = column.decode('utf-8')
                if isinstance(value, bytes):
                    value = value.decode('utf-8')

                # 컬럼 패밀리:컬럼 분리
                if ':' in column:
                    cf_col = column
                else:
                    cf_col = f"cf:{column}"

                cells.append({
                    "column": base64.b64encode(cf_col.encode('utf-8')).decode('utf-8'),
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

# PySpark 설정
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql import Row

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'papers_db',
    'user': 'postgres',
    'password': 'postgres123'
}

# HBase REST API 연결 설정
HBASE_CONFIG = {
    'host': 'localhost',
    'port': 8080,
    'rest_url': 'http://localhost:8080'
}

class PySparkSimilarityCalculator:
    def __init__(self, top_k: int = 30):
        """
        PySpark 유사도 계산기 초기화

        Args:
            top_k: 각 논문당 유사한 논문 상위 K개
        """
        self.top_k = top_k
        self.spark = None
        self.setup_spark()

    def setup_spark(self):
        """Spark 세션 설정"""
        try:
            print("🔧 Spark 세션 설정 중...")

            # Spark 설정
            self.spark = SparkSession.builder \
                .appName("PaperSimilarityCalculation") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.cores", "2") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()

            # 로그 레벨 설정
            self.spark.sparkContext.setLogLevel("WARN")

            print(f"✅ Spark 세션 시작 완료")
            print(f"   Spark Version: {self.spark.version}")
            print(f"   Total Cores: {self.spark.sparkContext.defaultParallelism}")

        except Exception as e:
            print(f"❌ Spark 세션 설정 실패: {e}")
            raise

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

    def create_spark_dataframe(self, embeddings_data: List[Dict[str, Any]]):
        """
        임베딩 데이터로 Spark DataFrame 생성

        Args:
            embeddings_data: 임베딩 데이터 리스트

        Returns:
            Spark DataFrame
        """
        try:
            print("🔄 Spark DataFrame 생성 중...")

            # Row 객체로 변환
            rows = []
            for item in embeddings_data:
                # 임베딩 벡터를 Spark ML Vector로 변환
                vector = Vectors.dense(item['embedding'].tolist())
                rows.append(Row(
                    paper_id=item['paper_id'],
                    title=item['title'],
                    embedding=vector,
                    embedding_model=item['embedding_model']
                ))

            # DataFrame 생성
            schema = StructType([
                StructField("paper_id", IntegerType(), False),
                StructField("title", StringType(), True),
                StructField("embedding", VectorUDT(), False),
                StructField("embedding_model", StringType(), True)
            ])

            df = self.spark.createDataFrame(rows, schema)
            df.cache()  # 메모리 캐싱

            print(f"✅ DataFrame 생성 완료: {df.count()}개 행")
            return df

        except Exception as e:
            print(f"❌ DataFrame 생성 실패: {e}")
            return None

    def calculate_pairwise_similarity(self, df):
        """
        모든 논문 쌍의 코사인 유사도 계산

        Args:
            df: 임베딩이 포함된 Spark DataFrame

        Returns:
            유사도 결과 DataFrame
        """
        try:
            print("🔢 논문 간 유사도 계산 중...")

            # 자기 자신과의 조인을 위해 별칭 사용
            df_a = df.alias("a")
            df_b = df.alias("b")

            # 카티시안 곱으로 모든 쌍 생성 (자기 자신 제외)
            pairs_df = df_a.crossJoin(df_b).where(col("a.paper_id") < col("b.paper_id"))

            print(f"📊 계산할 논문 쌍 수: {pairs_df.count()}개")

            # 코사인 유사도 계산 UDF 정의
            def cosine_similarity(v1, v2):
                """코사인 유사도 계산"""
                try:
                    # Spark Vector를 numpy array로 변환
                    arr1 = np.array(v1.toArray())
                    arr2 = np.array(v2.toArray())

                    # 코사인 유사도 계산
                    dot_product = np.dot(arr1, arr2)
                    norm_a = np.linalg.norm(arr1)
                    norm_b = np.linalg.norm(arr2)

                    if norm_a == 0 or norm_b == 0:
                        return 0.0

                    similarity = dot_product / (norm_a * norm_b)
                    return float(similarity)

                except Exception as e:
                    print(f"유사도 계산 오류: {e}")
                    return 0.0

            # UDF 등록
            cosine_similarity_udf = udf(cosine_similarity, DoubleType())

            # 유사도 계산
            similarity_df = pairs_df.withColumn(
                "similarity",
                cosine_similarity_udf(col("a.embedding"), col("b.embedding"))
            ).select(
                col("a.paper_id").alias("paper_id_1"),
                col("a.title").alias("title_1"),
                col("b.paper_id").alias("paper_id_2"),
                col("b.title").alias("title_2"),
                col("similarity")
            )

            # 유사도가 0보다 큰 것만 필터링
            similarity_df = similarity_df.filter(col("similarity") > 0)

            print(f"✅ 유사도 계산 완료")
            return similarity_df

        except Exception as e:
            print(f"❌ 유사도 계산 실패: {e}")
            return None

    def get_top_k_similarities_per_paper(self, similarity_df):
        """
        각 논문당 상위 K개 유사 논문 추출

        Args:
            similarity_df: 유사도 DataFrame

        Returns:
            Top-K 결과 리스트
        """
        try:
            print(f"🔝 각 논문당 상위 {self.top_k}개 유사 논문 추출...")

            # 대칭성을 위해 양방향 관계 생성
            df1 = similarity_df.select(
                col("paper_id_1").alias("source_paper"),
                col("title_1").alias("source_title"),
                col("paper_id_2").alias("target_paper"),
                col("title_2").alias("target_title"),
                col("similarity")
            )

            df2 = similarity_df.select(
                col("paper_id_2").alias("source_paper"),
                col("title_2").alias("source_title"),
                col("paper_id_1").alias("target_paper"),
                col("title_1").alias("target_title"),
                col("similarity")
            )

            # 두 방향 결합
            bidirectional_df = df1.union(df2)

            # 각 논문에 대해 상위 K개 유사 논문 선택
            from pyspark.sql.window import Window

            window_spec = Window.partitionBy("source_paper").orderBy(desc("similarity"))
            top_k_df = bidirectional_df.withColumn(
                "rank",
                row_number().over(window_spec)
            ).filter(col("rank") <= self.top_k)

            # 결과 수집
            results = top_k_df.collect()

            print(f"✅ Top-K 추출 완료: {len(results)}개 관계")

            # 논문별로 그룹화
            paper_similarities = {}
            for row in results:
                source_id = row['source_paper']
                if source_id not in paper_similarities:
                    paper_similarities[source_id] = {
                        'source_title': row['source_title'],
                        'similarities': []
                    }

                paper_similarities[source_id]['similarities'].append({
                    'target_paper_id': row['target_paper'],
                    'target_title': row['target_title'],
                    'similarity': row['similarity'],
                    'rank': row['rank']
                })

            return paper_similarities

        except Exception as e:
            print(f"❌ Top-K 추출 실패: {e}")
            return {}

    def setup_hbase_table(self):
        """HBase 테이블 설정"""
        try:
            print("🔧 HBase REST API 연결 및 테이블 설정...")

            hbase_client = HBaseRestClient(HBASE_CONFIG['rest_url'])
            table_name = 'paper_similarities'

            # 테이블 생성 (존재하지 않는 경우)
            column_families = {
                'similar': {},  # 유사도 정보
                'meta': {}     # 메타데이터
            }

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
                    hbase_data['meta:model'] = 'all-mpnet-base-v2'

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
                    success = hbase_client.put_row(table_name, row_key, hbase_data)
                    if success:
                        saved_count += 1

                    if saved_count % 10 == 0:
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
                    # JSON 응답에서 실제 데이터 파싱은 복잡하므로 간단히 확인만
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

            # 2. Spark DataFrame 생성
            df = self.create_spark_dataframe(embeddings_data)
            if df is None:
                return

            # 3. 유사도 계산
            similarity_df = self.calculate_pairwise_similarity(df)
            if similarity_df is None:
                return

            # 4. Top-K 추출
            paper_similarities = self.get_top_k_similarities_per_paper(similarity_df)
            if not paper_similarities:
                return

            # 5. HBase 테이블 설정
            hbase_client_table = self.setup_hbase_table()
            if hbase_client_table[0] is None:
                return

            # 6. HBase에 저장
            self.save_similarities_to_hbase(paper_similarities, hbase_client_table)

            # 7. 검증
            sample_paper_id = list(paper_similarities.keys())[0] if paper_similarities else None
            self.verify_hbase_data(hbase_client_table, sample_paper_id)

            print("🎉 논문 유사도 계산 및 저장 완료!")

        except Exception as e:
            print(f"❌ 유사도 계산 프로세스 실패: {e}")

        finally:
            if self.spark:
                self.spark.stop()
                print("🛑 Spark 세션 종료")

def main():
    """메인 실행 함수"""
    # Top-30 유사 논문 계산
    calculator = PySparkSimilarityCalculator(top_k=30)
    calculator.run_similarity_calculation()

if __name__ == "__main__":
    main()