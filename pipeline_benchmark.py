#!/usr/bin/env python3
"""
논문 처리 파이프라인 병목 분석 및 벤치마크

각 단계별 처리 속도 측정:
1. OpenAlex 메타데이터 수집
2. PDF 다운로드 + GROBID 처리
3. 임베딩 생성
4. 유사도 계산 + HBase 저장
"""

import os
import time
import requests
import psycopg2
import psycopg2.extras
import numpy as np
from datetime import datetime
import subprocess
import json
import base64
from typing import Dict, List, Tuple
import statistics

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

class PipelineBenchmark:
    def __init__(self):
        self.results = {
            'openalex_metadata': [],
            'pdf_grobid': [],
            'embedding': [],
            'similarity_hbase': []
        }

    def benchmark_openalex_metadata(self, count=5):
        """OpenAlex 메타데이터 수집 벤치마크"""
        print(f"🔍 1. OpenAlex 메타데이터 수집 벤치마크 ({count}개 논문)")

        base_url = "https://api.openalex.org/works"
        headers = {'User-Agent': 'Mozilla/5.0 (RSP-Paper-System/1.0; mailto:test@example.com)'}

        times = []

        for i in range(count):
            start_time = time.time()

            try:
                params = {
                    'search': f'machine learning page:{i+1}',
                    'filter': 'concepts.id:C41008148,type_crossref:journal-article,open_access.is_oa:true,has_doi:true',
                    'per-page': 1,
                    'sort': 'cited_by_count:desc'
                }

                response = requests.get(base_url, params=params, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    paper = data['results'][0] if data['results'] else None

                    if paper:
                        elapsed = time.time() - start_time
                        times.append(elapsed)
                        print(f"   논문 {i+1}: {elapsed:.2f}초 - {paper['title'][:50]}...")
                    else:
                        print(f"   논문 {i+1}: 데이터 없음")
                else:
                    print(f"   논문 {i+1}: API 오류 {response.status_code}")

            except Exception as e:
                print(f"   논문 {i+1}: 오류 - {e}")

            # API 제한 방지
            time.sleep(0.1)

        if times:
            avg_time = statistics.mean(times)
            self.results['openalex_metadata'] = times
            print(f"   📊 평균 시간: {avg_time:.2f}초/논문")
            print(f"   📊 처리 속도: {3600/avg_time:.0f}논문/시간")

        return times

    def benchmark_pdf_grobid(self, sample_size=3):
        """PDF + GROBID 처리 벤치마크"""
        print(f"\n📄 2. PDF 다운로드 + GROBID 처리 벤치마크 ({sample_size}개 논문)")

        # PDF URL이 있는 논문 샘플 조회
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT id, title, pdf_url
                        FROM papers
                        WHERE pdf_url IS NOT NULL
                        AND grobid_status IS NULL
                        LIMIT %s
                    """, (sample_size,))
                    papers = cursor.fetchall()
        except Exception as e:
            print(f"   ❌ DB 조회 실패: {e}")
            return []

        if not papers:
            print("   ⚠️ 처리할 PDF가 없습니다")
            return []

        times = []
        grobid_url = "http://localhost:8070/api/processFulltextDocument"

        for paper in papers:
            start_time = time.time()

            try:
                print(f"   처리 중: {paper['title'][:50]}...")

                # PDF 다운로드
                pdf_start = time.time()
                pdf_response = requests.get(paper['pdf_url'], timeout=30)
                pdf_download_time = time.time() - pdf_start

                if pdf_response.status_code == 200:
                    # GROBID 처리
                    grobid_start = time.time()
                    files = {'input': ('paper.pdf', pdf_response.content, 'application/pdf')}
                    grobid_response = requests.post(grobid_url, files=files, timeout=60)
                    grobid_process_time = time.time() - grobid_start

                    if grobid_response.status_code == 200:
                        total_time = time.time() - start_time
                        times.append(total_time)
                        print(f"     ✅ 완료: {total_time:.2f}초 (PDF: {pdf_download_time:.2f}초, GROBID: {grobid_process_time:.2f}초)")
                    else:
                        print(f"     ❌ GROBID 실패: {grobid_response.status_code}")
                else:
                    print(f"     ❌ PDF 다운로드 실패: {pdf_response.status_code}")

            except Exception as e:
                print(f"     ❌ 오류: {e}")

        if times:
            avg_time = statistics.mean(times)
            self.results['pdf_grobid'] = times
            print(f"   📊 평균 시간: {avg_time:.2f}초/논문")
            print(f"   📊 처리 속도: {3600/avg_time:.0f}논문/시간")

        return times

    def benchmark_embedding_generation(self, sample_size=5):
        """임베딩 생성 벤치마크"""
        print(f"\n🔄 3. 임베딩 생성 벤치마크 ({sample_size}개 논문)")

        # 임베딩이 없는 논문 샘플 조회
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT id, title, abstract
                        FROM papers
                        WHERE embedding IS NULL
                        AND (abstract IS NOT NULL OR title IS NOT NULL)
                        LIMIT %s
                    """, (sample_size,))
                    papers = cursor.fetchall()
        except Exception as e:
            print(f"   ❌ DB 조회 실패: {e}")
            return []

        if not papers:
            print("   ⚠️ 처리할 논문이 없습니다")
            return []

        # 임베딩 모델 로드 시간 측정
        print("   🔧 임베딩 모델 로딩 중...")
        model_start = time.time()

        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
            model = SentenceTransformer(model_name)
            model_load_time = time.time() - model_start
            print(f"   ✅ 모델 로딩 완료: {model_load_time:.2f}초")
        except Exception as e:
            print(f"   ❌ 모델 로딩 실패: {e}")
            return []

        times = []

        for paper in papers:
            start_time = time.time()

            try:
                # 텍스트 준비
                text_parts = []
                if paper['title']:
                    text_parts.append(paper['title'])
                if paper['abstract']:
                    text_parts.append(paper['abstract'])

                combined_text = ' '.join(text_parts)

                if combined_text.strip():
                    embedding = model.encode(combined_text)
                    embedding_list = embedding.tolist()
                    if len(embedding_list) != 384:
                        print(f"   논문 {paper['id']}: 차원 {len(embedding_list)} 은 VECTOR(384)에 넣지 않음")
                        continue

                    embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'

                    with psycopg2.connect(**DB_CONFIG) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                UPDATE papers
                                SET embedding = %s::vector,
                                    embedding_model = %s,
                                    embedding_generated_at = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (embedding_str, model_name, datetime.now(), paper['id']))
                            conn.commit()

                    elapsed = time.time() - start_time
                    times.append(elapsed)
                    print(f"   논문 {paper['id']}: {elapsed:.2f}초 - {paper['title'][:50]}...")

            except Exception as e:
                print(f"   논문 {paper['id']}: 오류 - {e}")

        if times:
            avg_time = statistics.mean(times)
            self.results['embedding'] = times
            print(f"   📊 평균 시간: {avg_time:.2f}초/논문 (모델 로딩 제외)")
            print(f"   📊 처리 속도: {3600/avg_time:.0f}논문/시간")
            print(f"   📊 모델 로딩: {model_load_time:.2f}초 (1회)")

        return times

    def benchmark_similarity_calculation(self, sample_size=10):
        """유사도 계산 벤치마크"""
        print(f"\n🔢 4. 유사도 계산 벤치마크 ({sample_size}개 논문)")

        # 임베딩이 있는 논문들 조회
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT id, title, embedding
                        FROM papers
                        WHERE embedding IS NOT NULL
                        LIMIT %s
                    """, (sample_size,))
                    papers = cursor.fetchall()
        except Exception as e:
            print(f"   ❌ DB 조회 실패: {e}")
            return []

        if len(papers) < 2:
            print("   ⚠️ 유사도 계산을 위한 논문이 부족합니다 (최소 2개 필요)")
            return []

        print(f"   📊 총 비교 횟수: {len(papers) * (len(papers) - 1) // 2}개")

        # 임베딩 파싱
        embeddings_data = []
        for paper in papers:
            try:
                embedding_str = paper['embedding']
                if embedding_str.startswith('[') and embedding_str.endswith(']'):
                    embedding_str = embedding_str[1:-1]

                embedding_values = [float(x.strip()) for x in embedding_str.split(',')]
                embedding_array = np.array(embedding_values, dtype=np.float32)

                embeddings_data.append({
                    'paper_id': paper['id'],
                    'title': paper['title'],
                    'embedding': embedding_array
                })
            except Exception as e:
                print(f"   ⚠️ 논문 {paper['id']} 임베딩 파싱 실패: {e}")

        if len(embeddings_data) < 2:
            print("   ❌ 유효한 임베딩이 부족합니다")
            return []

        # 유사도 계산 시간 측정
        start_time = time.time()

        similarities = []
        comparison_count = 0

        for i, paper_a in enumerate(embeddings_data):
            paper_similarities = []

            for j, paper_b in enumerate(embeddings_data):
                if i != j:
                    # 코사인 유사도 계산
                    vec1 = paper_a['embedding']
                    vec2 = paper_b['embedding']

                    dot_product = np.dot(vec1, vec2)
                    norm_a = np.linalg.norm(vec1)
                    norm_b = np.linalg.norm(vec2)

                    if norm_a != 0 and norm_b != 0:
                        similarity = dot_product / (norm_a * norm_b)
                        paper_similarities.append({
                            'target_paper_id': paper_b['paper_id'],
                            'similarity': float(similarity)
                        })
                        comparison_count += 1

            # Top-K 선택 (여기서는 모든 유사도)
            paper_similarities.sort(key=lambda x: x['similarity'], reverse=True)
            similarities.append({
                'source_paper_id': paper_a['paper_id'],
                'similarities': paper_similarities
            })

        calculation_time = time.time() - start_time

        print(f"   ✅ 유사도 계산 완료: {calculation_time:.2f}초")
        print(f"   📊 비교 횟수: {comparison_count}개")
        print(f"   📊 계산 속도: {comparison_count/calculation_time:.0f}회/초")

        # HBase 저장 시간 측정 (샘플만)
        hbase_start = time.time()

        try:
            hbase_url = "http://localhost:8080"
            sample_paper = similarities[0]

            # 테스트 데이터 저장
            test_data = {
                'meta:source_title': f"benchmark_test_{int(time.time())}",
                'meta:total_similarities': str(len(sample_paper['similarities'])),
                'similar:rank_01_paper_id': str(sample_paper['similarities'][0]['target_paper_id']),
                'similar:rank_01_score': f"{sample_paper['similarities'][0]['similarity']:.6f}"
            }

            # HBase REST API 호출
            cells = []
            for column, value in test_data.items():
                cells.append({
                    "column": base64.b64encode(column.encode('utf-8')).decode('utf-8'),
                    "$": base64.b64encode(str(value).encode('utf-8')).decode('utf-8')
                })

            row_data = {
                "Row": [{
                    "key": base64.b64encode("benchmark_test".encode('utf-8')).decode('utf-8'),
                    "Cell": cells
                }]
            }

            response = requests.put(
                f"{hbase_url}/paper_similarities/benchmark_test",
                json=row_data,
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
            )

            hbase_time = time.time() - hbase_start

            if response.status_code in [200, 201]:
                print(f"   ✅ HBase 저장 테스트: {hbase_time:.3f}초")

                # 저장된 데이터 정리
                requests.delete(f"{hbase_url}/paper_similarities/benchmark_test")
            else:
                print(f"   ❌ HBase 저장 실패: {response.status_code}")

        except Exception as e:
            print(f"   ❌ HBase 테스트 실패: {e}")
            hbase_time = 0

        # 논문당 처리 시간 계산
        total_time = calculation_time + hbase_time
        per_paper_time = total_time / len(embeddings_data)

        self.results['similarity_hbase'].append(per_paper_time)

        print(f"   📊 논문당 평균 시간: {per_paper_time:.3f}초")
        print(f"   📊 처리 속도: {3600/per_paper_time:.0f}논문/시간")

        return [per_paper_time]

    def run_full_benchmark(self):
        """전체 벤치마크 실행"""
        print("🚀 논문 처리 파이프라인 벤치마크 시작")
        print("="*80)

        # 1. OpenAlex 메타데이터
        self.benchmark_openalex_metadata(count=3)

        # 2. PDF + GROBID
        self.benchmark_pdf_grobid(sample_size=2)

        # 3. 임베딩 생성
        self.benchmark_embedding_generation(sample_size=3)

        # 4. 유사도 계산
        self.benchmark_similarity_calculation(sample_size=10)

        # 결과 요약
        self.print_summary()

    def print_summary(self):
        """벤치마크 결과 요약"""
        print("\n" + "="*80)
        print("📊 파이프라인 벤치마크 결과 요약")
        print("="*80)

        stages = [
            ('OpenAlex 메타데이터 수집', 'openalex_metadata'),
            ('PDF + GROBID 처리', 'pdf_grobid'),
            ('임베딩 생성', 'embedding'),
            ('유사도 계산 + HBase', 'similarity_hbase')
        ]

        total_time_per_paper = 0

        for stage_name, key in stages:
            times = self.results[key]
            if times:
                avg_time = statistics.mean(times)
                throughput = 3600 / avg_time
                total_time_per_paper += avg_time

                print(f"\n{stage_name}:")
                print(f"  평균 시간: {avg_time:.2f}초/논문")
                print(f"  처리 속도: {throughput:.0f}논문/시간")
                print(f"  병목도: {(avg_time/total_time_per_paper)*100:.1f}%")
            else:
                print(f"\n{stage_name}: 측정 데이터 없음")

        print(f"\n🔍 전체 파이프라인:")
        print(f"  논문당 총 시간: {total_time_per_paper:.2f}초")
        print(f"  전체 처리 속도: {3600/total_time_per_paper:.0f}논문/시간")
        print(f"  일일 처리 가능량: {24*3600/total_time_per_paper:.0f}논문/일")

        # 병목 구간 식별
        if self.results:
            max_time = 0
            bottleneck = ""
            for stage_name, key in stages:
                if self.results[key]:
                    avg_time = statistics.mean(self.results[key])
                    if avg_time > max_time:
                        max_time = avg_time
                        bottleneck = stage_name

            print(f"\n🚨 주요 병목: {bottleneck} ({max_time:.2f}초/논문)")

def main():
    benchmark = PipelineBenchmark()
    benchmark.run_full_benchmark()

if __name__ == "__main__":
    main()