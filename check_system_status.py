#!/usr/bin/env python3
"""
RSP 논문 추천 시스템 전체 상태 점검 스크립트
"""

import os
import sys
import psycopg2
import requests
import json
from datetime import datetime

def check_postgresql_status():
    """PostgreSQL 데이터베이스 상태 확인"""
    print("=" * 60)
    print("1. PostgreSQL 데이터베이스 상태 확인")
    print("=" * 60)

    # 시도할 비밀번호 목록
    passwords = ["ssafy", "postgres123", "postgres", "SecurePostgres123!", ""]
    databases = ["rsp_db", "papers_db", "postgres"]

    conn = None
    for db in databases:
        for password in passwords:
            try:
                # 데이터베이스 연결 시도
                conn = psycopg2.connect(
                    host="localhost",
                    database=db,
                    user="postgres",
                    password=password
                )
                print(f"✅ 데이터베이스 연결 성공: {db} (password: {'***' if password else 'none'})")
                break
            except Exception as e:
                continue
        if conn:
            break

    if not conn:
        print("❌ 모든 데이터베이스 연결 시도 실패")
        return None

    try:
        cursor = conn.cursor()

        # 전체 논문 수
        cursor.execute("SELECT COUNT(*) FROM papers")
        total_papers = cursor.fetchone()[0]
        print(f"전체 논문 수: {total_papers:,}")

        # 임베딩 생성된 논문 수
        cursor.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL")
        papers_with_embeddings = cursor.fetchone()[0]
        print(f"임베딩 생성된 논문 수: {papers_with_embeddings:,}")

        # GROBID 처리된 논문 수 (grobid_data가 있는 논문)
        cursor.execute("SELECT COUNT(*) FROM papers WHERE grobid_data IS NOT NULL")
        grobid_processed = cursor.fetchone()[0]
        print(f"GROBID 처리된 논문 수: {grobid_processed:,}")

        # 키워드 추출된 논문 수
        cursor.execute("SELECT COUNT(*) FROM papers WHERE keywords IS NOT NULL")
        papers_with_keywords = cursor.fetchone()[0]
        print(f"키워드 추출된 논문 수: {papers_with_keywords:,}")

        # PDF 파일이 있는 논문 수
        cursor.execute("SELECT COUNT(*) FROM papers WHERE pdf_url IS NOT NULL")
        papers_with_pdf = cursor.fetchone()[0]
        print(f"PDF 파일이 있는 논문 수: {papers_with_pdf:,}")

        # Full text가 있는 논문 수
        cursor.execute("SELECT COUNT(*) FROM papers WHERE full_text IS NOT NULL AND full_text != ''")
        papers_with_fulltext = cursor.fetchone()[0]
        print(f"전문 텍스트가 있는 논문 수: {papers_with_fulltext:,}")

        # GROBID 상태별 논문 수
        cursor.execute("SELECT grobid_status, COUNT(*) FROM papers WHERE grobid_status IS NOT NULL GROUP BY grobid_status")
        grobid_status_counts = cursor.fetchall()
        if grobid_status_counts:
            print("\nGROBID 상태별 논문 수:")
            for status, count in grobid_status_counts:
                print(f"  {status}: {count:,}")

        # 논문 임베딩 관련 통계
        cursor.execute("SELECT COUNT(*) FROM paper_embeddings")
        embedding_table_count = cursor.fetchone()[0]
        print(f"paper_embeddings 테이블 레코드 수: {embedding_table_count:,}")

        # 최근 추가된 논문 (최근 7일)
        cursor.execute("""
            SELECT COUNT(*) FROM papers
            WHERE created_at >= NOW() - INTERVAL '7 days'
        """)
        recent_papers = cursor.fetchone()[0]
        print(f"최근 7일간 추가된 논문 수: {recent_papers:,}")

        # 처리 완료 비율 계산
        if total_papers > 0:
            embedding_ratio = (papers_with_embeddings / total_papers) * 100
            grobid_ratio = (grobid_processed / total_papers) * 100
            keyword_ratio = (papers_with_keywords / total_papers) * 100

            print(f"\n처리 완료 비율:")
            print(f"  임베딩 생성: {embedding_ratio:.1f}%")
            print(f"  GROBID 처리: {grobid_ratio:.1f}%")
            print(f"  키워드 추출: {keyword_ratio:.1f}%")

        cursor.close()
        conn.close()

        return {
            'total_papers': total_papers,
            'papers_with_embeddings': papers_with_embeddings,
            'grobid_processed': grobid_processed,
            'papers_with_keywords': papers_with_keywords,
            'papers_with_pdf': papers_with_pdf,
            'papers_with_fulltext': papers_with_fulltext,
            'embedding_table_count': embedding_table_count,
            'recent_papers': recent_papers
        }

    except Exception as e:
        print(f"PostgreSQL 연결 오류: {e}")
        return None

def check_hbase_status():
    """HBase 유사도 데이터 상태 확인"""
    print("\n" + "=" * 60)
    print("2. HBase 유사도 데이터 상태 확인")
    print("=" * 60)

    try:
        # HBase REST API를 통한 테이블 스캔
        hbase_url = "http://localhost:8080"

        # paper_similarities 테이블 존재 확인
        tables_url = f"{hbase_url}/tables"
        response = requests.get(tables_url, headers={'Accept': 'application/json'})

        if response.status_code == 200:
            tables = response.json().get('tables', [])
            similarity_table_exists = 'paper_similarities' in [table['name'] for table in tables]
            print(f"paper_similarities 테이블 존재: {similarity_table_exists}")

            if similarity_table_exists:
                # 테이블 행 수 확인 (샘플링)
                scan_url = f"{hbase_url}/paper_similarities/*"
                scan_response = requests.get(scan_url, headers={'Accept': 'application/json'})

                if scan_response.status_code == 200:
                    scan_data = scan_response.json()
                    row_count = len(scan_data.get('Row', []))
                    print(f"유사도 데이터 행 수 (샘플): {row_count}")
                else:
                    print("유사도 데이터 스캔 실패")

        else:
            print(f"HBase REST API 연결 실패: {response.status_code}")

    except Exception as e:
        print(f"HBase 연결 오류: {e}")

def check_opensearch_status():
    """OpenSearch 인덱스 상태 확인"""
    print("\n" + "=" * 60)
    print("3. OpenSearch 인덱스 상태 확인")
    print("=" * 60)

    try:
        opensearch_url = "http://localhost:9200"

        # 클러스터 상태 확인
        health_response = requests.get(f"{opensearch_url}/_cluster/health")
        if health_response.status_code == 200:
            health_data = health_response.json()
            print(f"클러스터 상태: {health_data.get('status', 'unknown')}")
            print(f"노드 수: {health_data.get('number_of_nodes', 0)}")

        # 인덱스 목록 확인
        indices_response = requests.get(f"{opensearch_url}/_cat/indices?format=json")
        if indices_response.status_code == 200:
            indices = indices_response.json()
            paper_indices = [idx for idx in indices if 'paper' in idx.get('index', '')]

            print(f"\n논문 관련 인덱스:")
            for idx in paper_indices:
                index_name = idx.get('index', '')
                doc_count = idx.get('docs.count', '0')
                print(f"  {index_name}: {doc_count} 문서")

        # papers 인덱스 상세 정보
        try:
            papers_stats = requests.get(f"{opensearch_url}/papers/_stats")
            if papers_stats.status_code == 200:
                stats_data = papers_stats.json()
                total_docs = stats_data['_all']['total']['docs']['count']
                print(f"\npapers 인덱스 총 문서 수: {total_docs:,}")
        except:
            print("papers 인덱스 통계 조회 실패")

    except Exception as e:
        print(f"OpenSearch 연결 오류: {e}")

def check_grobid_service():
    """GROBID 서비스 상태 확인"""
    print("\n" + "=" * 60)
    print("4. GROBID 서비스 상태 확인")
    print("=" * 60)

    try:
        grobid_url = "http://localhost:8070"

        # GROBID 서비스 상태 확인
        response = requests.get(f"{grobid_url}/api/isalive", timeout=5)
        if response.status_code == 200:
            print("GROBID 서비스: 정상 작동")

            # 버전 정보 확인
            version_response = requests.get(f"{grobid_url}/api/version", timeout=5)
            if version_response.status_code == 200:
                print(f"GROBID 버전: {version_response.text.strip()}")
        else:
            print(f"GROBID 서비스 상태 불량: {response.status_code}")

    except Exception as e:
        print(f"GROBID 서비스 연결 오류: {e}")

def generate_system_report(pg_stats):
    """시스템 종합 평가 보고서 생성"""
    print("\n" + "=" * 60)
    print("5. 시스템 종합 평가")
    print("=" * 60)

    if not pg_stats:
        print("❌ 데이터베이스 연결 실패로 평가 불가")
        return

    total_papers = pg_stats['total_papers']

    if total_papers == 0:
        print("❌ 논문 데이터가 없습니다.")
        return

    print(f"📊 총 {total_papers:,}개의 논문이 시스템에 등록되어 있습니다.\n")

    # 각 단계별 완료도 평가
    embedding_ratio = (pg_stats['papers_with_embeddings'] / total_papers) * 100
    grobid_ratio = (pg_stats['grobid_processed'] / total_papers) * 100
    keyword_ratio = (pg_stats['papers_with_keywords'] / total_papers) * 100

    print("📈 처리 단계별 완료도:")
    print(f"  🔍 임베딩 생성: {embedding_ratio:.1f}% {'✅' if embedding_ratio > 80 else '⚠️' if embedding_ratio > 50 else '❌'}")
    print(f"  📄 GROBID 처리: {grobid_ratio:.1f}% {'✅' if grobid_ratio > 80 else '⚠️' if grobid_ratio > 50 else '❌'}")
    print(f"  🏷️  키워드 추출: {keyword_ratio:.1f}% {'✅' if keyword_ratio > 80 else '⚠️' if keyword_ratio > 50 else '❌'}")

    # 시스템 상태 평가
    if embedding_ratio > 80 and grobid_ratio > 80:
        print(f"\n🎉 시스템이 정상적으로 작동하고 있습니다!")
        print("   - 논문 추천 기능을 사용할 수 있습니다.")
    elif embedding_ratio > 50 or grobid_ratio > 50:
        print(f"\n⚠️  시스템이 부분적으로 작동하고 있습니다.")
        print("   - 일부 논문에 대해서만 추천이 가능합니다.")
    else:
        print(f"\n❌ 시스템 초기화가 필요합니다.")
        print("   - 임베딩 생성 및 GROBID 처리를 먼저 수행해주세요.")

    # 최근 활동 평가
    if pg_stats['recent_papers'] > 0:
        print(f"\n📅 최근 7일간 {pg_stats['recent_papers']:,}개의 새로운 논문이 추가되었습니다.")

def main():
    print("RSP 논문 추천 시스템 상태 점검을 시작합니다...")
    print(f"점검 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. PostgreSQL 상태 확인
    pg_stats = check_postgresql_status()

    # 2. HBase 상태 확인
    check_hbase_status()

    # 3. OpenSearch 상태 확인
    check_opensearch_status()

    # 4. GROBID 서비스 확인
    check_grobid_service()

    # 5. 종합 평가
    generate_system_report(pg_stats)

if __name__ == "__main__":
    main()