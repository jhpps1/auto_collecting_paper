#!/usr/bin/env python3
"""
무한 임베딩 파이프라인

논문 수집 → 임베딩 생성 → 유사도 계산 → HBase 저장을 반복
"""

import time
import sys
import subprocess
import psycopg2
import os
from datetime import datetime

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

class InfinitePipeline:
    def __init__(self):
        self.iteration = 0
        self.target_papers_per_batch = 50  # 배치당 수집할 논문 수
        self.current_page = 1  # 현재 페이지 번호 추적 (무한 증가)

    def get_current_stats(self):
        """현재 시스템 상태 확인"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    # 총 논문 수
                    cursor.execute("SELECT COUNT(*) FROM papers")
                    total_papers = cursor.fetchone()[0]

                    # 임베딩 생성된 논문 수
                    cursor.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL")
                    papers_with_embeddings = cursor.fetchone()[0]

                    return {
                        'total_papers': total_papers,
                        'papers_with_embeddings': papers_with_embeddings,
                        'embedding_coverage': (papers_with_embeddings / total_papers * 100) if total_papers > 0 else 0
                    }
        except Exception as e:
            print(f"❌ 상태 확인 실패: {e}")
            return None

    def collect_papers(self):
        """새로운 논문 수집"""
        try:
            print(f"📥 새로운 논문 수집 중... (목표: {self.target_papers_per_batch}개)")

            # 다양한 검색 키워드 사용
            keywords = [
                "machine learning", "artificial intelligence", "computer vision",
                "natural language processing", "deep learning", "neural networks",
                "data mining", "robotics", "algorithm", "software engineering"
            ]

            keyword = keywords[self.iteration % len(keywords)]
            print(f"   검색 키워드: '{keyword}' (페이지: {self.current_page})")

            # OpenAlex 수집기 실행 (페이지 번호 추가)
            result = subprocess.run([
                'python3', 'full_openalex_collector.py',
                '--query', keyword,
                '--count', str(self.target_papers_per_batch),
                '--page', str(self.current_page)
            ], capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                print(f"✅ 논문 수집 완료")
                return True
            else:
                print(f"❌ 논문 수집 실패: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print("⏰ 논문 수집 타임아웃 (5분)")
            return False
        except Exception as e:
            print(f"❌ 논문 수집 오류: {e}")
            return False

    def generate_embeddings(self):
        """임베딩 생성"""
        try:
            print("🔄 임베딩 생성 중...")

            # 임베딩 생성기 실행
            result = subprocess.run([
                'python3', 'embedding_generator.py'
            ], capture_output=True, text=True, timeout=600)

            if result.returncode == 0:
                print("✅ 임베딩 생성 완료")
                return True
            else:
                print(f"❌ 임베딩 생성 실패: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print("⏰ 임베딩 생성 타임아웃 (10분)")
            return False
        except Exception as e:
            print(f"❌ 임베딩 생성 오류: {e}")
            return False

    def calculate_similarities(self):
        """유사도 계산 및 HBase 저장"""
        try:
            print("🔢 유사도 계산 중... (Top-K 30)")

            # 유사도 계산기 실행
            result = subprocess.run([
                'python3', 'simple_similarity.py'
            ], capture_output=True, text=True, timeout=1800)

            if result.returncode == 0:
                print("✅ 유사도 계산 및 HBase 저장 완료")
                return True
            else:
                print(f"❌ 유사도 계산 실패: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print("⏰ 유사도 계산 타임아웃 (30분)")
            return False
        except Exception as e:
            print(f"❌ 유사도 계산 오류: {e}")
            return False

    def run_iteration(self):
        """한 번의 파이프라인 실행"""
        self.iteration += 1

        print("\n" + "="*80)
        print(f"🚀 파이프라인 반복 #{self.iteration} 시작")
        print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # 현재 상태 확인
        stats = self.get_current_stats()
        if stats:
            print(f"📊 현재 상태:")
            print(f"   총 논문: {stats['total_papers']:,}개")
            print(f"   임베딩 생성: {stats['papers_with_embeddings']:,}개 ({stats['embedding_coverage']:.1f}%)")

        success_count = 0

        # 1. 논문 수집
        if self.collect_papers():
            success_count += 1
            # 페이지 번호 무한 증가 (계속해서 새로운 논문 수집)
            self.current_page += 1

        # 2. 임베딩 생성
        if self.generate_embeddings():
            success_count += 1

        # 3. 유사도 계산 (논문이 충분할 때만)
        if stats and stats['total_papers'] >= 10:
            if self.calculate_similarities():
                success_count += 1
        else:
            print("ℹ️ 논문 수가 부족하여 유사도 계산 건너뜀")
            success_count += 1  # 건너뛴 것도 성공으로 처리

        # 결과 출력
        final_stats = self.get_current_stats()
        if final_stats:
            print(f"\n📈 반복 #{self.iteration} 결과:")
            print(f"   총 논문: {final_stats['total_papers']:,}개")
            print(f"   임베딩 생성: {final_stats['papers_with_embeddings']:,}개 ({final_stats['embedding_coverage']:.1f}%)")
            if stats:
                new_papers = final_stats['total_papers'] - stats['total_papers']
                new_embeddings = final_stats['papers_with_embeddings'] - stats['papers_with_embeddings']
                print(f"   이번 반복 추가: 논문 {new_papers}개, 임베딩 {new_embeddings}개")

        print(f"⏰ 종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 성공한 단계: {success_count}/3")

        return success_count >= 2  # 최소 2단계는 성공해야 함

    def run_infinite(self):
        """무한 파이프라인 실행"""
        print("🌟 무한 임베딩 파이프라인 시작!")
        print("   중단하려면 Ctrl+C를 누르세요.")

        consecutive_failures = 0
        max_failures = 3

        try:
            while True:
                try:
                    success = self.run_iteration()

                    if success:
                        consecutive_failures = 0
                        # 성공 시 잠시 대기 (시스템 부하 방지)
                        print(f"\n💤 다음 반복까지 60초 대기...")
                        time.sleep(60)
                    else:
                        consecutive_failures += 1
                        print(f"⚠️ 연속 실패 횟수: {consecutive_failures}/{max_failures}")

                        if consecutive_failures >= max_failures:
                            print(f"❌ 연속 {max_failures}회 실패로 파이프라인 중단")
                            break

                        # 실패 시 더 긴 대기
                        print(f"\n💤 실패 후 180초 대기...")
                        time.sleep(180)

                except KeyboardInterrupt:
                    print("\n\n🛑 사용자가 파이프라인을 중단했습니다.")
                    break

                except Exception as e:
                    print(f"\n❌ 예상치 못한 오류: {e}")
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        break
                    time.sleep(180)

        finally:
            print("\n🏁 무한 파이프라인 종료")
            final_stats = self.get_current_stats()
            if final_stats:
                print(f"📊 최종 상태:")
                print(f"   총 논문: {final_stats['total_papers']:,}개")
                print(f"   임베딩 생성: {final_stats['papers_with_embeddings']:,}개 ({final_stats['embedding_coverage']:.1f}%)")

def main():
    """메인 실행 함수"""
    pipeline = InfinitePipeline()
    pipeline.run_infinite()

if __name__ == "__main__":
    main()