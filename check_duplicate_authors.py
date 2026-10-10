#!/usr/bin/env python3
"""
중복 저자 확인 스크립트
"""

import os
import psycopg2
import psycopg2.extras

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

def check_duplicate_authors():
    """중복 저자 확인"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                # 1. 같은 이름으로 중복된 저자들 확인
                print("🔍 같은 이름으로 중복된 저자들:")
                cursor.execute("""
                    SELECT name, COUNT(*) as count,
                           array_agg(id) as ids,
                           array_agg(openalex_author_id) as openalex_ids
                    FROM authors
                    GROUP BY name
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                    LIMIT 20
                """)

                duplicates = cursor.fetchall()
                if duplicates:
                    for dup in duplicates:
                        print(f"  📝 {dup['name']}: {dup['count']}개")
                        print(f"     IDs: {dup['ids']}")
                        print(f"     OpenAlex IDs: {dup['openalex_ids']}")
                        print()
                else:
                    print("  ✅ 중복된 저자 이름 없음")

                # 2. 전체 저자 통계
                cursor.execute("SELECT COUNT(*) FROM authors")
                total_authors = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT name) FROM authors")
                unique_names = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT openalex_author_id) FROM authors WHERE openalex_author_id IS NOT NULL")
                unique_openalex_ids = cursor.fetchone()[0]

                print(f"📊 저자 통계:")
                print(f"  전체 저자: {total_authors}명")
                print(f"  고유 이름: {unique_names}개")
                print(f"  고유 OpenAlex ID: {unique_openalex_ids}개")
                print(f"  중복률: {((total_authors - unique_names) / total_authors * 100):.1f}%")

                # 3. OpenAlex ID 없는 저자들
                cursor.execute("SELECT COUNT(*) FROM authors WHERE openalex_author_id IS NULL")
                no_openalex_id = cursor.fetchone()[0]
                print(f"  OpenAlex ID 없음: {no_openalex_id}명")

                # 4. 샘플 저자들 확인
                print(f"\n📋 최근 저자 샘플:")
                cursor.execute("""
                    SELECT id, name, openalex_author_id, created_at
                    FROM authors
                    ORDER BY created_at DESC
                    LIMIT 10
                """)

                samples = cursor.fetchall()
                for sample in samples:
                    print(f"  ID:{sample['id']} {sample['name']} | OpenAlex:{sample['openalex_author_id']}")

    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    check_duplicate_authors()