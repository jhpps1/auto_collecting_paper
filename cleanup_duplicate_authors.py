#!/usr/bin/env python3
"""
중복 저자 정리 스크립트
같은 이름의 저자들을 하나로 통합하고 관련 데이터 업데이트
"""

import psycopg2
import psycopg2.extras

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'papers_db',
    'user': 'postgres',
    'password': 'postgres123'
}

def cleanup_duplicate_authors():
    """중복 저자 정리"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                print("🔧 중복 저자 정리 시작...")

                # 1. 중복된 저자들 찾기
                cursor.execute("""
                    SELECT name, array_agg(id ORDER BY created_at ASC) as ids
                    FROM authors
                    GROUP BY name
                    HAVING COUNT(*) > 1
                    ORDER BY COUNT(*) DESC
                """)

                duplicates = cursor.fetchall()

                if not duplicates:
                    print("✅ 중복된 저자가 없습니다.")
                    return

                print(f"📋 중복 저자 그룹: {len(duplicates)}개")

                total_merged = 0
                total_deleted = 0

                for dup in duplicates:
                    name = dup['name']
                    ids = dup['ids']
                    keep_id = ids[0]  # 가장 오래된 ID를 유지
                    delete_ids = ids[1:]  # 나머지는 삭제

                    print(f"\n👤 {name}:")
                    print(f"   유지할 ID: {keep_id}")
                    print(f"   삭제할 IDs: {delete_ids}")

                    # 2. paper_authors 테이블의 관계 업데이트
                    for delete_id in delete_ids:
                        # 해당 저자와 관련된 논문-저자 관계를 유지할 저자로 변경
                        cursor.execute("""
                            UPDATE paper_authors
                            SET author_id = %s
                            WHERE author_id = %s
                            AND NOT EXISTS (
                                SELECT 1 FROM paper_authors pa2
                                WHERE pa2.paper_id = paper_authors.paper_id
                                AND pa2.author_id = %s
                            )
                        """, (keep_id, delete_id, keep_id))

                        updated_relations = cursor.rowcount
                        print(f"     ID {delete_id}: {updated_relations}개 논문 관계 이전")

                        # 중복된 관계는 삭제
                        cursor.execute("""
                            DELETE FROM paper_authors
                            WHERE author_id = %s
                        """, (delete_id,))

                    # 3. 중복 저자 삭제
                    cursor.execute("""
                        DELETE FROM authors
                        WHERE id = ANY(%s)
                    """, (delete_ids,))

                    deleted_count = cursor.rowcount
                    total_deleted += deleted_count
                    total_merged += 1

                    print(f"     삭제된 중복 저자: {deleted_count}개")

                # 4. 커밋
                conn.commit()

                print(f"\n✨ 중복 저자 정리 완료:")
                print(f"   통합된 저자 그룹: {total_merged}개")
                print(f"   삭제된 중복 저자: {total_deleted}개")

                # 5. 최종 통계
                cursor.execute("SELECT COUNT(*) FROM authors")
                total_authors = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT name) FROM authors")
                unique_names = cursor.fetchone()[0]

                print(f"\n📊 정리 후 저자 통계:")
                print(f"   전체 저자: {total_authors}명")
                print(f"   고유 이름: {unique_names}개")
                print(f"   중복률: {((total_authors - unique_names) / total_authors * 100):.1f}%")

                # 6. 정리 검증
                cursor.execute("""
                    SELECT name, COUNT(*) as count
                    FROM authors
                    GROUP BY name
                    HAVING COUNT(*) > 1
                """)

                remaining_duplicates = cursor.fetchall()
                if remaining_duplicates:
                    print(f"⚠️ 남은 중복: {len(remaining_duplicates)}개")
                else:
                    print("✅ 모든 중복 저자 정리 완료!")

    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    cleanup_duplicate_authors()