#!/usr/bin/env python3
import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'papers_db',
    'user': 'postgres',
    'password': 'postgres123'
}

try:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM papers")
            count = cursor.fetchone()[0]
            print(f"실제 papers 테이블 논문 수: {count}개")

            cursor.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL")
            embedding_count = cursor.fetchone()[0]
            print(f"임베딩 있는 논문: {embedding_count}개")

            cursor.execute("SELECT MAX(id), MIN(id) FROM papers")
            max_id, min_id = cursor.fetchone()
            print(f"논문 ID 범위: {min_id} ~ {max_id}")

except Exception as e:
    print(f"오류: {e}")