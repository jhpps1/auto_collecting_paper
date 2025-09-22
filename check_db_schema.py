#!/usr/bin/env python3
import psycopg2

def check_database_schema():
    """데이터베이스 스키마 확인"""
    passwords = ["ssafy", "postgres123", "postgres", "SecurePostgres123!", ""]
    databases = ["rsp_db", "papers_db", "postgres"]

    conn = None
    for db in databases:
        for password in passwords:
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    database=db,
                    user="postgres",
                    password=password
                )
                print(f"연결된 데이터베이스: {db}")
                break
            except Exception as e:
                continue
        if conn:
            break

    if not conn:
        print("데이터베이스 연결 실패")
        return

    cursor = conn.cursor()

    # 테이블 목록 확인
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    print(f"테이블 목록: {[table[0] for table in tables]}")

    # papers 테이블 스키마 확인
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'papers'
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    print("\npapers 테이블 컬럼:")
    for col in columns:
        print(f"  {col[0]} ({col[1]}) - NULL 허용: {col[2]}")

    # 데이터 샘플 확인
    cursor.execute("SELECT * FROM papers LIMIT 3")
    sample_data = cursor.fetchall()
    print(f"\n샘플 데이터 (첫 3행):")
    for row in sample_data:
        print(f"  {row}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_database_schema()