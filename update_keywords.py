#!/usr/bin/env python3
"""
기존 논문들의 keywords를 OpenAlex API에서 다시 가져와서 업데이트
"""

import psycopg2
import requests
import json
import time
from typing import List, Dict, Any

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'papers_db',
    'user': 'postgres',
    'password': 'postgres123'
}

def extract_keywords_from_paper(paper: Dict[str, Any]) -> List[str]:
    """OpenAlex 논문 데이터에서 keywords 추출"""
    keywords_list = []

    # 1. keywords 필드
    if paper.get('keywords'):
        keywords_list.extend([kw.get('display_name', kw) for kw in paper['keywords'] if kw])

    # 2. concepts에서 높은 점수 항목들 추출 (score > 0.5)
    if paper.get('concepts'):
        for concept in paper['concepts'][:10]:  # 최대 10개
            if concept.get('score', 0) > 0.5:
                keywords_list.append(concept.get('display_name', ''))

    # 3. topics에서 추출 (최대 3개)
    if paper.get('topics'):
        for topic in paper['topics'][:3]:
            if topic.get('display_name'):
                keywords_list.append(topic['display_name'])

    # 중복 제거하고 리스트로 변환
    keywords_list = list(set([kw for kw in keywords_list if kw]))[:20]  # 최대 20개
    return keywords_list

def update_keywords():
    """keywords가 없는 논문들을 업데이트"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # keywords가 없거나 비어있는 논문들 조회
                cursor.execute("""
                    SELECT id, openalex_paper_id, title
                    FROM papers
                    WHERE (keywords IS NULL OR keywords = '{}')
                    AND openalex_paper_id IS NOT NULL
                    ORDER BY id
                """)

                papers = cursor.fetchall()
                print(f"📋 업데이트할 논문: {len(papers)}개")

                updated_count = 0
                failed_count = 0

                for paper_id, openalex_id, title in papers:
                    print(f"\n논문 {paper_id}: {title[:50]}...")

                    try:
                        # OpenAlex API 호출
                        url = f"https://api.openalex.org/works/{openalex_id}"
                        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})

                        if response.status_code == 200:
                            paper_data = response.json()
                            keywords = extract_keywords_from_paper(paper_data)

                            if keywords:
                                # keywords 업데이트 (jsonb 타입으로 변환)
                                import json
                                cursor.execute("""
                                    UPDATE papers
                                    SET keywords = %s::jsonb, updated_at = NOW()
                                    WHERE id = %s
                                """, (json.dumps(keywords), paper_id))

                                updated_count += 1
                                print(f"  ✅ Keywords 업데이트: {len(keywords)}개 - {keywords[:5]}...")
                            else:
                                print(f"  ⚠️ Keywords 없음")
                        else:
                            print(f"  ❌ API 오류: {response.status_code}")
                            failed_count += 1

                        # Rate limit 고려
                        time.sleep(0.5)

                    except Exception as e:
                        print(f"  ❌ 처리 실패: {e}")
                        failed_count += 1
                        continue

                conn.commit()

                print(f"\n✨ 업데이트 완료:")
                print(f"  성공: {updated_count}개")
                print(f"  실패: {failed_count}개")

                # 최종 통계
                cursor.execute("""
                    SELECT COUNT(*) as total,
                           COUNT(CASE WHEN keywords IS NOT NULL AND keywords != '{}' THEN 1 END) as with_keywords
                    FROM papers
                """)
                result = cursor.fetchone()
                print(f"\n📊 전체 논문: {result[0]}개")
                print(f"   Keywords 있음: {result[1]}개 ({result[1]/result[0]*100:.1f}%)")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    update_keywords()