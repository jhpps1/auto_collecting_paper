#!/usr/bin/env python3
"""
OpenAlex API로 논문 메타데이터 수집하고 모든 관련 테이블에 저장
(papers, authors, concepts, journals, paper_authors, paper_concepts)
"""

import requests
import psycopg2
import psycopg2.extras
import json
import os
from datetime import datetime
import time

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'papers_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres123')
}

class FullOpenAlexCollector:
    def __init__(self):
        self.base_url = "https://api.openalex.org/works"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (RSP-Paper-System/1.0; mailto:test@example.com)'
        }

    def fetch_papers(self, query="machine learning", count=10):
        """OpenAlex API에서 Computer Science 분야 논문 데이터 가져오기"""

        # ===================================================
        # 🎯 팀원별 Concept 분담 수집 시스템
        # ===================================================
        # 각 팀원은 아래 5개 concept 중 하나를 담당합니다.
        # 담당자는 해당 라인의 주석(#)을 제거하고 실행하세요.
        # ===================================================

        # 👤 수진: Machine Learning 담당
        # assigned_concept = 'C78519656'  # Machine Learning (수진이 이 줄 주석 해제)

        # 👤 승연: Artificial Intelligence 담당
        # assigned_concept = 'C119857082'  # Artificial Intelligence (승연이 이 줄 주석 해제)

        # 👤 승균: Computer Vision 담당
        # assigned_concept = 'C162324750'  # Computer Vision (승균이 이 줄 주석 해제)

        # 👤 경찬: Natural Language Processing 담당
        # assigned_concept = 'C2779118'   # Natural Language Processing (경찬이 이 줄 주석 해제)

        # 👤 민: Computer Science 전반 담당
        # assigned_concept = 'C41008148'  # Computer Science (민이 이 줄 주석 해제)

        # ===================================================
        # ⚠️  주의: 위에서 정확히 하나만 주석 해제하세요!
        # ===================================================

        # 기본값 설정 (아무것도 주석 해제 안 했을 때)
        try:
            concept_filter = assigned_concept
        except NameError:
            concept_filter = 'C41008148'  # 기본값: Computer Science
            print("⚠️  경고: concept가 설정되지 않았습니다. 기본값 사용 중...")
            print("   담당 concept 라인의 주석(#)을 제거하고 다시 실행하세요.")

        # Concept별 설명 매핑
        concept_names = {
            'C78519656': 'Machine Learning (머신러닝)',
            'C119857082': 'Artificial Intelligence (인공지능)',
            'C162324750': 'Computer Vision (컴퓨터 비전)',
            'C2779118': 'Natural Language Processing (자연어 처리)',
            'C41008148': 'Computer Science (컴퓨터 과학 전반)'
        }

        print(f"🎯 수집 대상: {concept_names.get(concept_filter, concept_filter)}")
        print(f"📊 수집 개수: {count}개 논문")
        print("-" * 60)

        # ===================================================
        # 🎯 팀원별 Concept 분야 필터링만 적용
        # (해당 분야의 모든 논문을 수집)
        # ===================================================
        filter_string = f'concepts.id:{concept_filter}'

        params = {
            'search': query,
            'filter': filter_string,
            'per-page': min(count, 25),  # API 제한
            'sort': 'cited_by_count:desc'
        }

        try:
            print(f"🔍 OpenAlex API 요청: '{query}', {count}개 논문...")
            response = requests.get(self.base_url, params=params, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            papers = data.get('results', [])

            print(f"✅ {len(papers)}개 논문 수집 완료")
            return papers

        except Exception as e:
            print(f"❌ OpenAlex API 요청 실패: {e}")
            return []

    def save_journal(self, host_venue, cursor):
        """저널 정보 저장하고 journal_id 반환"""
        if not host_venue or not host_venue.get('id'):
            return None

        try:
            openalex_source_id = host_venue.get('id', '').replace('https://openalex.org/', '')
            name = host_venue.get('display_name', '').strip()

            if not name:
                return None

            # 저널 정보 추출
            issn_data = host_venue.get('issn', [])
            issn_l = host_venue.get('issn_l')
            is_oa = host_venue.get('is_oa', False)
            source_type = host_venue.get('type', 'journal')

            insert_journal_sql = """
            INSERT INTO journals (
                openalex_source_id, name, issn_l, issn, is_oa, source_type, created_at
            ) VALUES (
                %(openalex_source_id)s, %(name)s, %(issn_l)s, %(issn)s, %(is_oa)s, %(source_type)s, NOW()
            ) ON CONFLICT (openalex_source_id) DO UPDATE SET
                name = EXCLUDED.name,
                issn_l = EXCLUDED.issn_l,
                issn = EXCLUDED.issn,
                is_oa = EXCLUDED.is_oa,
                updated_at = NOW()
            RETURNING id
            """

            cursor.execute(insert_journal_sql, {
                'openalex_source_id': openalex_source_id,
                'name': name,
                'issn_l': issn_l,
                'issn': json.dumps(issn_data) if issn_data else None,
                'is_oa': is_oa,
                'source_type': source_type
            })

            journal_id = cursor.fetchone()['id']
            print(f"  📓 저널 저장: {name}")
            return journal_id

        except Exception as e:
            print(f"❌ 저널 저장 실패: {e}")
            return None

    def save_author(self, author_data, cursor):
        """저자 정보 저장하고 author_id 반환 (이름 기준 중복 방지)"""
        if not author_data or not author_data.get('display_name'):
            return None

        try:
            openalex_author_id = author_data.get('id', '').replace('https://openalex.org/', '')
            name = author_data.get('display_name', '').strip()
            orcid = author_data.get('orcid', '').replace('https://orcid.org/', '') if author_data.get('orcid') else None

            # 1. 먼저 같은 이름의 저자가 있는지 확인
            cursor.execute("SELECT id FROM authors WHERE name = %s LIMIT 1", (name,))
            existing_author = cursor.fetchone()

            if existing_author:
                # 이미 같은 이름의 저자가 있으면 기존 ID 반환
                return existing_author['id']

            # 2. 새로운 저자 저장 (OpenAlex ID 기준 중복 방지)
            insert_author_sql = """
            INSERT INTO authors (
                openalex_author_id, name, orcid, created_at
            ) VALUES (
                %(openalex_author_id)s, %(name)s, %(orcid)s, NOW()
            ) ON CONFLICT (openalex_author_id) DO UPDATE SET
                name = EXCLUDED.name,
                orcid = EXCLUDED.orcid,
                updated_at = NOW()
            RETURNING id
            """

            cursor.execute(insert_author_sql, {
                'openalex_author_id': openalex_author_id,
                'name': name,
                'orcid': orcid
            })

            author_id = cursor.fetchone()['id']
            return author_id

        except Exception as e:
            print(f"❌ 저자 저장 실패: {e}")
            return None

    def save_concept(self, concept_data, cursor):
        """개념 정보 저장하고 concept_id 반환"""
        if not concept_data or not concept_data.get('display_name'):
            return None

        try:
            openalex_concept_id = concept_data.get('id', '').replace('https://openalex.org/', '')
            name = concept_data.get('display_name', '').strip()
            level = concept_data.get('level', 0)
            works_count = concept_data.get('works_count', 0)

            insert_concept_sql = """
            INSERT INTO concepts (
                openalex_concept_id, name, level, works_count, created_at
            ) VALUES (
                %(openalex_concept_id)s, %(name)s, %(level)s, %(works_count)s, NOW()
            ) ON CONFLICT (openalex_concept_id) DO UPDATE SET
                name = EXCLUDED.name,
                level = EXCLUDED.level,
                works_count = EXCLUDED.works_count,
                updated_at = NOW()
            RETURNING id
            """

            cursor.execute(insert_concept_sql, {
                'openalex_concept_id': openalex_concept_id,
                'name': name,
                'level': level,
                'works_count': works_count
            })

            concept_id = cursor.fetchone()['id']
            return concept_id

        except Exception as e:
            print(f"❌ 개념 저장 실패: {e}")
            return None

    def find_journal_by_issn_or_name(self, location_data, journal_name, cursor):
        """ISSN 우선, 저널명 차순으로 저널 매칭"""
        try:
            # 1순위: ISSN-L로 매칭
            if location_data and location_data.get('source'):
                source = location_data['source']
                issn_l = source.get('issn_l')
                if issn_l:
                    cursor.execute(
                        "SELECT id FROM journals WHERE issn_l = %s LIMIT 1",
                        (issn_l,)
                    )
                    result = cursor.fetchone()
                    if result:
                        print(f"    ✅ ISSN-L 매칭: {issn_l}")
                        return result['id']

                # 2순위: ISSN 배열에서 하나라도 매칭
                issn_list = source.get('issn')
                if issn_list:
                    for issn in issn_list:
                        cursor.execute(
                            "SELECT id FROM journals WHERE issn ? %s LIMIT 1",
                            (issn,)
                        )
                        result = cursor.fetchone()
                        if result:
                            print(f"    ✅ ISSN 매칭: {issn}")
                            return result['id']

                # 3순위: OpenAlex Source ID 매칭
                openalex_source_id = source.get('id', '').replace('https://openalex.org/', '')
                if openalex_source_id:
                    cursor.execute(
                        "SELECT id FROM journals WHERE openalex_source_id = %s LIMIT 1",
                        (openalex_source_id,)
                    )
                    result = cursor.fetchone()
                    if result:
                        print(f"    ✅ OpenAlex ID 매칭: {openalex_source_id}")
                        return result['id']

            # 4순위: 저널명 정확 매칭
            if journal_name:
                cursor.execute(
                    "SELECT id FROM journals WHERE name ILIKE %s LIMIT 1",
                    (journal_name,)
                )
                result = cursor.fetchone()
                if result:
                    print(f"    ✅ 저널명 정확 매칭: {journal_name}")
                    return result['id']

                # 5순위: 저널명 부분 매칭
                cursor.execute(
                    "SELECT id FROM journals WHERE name ILIKE %s LIMIT 1",
                    (f"%{journal_name}%",)
                )
                result = cursor.fetchone()
                if result:
                    print(f"    ⚠️ 저널명 부분 매칭: {journal_name}")
                    return result['id']

            return None

        except Exception as e:
            print(f"❌ 저널 매칭 오류: {e}")
            return None

    def save_paper_complete(self, paper):
        """논문과 모든 관련 데이터를 완전히 저장"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # 1. 저널 정보 저장
                    journal_id = None
                    host_venue = paper.get('host_venue')
                    if host_venue:
                        journal_id = self.save_journal(host_venue, cursor)

                    # 2. 논문 기본 정보 저장
                    openalex_id = paper.get('id', '').replace('https://openalex.org/', '')
                    title = (paper.get('title') or '').strip()
                    abstract = paper.get('abstract_inverted_index')

                    # Abstract 복원 (OpenAlex는 inverted index로 제공)
                    abstract_text = self.reconstruct_abstract(abstract) if abstract else ""

                    doi = paper.get('doi', '').replace('https://doi.org/', '') if paper.get('doi') else None

                    # PDF URL 추출
                    pdf_url = None
                    open_access = paper.get('open_access', {})
                    if open_access.get('oa_url'):
                        pdf_url = open_access['oa_url']

                    citation_count = paper.get('cited_by_count', 0)
                    publication_date = paper.get('publication_date')
                    is_open_access = open_access.get('is_oa', False)

                    # 저널 정보 - primary_location에서 추출
                    journal_id = None
                    journal_name = None
                    publisher = None

                    # 1. primary_location에서 저널 찾기
                    primary_location = paper.get('primary_location')
                    if primary_location and primary_location.get('source'):
                        source = primary_location['source']
                        if source.get('type') == 'journal':
                            journal_name = source.get('display_name')
                            publisher = source.get('host_organization_name')

                    # 2. primary_location에 없으면 locations에서 저널 타입 찾기
                    if not journal_name:
                        locations = paper.get('locations', [])
                        for location in locations:
                            source = location.get('source')
                            if source and source.get('type') == 'journal':
                                journal_name = source.get('display_name')
                                publisher = source.get('host_organization_name')
                                break

                    # 3. 기존 저널 테이블에서 매칭되는 저널 찾기
                    if journal_name:
                        journal_id = self.find_journal_by_issn_or_name(primary_location, journal_name, cursor)
                        if journal_id:
                            print(f"  📓 저널 매칭 성공: {journal_name} -> ID {journal_id}")
                        else:
                            print(f"  ⚠️ 저널 매칭 실패: {journal_name}")
                    else:
                        print(f"  ❌ 저널 정보 없음 (locations에서도 저널 타입 발견 못함)")

                    # 논문 타입과 키워드 정보 추출
                    paper_type = paper.get('type', 'article')

                    # 키워드 정보 추출 (keywords, concepts, topics 활용)
                    keywords_list = []

                    # 1. keywords 필드 (보통 비어있지만 있으면 활용)
                    if paper.get('keywords'):
                        for kw in paper['keywords']:
                            if isinstance(kw, dict) and kw.get('display_name'):
                                keywords_list.append(kw['display_name'])
                            elif isinstance(kw, str):
                                keywords_list.append(kw)

                    # 2. concepts에서 높은 점수 항목들 추출 (score > 0.5)
                    if paper.get('concepts'):
                        for concept in paper['concepts'][:10]:  # 최대 10개
                            if isinstance(concept, dict) and concept.get('score', 0) > 0.5:
                                name = concept.get('display_name', '')
                                if name and name not in keywords_list:  # 중복 방지
                                    keywords_list.append(name)

                    # 3. topics에서 추출 (최대 3개)
                    if paper.get('topics'):
                        for topic in paper['topics'][:3]:
                            if isinstance(topic, dict) and topic.get('display_name'):
                                name = topic['display_name']
                                if name and name not in keywords_list:  # 중복 방지
                                    keywords_list.append(name)

                    # 중복 제거하고 리스트로 저장 (PostgreSQL array 타입으로 저장)
                    # 빈 문자열 제거 및 최대 20개 제한
                    keywords_list = [kw.strip() for kw in keywords_list if kw and kw.strip()]
                    keywords_list = list(dict.fromkeys(keywords_list))[:20]  # 순서 유지하며 중복 제거
                    keywords = keywords_list if keywords_list else None

                    insert_paper_sql = """
                    INSERT INTO papers (
                        openalex_paper_id, title, abstract_text, doi, pdf_url,
                        citation_count, publication_date, is_open_access,
                        publisher, journal_id, type, keywords, created_at
                    ) VALUES (
                        %(openalex_paper_id)s, %(title)s, %(abstract_text)s, %(doi)s, %(pdf_url)s,
                        %(citation_count)s, %(publication_date)s, %(is_open_access)s,
                        %(publisher)s, %(journal_id)s, %(type)s, %(keywords)s, NOW()
                    ) ON CONFLICT (openalex_paper_id) DO UPDATE SET
                        citation_count = EXCLUDED.citation_count,
                        keywords = EXCLUDED.keywords,
                        updated_at = NOW()
                    RETURNING id
                    """

                    cursor.execute(insert_paper_sql, {
                        'openalex_paper_id': openalex_id,
                        'title': title,
                        'abstract_text': abstract_text,
                        'doi': doi,
                        'pdf_url': pdf_url,
                        'citation_count': citation_count,
                        'publication_date': publication_date,
                        'is_open_access': is_open_access,
                        'publisher': publisher,
                        'journal_id': journal_id,
                        'type': paper_type,
                        'keywords': json.dumps(keywords) if keywords else None
                    })

                    paper_id = cursor.fetchone()['id']
                    print(f"✅ 논문 저장: ID {paper_id}, '{title[:30]}...'")

                    # 3. 저자 정보 저장 및 관계 설정
                    authorships = paper.get('authorships', [])
                    for idx, authorship in enumerate(authorships):
                        author_info = authorship.get('author', {})
                        if author_info:
                            author_id = self.save_author(author_info, cursor)
                            if author_id:
                                # 소속기관 정보
                                institutions = authorship.get('institutions', [])
                                affiliation = institutions[0].get('display_name') if institutions else None

                                if affiliation:
                                    # 저자 소속 업데이트
                                    cursor.execute(
                                        "UPDATE authors SET affiliation = %s WHERE id = %s",
                                        (affiliation, author_id)
                                    )

                                # 논문-저자 관계 저장
                                is_corresponding = authorship.get('is_corresponding', False)

                                cursor.execute("""
                                INSERT INTO paper_authors (paper_id, author_id, author_order, is_corresponding)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (paper_id, author_id) DO UPDATE SET
                                    author_order = EXCLUDED.author_order,
                                    is_corresponding = EXCLUDED.is_corresponding
                                """, (paper_id, author_id, idx + 1, is_corresponding))

                    # 4. 개념 정보 저장 및 관계 설정
                    concepts = paper.get('concepts', [])
                    for concept in concepts:
                        concept_id = self.save_concept(concept, cursor)
                        if concept_id:
                            relevance_score = concept.get('score', 0.0)

                            cursor.execute("""
                            INSERT INTO paper_concepts (paper_id, concept_id, relevance_score)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (paper_id, concept_id) DO UPDATE SET
                                relevance_score = EXCLUDED.relevance_score
                            """, (paper_id, concept_id, relevance_score))

                    print(f"  👥 저자 {len(authorships)}명, 🏷️ 개념 {len(concepts)}개 저장 완료")
                    return paper_id

        except Exception as e:
            print(f"❌ 논문 저장 실패 '{paper.get('title', 'Unknown')[:30]}...': {e}")
            return None

    def reconstruct_abstract(self, inverted_index):
        """OpenAlex의 inverted index에서 abstract 복원"""
        if not inverted_index:
            return ""

        try:
            # 단어 위치 매핑
            word_positions = {}
            for word, positions in inverted_index.items():
                for pos in positions:
                    word_positions[pos] = word

            # 위치 순서대로 정렬해서 텍스트 복원
            sorted_positions = sorted(word_positions.keys())
            abstract_words = [word_positions[pos] for pos in sorted_positions]

            return ' '.join(abstract_words)
        except:
            return ""

    def collect_and_save(self, query="machine learning", count=10):
        """전체 수집 및 저장 프로세스"""
        print(f"🚀 OpenAlex 전체 메타데이터 수집 시작")
        print(f"   쿼리: '{query}', 개수: {count}")

        # 1. OpenAlex API에서 논문 데이터 수집
        papers = self.fetch_papers(query=query, count=count)

        if not papers:
            print("❌ 수집된 논문이 없습니다")
            return

        # 2. 각 논문을 모든 관련 테이블에 저장
        saved_count = 0
        for i, paper in enumerate(papers, 1):
            print(f"\n📄 논문 {i}/{len(papers)} 처리 중...")

            paper_id = self.save_paper_complete(paper)
            if paper_id:
                saved_count += 1

            # API 속도 제한 준수
            time.sleep(0.1)

        print(f"\n🎉 수집 완료: {saved_count}/{len(papers)}개 논문 저장됨")

        # 3. 최종 통계 출력
        self.print_db_statistics()

    def print_db_statistics(self):
        """DB 통계 출력"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM papers")
                    papers_count = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM papers WHERE pdf_url IS NOT NULL")
                    papers_with_pdf = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM authors")
                    authors_count = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM concepts")
                    concepts_count = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM journals")
                    journals_count = cursor.fetchone()[0]

                    print(f"\n📊 DB 현황:")
                    print(f"   총 논문: {papers_count}개")
                    print(f"   PDF 있는 논문: {papers_with_pdf}개")
                    print(f"   저자: {authors_count}명")
                    print(f"   개념: {concepts_count}개")
                    print(f"   저널: {journals_count}개")

        except Exception as e:
            print(f"❌ DB 통계 조회 실패: {e}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='OpenAlex 논문 수집기')
    parser.add_argument('--query', default='machine learning neural networks', help='검색 쿼리')
    parser.add_argument('--count', type=int, default=15, help='수집할 논문 수')

    args = parser.parse_args()

    collector = FullOpenAlexCollector()
    collector.collect_and_save(query=args.query, count=args.count)

if __name__ == "__main__":
    main()