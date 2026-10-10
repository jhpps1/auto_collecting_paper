#!/usr/bin/env python3
"""
PDF 다운로드 및 GROBID 처리 스크립트

저장된 논문들의 PDF를 다운로드하고 GROBID로 텍스트 추출하여 JSON으로 저장
"""

import requests
import psycopg2
import psycopg2.extras
import json
import tempfile
import os
from datetime import datetime
import time
from pathlib import Path

# PostgreSQL 연결 설정
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'paperdb'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password')
}

# GROBID 서비스 설정
GROBID_URL = os.getenv('GROBID_URL', "http://localhost:8070")

class PDFGrobidProcessor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/octet-stream,*/*;q=0.9',
            'Referer': 'https://www.google.com/',
            'Accept-Language': 'en-US,en;q=0.9'
        })

    def download_pdf(self, pdf_url, max_size_mb=50):
        """PDF 다운로드"""
        try:
            print(f"  📥 PDF 다운로드 중: {pdf_url}")

            # HEAD 요청으로 파일 크기 확인
            head_response = self.session.head(pdf_url, timeout=10)
            content_length = head_response.headers.get('content-length')

            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > max_size_mb:
                    print(f"  ⚠️ PDF 파일이 너무 큼: {size_mb:.1f}MB (제한: {max_size_mb}MB)")
                    return None

            # PDF 다운로드
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()

            # Content-Type 확인 (너무 엄격하지 않게)
            content_type = response.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not response.content.startswith(b'%PDF'):
                print(f"  ⚠️ PDF가 아닌 파일: {content_type}")
                return None

            print(f"  ✅ PDF 다운로드 완료: {len(response.content)} bytes")
            return response.content

        except requests.exceptions.Timeout:
            print(f"  ❌ PDF 다운로드 타임아웃: {pdf_url}")
        except requests.exceptions.RequestException as e:
            print(f"  ❌ PDF 다운로드 실패: {e}")
        except Exception as e:
            print(f"  ❌ PDF 다운로드 오류: {e}")

        return None

    def process_with_grobid(self, pdf_content):
        """GROBID로 PDF 처리하여 JSON 구조화 데이터 추출"""
        try:
            print(f"  🔍 GROBID 처리 중...")

            # GROBID fulltext 엔드포인트로 요청
            files = {'input': ('paper.pdf', pdf_content, 'application/pdf')}
            data = {
                'consolidateHeader': '1',
                'consolidateCitations': '1',
                'includeRawCitations': '1',
                'includeRawAffiliations': '1'
            }

            response = self.session.post(
                f"{GROBID_URL}/api/processFulltextDocument",
                files=files,
                data=data,
                timeout=60
            )

            if response.status_code != 200:
                print(f"  ❌ GROBID 처리 실패: HTTP {response.status_code}")
                return None

            # XML 응답을 파싱하여 구조화된 정보 추출
            xml_content = response.text
            structured_data = self.parse_grobid_xml(xml_content)

            print(f"  ✅ GROBID 처리 완료")
            return structured_data

        except requests.exceptions.Timeout:
            print(f"  ❌ GROBID 처리 타임아웃")
        except Exception as e:
            print(f"  ❌ GROBID 처리 오류: {e}")

        return None

    def parse_grobid_xml(self, xml_content):
        """GROBID XML을 파싱하여 구조화된 JSON 데이터 생성"""
        try:
            from xml.etree import ElementTree as ET

            # XML 파싱
            root = ET.fromstring(xml_content)

            # 네임스페이스 처리
            namespaces = {
                'tei': 'http://www.tei-c.org/ns/1.0'
            }

            structured_data = {
                'title': '',
                'abstract': '',
                'full_text': '',
                'authors': [],
                'sections': [],
                'references': [],
                'keywords': [],
                'metadata': {},
                'processing_info': {
                    'processed_at': datetime.now().isoformat(),
                    'grobid_version': 'grobid-0.7.3',
                    'source': 'grobid_fulltext'
                }
            }

            # 제목 추출
            title_elem = root.find('.//tei:titleStmt/tei:title', namespaces)
            if title_elem is not None and title_elem.text:
                structured_data['title'] = title_elem.text.strip()

            # 초록 추출
            abstract_elem = root.find('.//tei:abstract', namespaces)
            if abstract_elem is not None:
                abstract_text = ''.join(abstract_elem.itertext()).strip()
                structured_data['abstract'] = abstract_text

            # 키워드 추출 (저자가 제공한 키워드)
            keywords = []

            # 방법 1: keywords 태그에서 추출
            for keyword_elem in root.findall('.//tei:keywords//tei:term', namespaces):
                if keyword_elem.text:
                    keyword_text = keyword_elem.text.strip()
                    if keyword_text:
                        keywords.append(keyword_text)

            # 방법 2: textClass의 키워드에서도 추출
            for keyword_elem in root.findall('.//tei:textClass//tei:keywords//tei:term', namespaces):
                if keyword_elem.text:
                    keyword_text = keyword_elem.text.strip()
                    if keyword_text and keyword_text not in keywords:
                        keywords.append(keyword_text)

            # 방법 3: 다른 형태의 키워드 태그 확인
            for keyword_elem in root.findall('.//tei:keywords/tei:list/tei:item', namespaces):
                if keyword_elem.text:
                    keyword_text = keyword_elem.text.strip()
                    if keyword_text and keyword_text not in keywords:
                        keywords.append(keyword_text)

            structured_data['keywords'] = keywords

            # 저자 정보 추출
            for author_elem in root.findall('.//tei:author', namespaces):
                author_info = {}

                # 이름 추출
                forename = author_elem.find('.//tei:forename', namespaces)
                surname = author_elem.find('.//tei:surname', namespaces)

                if forename is not None and surname is not None:
                    author_info['name'] = f"{forename.text} {surname.text}".strip()
                elif surname is not None:
                    author_info['name'] = surname.text.strip()

                # 소속 추출
                affiliation = author_elem.find('.//tei:affiliation', namespaces)
                if affiliation is not None:
                    org_name = affiliation.find('.//tei:orgName', namespaces)
                    if org_name is not None:
                        author_info['affiliation'] = org_name.text.strip()

                if author_info.get('name'):
                    structured_data['authors'].append(author_info)

            # 섹션별 텍스트 추출
            full_text_parts = []
            for div_elem in root.findall('.//tei:body//tei:div', namespaces):
                section_data = {}

                # 섹션 제목
                head_elem = div_elem.find('tei:head', namespaces)
                if head_elem is not None and head_elem.text:
                    section_data['title'] = head_elem.text.strip()

                # 섹션 내용
                section_text = []
                for p_elem in div_elem.findall('.//tei:p', namespaces):
                    p_text = ''.join(p_elem.itertext()).strip()
                    if p_text:
                        section_text.append(p_text)

                if section_text:
                    section_content = '\n\n'.join(section_text)
                    section_data['content'] = section_content
                    structured_data['sections'].append(section_data)
                    full_text_parts.append(section_content)

            # 전체 텍스트 조합
            structured_data['full_text'] = '\n\n'.join(full_text_parts)

            # 참고문헌 추출
            for ref_elem in root.findall('.//tei:listBibl/tei:biblStruct', namespaces):
                ref_data = {}

                # 제목
                title = ref_elem.find('.//tei:title[@level="a"]', namespaces)
                if title is not None and title.text:
                    ref_data['title'] = title.text.strip()

                # 저자들
                ref_authors = []
                for ref_author in ref_elem.findall('.//tei:author', namespaces):
                    forename = ref_author.find('tei:forename', namespaces)
                    surname = ref_author.find('tei:surname', namespaces)

                    if surname is not None:
                        if forename is not None:
                            ref_authors.append(f"{forename.text} {surname.text}".strip())
                        else:
                            ref_authors.append(surname.text.strip())

                if ref_authors:
                    ref_data['authors'] = ref_authors

                # 출판 정보
                journal = ref_elem.find('.//tei:title[@level="j"]', namespaces)
                if journal is not None and journal.text:
                    ref_data['journal'] = journal.text.strip()

                date_elem = ref_elem.find('.//tei:date', namespaces)
                if date_elem is not None and date_elem.get('when'):
                    ref_data['year'] = date_elem.get('when')

                if ref_data:
                    structured_data['references'].append(ref_data)

            # 메타데이터
            structured_data['metadata'] = {
                'sections_count': len(structured_data['sections']),
                'references_count': len(structured_data['references']),
                'authors_count': len(structured_data['authors']),
                'keywords_count': len(structured_data['keywords']),
                'text_length': len(structured_data['full_text']),
                'has_abstract': bool(structured_data['abstract'])
            }

            return structured_data

        except ET.ParseError as e:
            print(f"  ❌ XML 파싱 실패: {e}")
            # XML 파싱 실패 시 단순 텍스트로 저장
            return {
                'full_text': xml_content,
                'processing_info': {
                    'processed_at': datetime.now().isoformat(),
                    'source': 'grobid_raw_xml',
                    'parse_error': str(e)
                }
            }
        except Exception as e:
            print(f"  ❌ GROBID XML 파싱 오류: {e}")
            return None

    def update_paper_grobid_data(self, paper_id, grobid_data, full_text):
        """논문의 GROBID 데이터를 데이터베이스에 업데이트"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    # 키워드 추출
                    keywords = grobid_data.get('keywords', [])
                    keywords_json = json.dumps(keywords) if keywords else None

                    cursor.execute("""
                        UPDATE papers
                        SET full_text = %s,
                            grobid_data = %s,
                            keywords = %s,
                            grobid_status = 'completed',
                            updated_at = NOW()
                        WHERE id = %s
                    """, (full_text, json.dumps(grobid_data), keywords_json, paper_id))

                    print(f"  💾 데이터베이스 업데이트 완료")
                    print(f"      키워드 {len(keywords)}개 저장: {keywords}")
                    return True

        except Exception as e:
            print(f"  ❌ 데이터베이스 업데이트 실패: {e}")
            return False

    def mark_paper_failed(self, paper_id, error_msg):
        """논문 처리 실패 상태로 마킹"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE papers
                        SET grobid_status = 'failed',
                            grobid_data = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (json.dumps({'error': error_msg, 'failed_at': datetime.now().isoformat()}), paper_id))

        except Exception as e:
            print(f"  ❌ 실패 상태 업데이트 실패: {e}")

    def process_all_papers(self):
        """저장된 모든 논문 처리"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # 아직 처리되지 않은 논문들 조회
                    cursor.execute("""
                        SELECT id, openalex_id, title, pdf_url
                        FROM papers
                        WHERE pdf_url IS NOT NULL
                        AND (grobid_status IS NULL OR grobid_status = 'pending')
                        ORDER BY id
                    """)

                    papers = cursor.fetchall()
                    print(f"🚀 처리할 논문: {len(papers)}개")

            processed_count = 0
            failed_count = 0

            for i, paper in enumerate(papers, 1):
                print(f"\n📄 논문 {i}/{len(papers)} 처리 중...")
                print(f"  제목: {paper['title'][:60]}...")
                print(f"  PDF: {paper['pdf_url']}")

                try:
                    # 1. PDF 다운로드
                    pdf_content = self.download_pdf(paper['pdf_url'])
                    if not pdf_content:
                        self.mark_paper_failed(paper['id'], "PDF 다운로드 실패")
                        failed_count += 1
                        continue

                    # 2. GROBID 처리
                    grobid_data = self.process_with_grobid(pdf_content)
                    if not grobid_data:
                        self.mark_paper_failed(paper['id'], "GROBID 처리 실패")
                        failed_count += 1
                        continue

                    # 3. 데이터베이스 업데이트
                    full_text = grobid_data.get('full_text', '')
                    success = self.update_paper_grobid_data(paper['id'], grobid_data, full_text)

                    if success:
                        processed_count += 1
                        print(f"  ✅ 처리 완료 (전체 텍스트: {len(full_text)} 문자)")
                    else:
                        failed_count += 1

                    # API 제한 고려하여 잠시 대기
                    time.sleep(1)

                except Exception as e:
                    print(f"  ❌ 논문 처리 실패: {e}")
                    self.mark_paper_failed(paper['id'], str(e))
                    failed_count += 1

            print(f"\n🎉 처리 완료:")
            print(f"  성공: {processed_count}개")
            print(f"  실패: {failed_count}개")

            # 최종 상태 출력
            self.print_processing_status()

        except Exception as e:
            print(f"❌ 전체 처리 실패: {e}")

    def print_processing_status(self):
        """처리 상태 통계 출력"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(*) as total,
                            COUNT(CASE WHEN grobid_status = 'completed' THEN 1 END) as completed,
                            COUNT(CASE WHEN grobid_status = 'failed' THEN 1 END) as failed,
                            COUNT(CASE WHEN grobid_status IS NULL OR grobid_status = 'pending' THEN 1 END) as pending,
                            COUNT(CASE WHEN full_text IS NOT NULL AND length(full_text) > 0 THEN 1 END) as with_text
                        FROM papers
                    """)

                    stats = cursor.fetchone()

                    print(f"\n📊 논문 처리 현황:")
                    print(f"  전체 논문: {stats[0]}개")
                    print(f"  처리 완료: {stats[1]}개")
                    print(f"  처리 실패: {stats[2]}개")
                    print(f"  처리 대기: {stats[3]}개")
                    print(f"  텍스트 있음: {stats[4]}개")

        except Exception as e:
            print(f"❌ 상태 조회 실패: {e}")

def main():
    processor = PDFGrobidProcessor()
    processor.process_all_papers()

if __name__ == "__main__":
    main()