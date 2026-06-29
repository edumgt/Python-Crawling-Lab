"""
금융감독원 DART 전자공시시스템 크롤러
OpenDART REST API 기반.

환경변수:
    DART_API_KEY  — opendart.fss.or.kr 에서 발급받은 API 키

Usage:
    python crawler.py [--corp-type Y] [--bgn-de 20240101] [--end-de 20241231]

Examples:
    DART_API_KEY=xxx python crawler.py --corp-type Y --bgn-de 20240101
    python crawler.py --output-format json --limit 50
"""

import argparse
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dart.crawler")

DART_BASE = "https://opendart.fss.or.kr/api"
DISCLOSURE_LIST_URL = f"{DART_BASE}/list.json"
COMPANY_INFO_URL = f"{DART_BASE}/company.json"
FINANCIAL_REPORT_URL = f"{DART_BASE}/fnlttSinglAcnt.json"

REQUEST_DELAY = 0.5

CORP_TYPE_MAP = {
    "Y": "유가증권 상장법인",
    "K": "코스닥 상장법인",
    "N": "코넥스 상장법인",
    "E": "기타법인",
}

DISCLOSURE_TYPE_MAP = {
    "A": "정기공시",
    "B": "주요사항보고",
    "C": "발행공시",
    "D": "지분공시",
    "E": "기타공시",
    "F": "외부감사관련",
    "G": "펀드공시",
    "H": "자산유동화",
    "I": "거래소공시",
    "J": "공정위공시",
}


@dataclass
class DisclosureInfo:
    """DART 공시 데이터 모델"""

    corp_code: str = ""       # 고유번호
    corp_name: str = ""       # 회사명
    stock_code: str = ""      # 종목코드
    corp_cls: str = ""        # 법인구분 (Y/K/N/E)
    corp_cls_name: str = ""   # 법인구분 명칭
    report_nm: str = ""       # 보고서명
    rcept_no: str = ""        # 접수번호
    flr_nm: str = ""          # 공시 제출인명
    rcept_dt: str = ""        # 접수일자 (YYYYMMDD)
    rm: str = ""              # 비고
    disclosure_url: str = ""  # 공시 URL
    crawled_at: str = ""


@dataclass
class FinancialStatement:
    """DART 재무제표 데이터 모델"""

    corp_code: str = ""
    corp_name: str = ""
    stock_code: str = ""
    bsns_year: str = ""       # 사업연도
    reprt_code: str = ""      # 보고서 코드 (11011=사업보고서 등)
    account_nm: str = ""      # 계정명
    fs_div: str = ""          # 연결/별도 구분
    sj_div: str = ""          # 재무제표 구분 (BS/IS/CF 등)
    thstrm_nm: str = ""       # 당기명
    thstrm_amount: str = ""   # 당기금액
    frmtrm_nm: str = ""       # 전기명
    frmtrm_amount: str = ""   # 전기금액
    crawled_at: str = ""


class DARTCrawler:
    """금융감독원 DART 공시 크롤러"""

    def __init__(self, api_key: Optional[str] = None, delay: float = REQUEST_DELAY):
        self.api_key = api_key or os.getenv("DART_API_KEY", "")
        self.delay = delay
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; DARTCrawler/1.0)",
        })

        if not self.api_key:
            logger.warning("DART_API_KEY 미설정 — 실제 호출 불가. 환경변수를 설정하세요.")

    def _get(self, url: str, params: dict) -> Optional[dict]:
        if not self.api_key:
            raise RuntimeError("DART_API_KEY 환경변수가 필요합니다. opendart.fss.or.kr에서 발급받으세요.")
        params["crtfc_key"] = self.api_key
        try:
            resp = self._session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") not in ("000", None):
                logger.warning("DART API 오류: status=%s message=%s", data.get("status"), data.get("message"))
                return None
            return data
        except Exception as exc:
            logger.error("DART API 호출 실패 url=%s: %s", url, exc)
            return None

    def crawl_disclosures(
        self,
        corp_type: str = "Y",
        bgn_de: Optional[str] = None,
        end_de: Optional[str] = None,
        pblntf_ty: str = "A",
        page_count: int = 100,
        max_pages: int = 5,
    ) -> List[DisclosureInfo]:
        """공시 목록 수집"""
        if not bgn_de:
            bgn_de = datetime.now(timezone.utc).strftime("%Y0101")
        if not end_de:
            end_de = datetime.now(timezone.utc).strftime("%Y%m%d")

        logger.info("DART 공시 수집: corp_type=%s, %s~%s, type=%s",
                    corp_type, bgn_de, end_de, pblntf_ty)
        now = datetime.now(timezone.utc).isoformat()
        all_items: List[DisclosureInfo] = []

        for page in range(1, max_pages + 1):
            params = {
                "corp_cls": corp_type,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": pblntf_ty,
                "page_no": str(page),
                "page_count": str(page_count),
            }
            data = self._get(DISCLOSURE_LIST_URL, params)
            if not data or "list" not in data:
                break

            items = data["list"]
            if not items:
                break

            for row in items:
                corp_cls = row.get("corp_cls", "")
                all_items.append(DisclosureInfo(
                    corp_code=row.get("corp_code", ""),
                    corp_name=row.get("corp_name", ""),
                    stock_code=row.get("stock_code", ""),
                    corp_cls=corp_cls,
                    corp_cls_name=CORP_TYPE_MAP.get(corp_cls, corp_cls),
                    report_nm=row.get("report_nm", ""),
                    rcept_no=row.get("rcept_no", ""),
                    flr_nm=row.get("flr_nm", ""),
                    rcept_dt=row.get("rcept_dt", ""),
                    rm=row.get("rm", ""),
                    disclosure_url=f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={row.get('rcept_no', '')}",
                    crawled_at=now,
                ))

            logger.info("  페이지 %d/%d: %d건 수집 (누적 %d)", page, max_pages, len(items), len(all_items))
            total_count = int(data.get("total_count", 0))
            if len(all_items) >= total_count:
                break
            time.sleep(self.delay)

        logger.info("DART 공시 수집 완료: %d건", len(all_items))
        return all_items

    def crawl_financial_statements(
        self,
        corp_codes: List[str],
        bsns_year: str = "",
        reprt_code: str = "11011",
        fs_div: str = "OFS",
    ) -> List[FinancialStatement]:
        """개별 종목 재무제표 수집 (사업보고서 기준)"""
        if not bsns_year:
            bsns_year = str(datetime.now(timezone.utc).year - 1)

        now = datetime.now(timezone.utc).isoformat()
        all_items: List[FinancialStatement] = []

        for corp_code in corp_codes:
            params = {
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            }
            data = self._get(FINANCIAL_REPORT_URL, params)
            if not data or "list" not in data:
                time.sleep(self.delay)
                continue

            for row in data["list"]:
                all_items.append(FinancialStatement(
                    corp_code=corp_code,
                    corp_name=row.get("corp_name", ""),
                    stock_code=row.get("stock_code", ""),
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    account_nm=row.get("account_nm", ""),
                    fs_div=row.get("fs_div", ""),
                    sj_div=row.get("sj_div", ""),
                    thstrm_nm=row.get("thstrm_nm", ""),
                    thstrm_amount=row.get("thstrm_amount", ""),
                    frmtrm_nm=row.get("frmtrm_nm", ""),
                    frmtrm_amount=row.get("frmtrm_amount", ""),
                    crawled_at=now,
                ))
            time.sleep(self.delay)

        logger.info("재무제표 수집 완료: %d건 (종목 %d개)", len(all_items), len(corp_codes))
        return all_items

    def to_json(self, items: list) -> str:
        return json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2)

    def to_xml(self, items: list, root_tag: str = "DisclosureList") -> str:
        root = ET.Element(root_tag)
        for item in items:
            node = ET.SubElement(root, "Item")
            for k, v in asdict(item).items():
                child = ET.SubElement(node, k)
                child.text = str(v)
        return ET.tostring(root, encoding="unicode", xml_declaration=False)


def main():
    parser = argparse.ArgumentParser(description="DART 전자공시 크롤러")
    parser.add_argument("--corp-type", default="Y",
                        choices=list(CORP_TYPE_MAP.keys()),
                        help="법인구분: Y=유가증권 K=코스닥 N=코넥스 E=기타")
    parser.add_argument("--bgn-de", help="검색 시작일 YYYYMMDD (기본: 올해 1월 1일)")
    parser.add_argument("--end-de", help="검색 종료일 YYYYMMDD (기본: 오늘)")
    parser.add_argument("--pblntf-ty", default="A",
                        help="공시 유형: A=정기 B=주요사항 ... (기본: A)")
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--output-format", choices=["json", "xml", "table"], default="table")
    parser.add_argument("--limit", type=int, default=20, help="테이블 출력 최대 줄 수")
    args = parser.parse_args()

    crawler = DARTCrawler()
    items = crawler.crawl_disclosures(
        corp_type=args.corp_type,
        bgn_de=args.bgn_de,
        end_de=args.end_de,
        pblntf_ty=args.pblntf_ty,
        max_pages=args.max_pages,
    )

    if args.output_format == "json":
        print(crawler.to_json(items))
    elif args.output_format == "xml":
        print(crawler.to_xml(items))
    else:
        print(f"\n{'접수일':<12} {'회사명':<20} {'보고서명':<40} {'종목코드':<10}")
        print("-" * 86)
        for d in items[:args.limit]:
            print(f"{d.rcept_dt:<12} {d.corp_name:<20} {d.report_nm:<40} {d.stock_code:<10}")
        print(f"\n총 {len(items)}건")


if __name__ == "__main__":
    main()
