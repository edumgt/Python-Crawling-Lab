"""
한국은행 경제통계시스템 (ECOS) 크롤러
ECOS Open API 기반.

환경변수:
    ECOS_API_KEY  — ecos.bok.or.kr/openapi 에서 발급받은 API 키

주요 통계 코드:
    722Y001  기준금리
    731Y003  소비자물가지수 (CPI)
    200Y001  국내총생산 (GDP)
    902Y003  원/달러 환율
    036Y001  M2 통화량
    251Y002  코스피 지수
    521Y003  수출 금액
    521Y004  수입 금액

Usage:
    python crawler.py [--stat-codes 722Y001 731Y003] [--start-date 202301] [--end-date 202312]

Examples:
    ECOS_API_KEY=xxx python crawler.py
    python crawler.py --stat-codes 902Y003 --output-format json
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
logger = logging.getLogger("ecos.crawler")

ECOS_BASE = "https://ecos.bok.or.kr/api"

REQUEST_DELAY = 0.3

DEFAULT_STAT_CODES = [
    ("722Y001", "기준금리", "MM"),
    ("731Y003", "소비자물가지수", "MM"),
    ("200Y001", "국내총생산(GDP)", "QQ"),
    ("902Y003", "원달러환율", "DD"),
    ("036Y001", "M2 통화량", "MM"),
    ("251Y002", "코스피지수", "DD"),
    ("521Y003", "수출금액", "MM"),
    ("521Y004", "수입금액", "MM"),
]

ITEM_CODE_MAP = {
    "722Y001": "0101000",   # 기준금리
    "731Y003": "0",          # 소비자물가지수 총지수
    "200Y001": "10111",      # GDP 성장률
    "902Y003": "0000001",    # 원/달러 환율 (매매기준율)
    "036Y001": "BBHS00",     # M2 (평잔, 원계열)
    "251Y002": "0001000",    # 코스피
    "521Y003": "01",         # 수출 합계
    "521Y004": "01",         # 수입 합계
}


@dataclass
class EcosStatItem:
    """한국은행 경제통계 데이터 포인트"""

    stat_code: str = ""       # 통계 코드
    stat_name: str = ""       # 통계 명칭
    item_code: str = ""       # 세부 항목 코드
    item_name: str = ""       # 세부 항목명
    time: str = ""            # 기간 (YYYYMM / YYYYMMDD / YYYY 등)
    data_value: str = ""      # 데이터 값
    unit_name: str = ""       # 단위
    crawled_at: str = ""


class ECOSCrawler:
    """한국은행 경제통계 크롤러"""

    def __init__(self, api_key: Optional[str] = None, delay: float = REQUEST_DELAY):
        self.api_key = api_key or os.getenv("ECOS_API_KEY", "")
        self.delay = delay
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; ECOSCrawler/1.0)",
            "Accept": "application/json",
        })

        if not self.api_key:
            logger.warning("ECOS_API_KEY 미설정 — 실제 호출 불가. ecos.bok.or.kr/openapi 에서 발급하세요.")

    def _build_url(self, stat_code: str, item_code: str,
                   cycle: str, start_dt: str, end_dt: str,
                   offset: int = 1, limit: int = 1000) -> str:
        return (
            f"{ECOS_BASE}/StatisticSearch/{self.api_key}/json/kr"
            f"/{offset}/{offset + limit - 1}"
            f"/{stat_code}/{cycle}/{start_dt}/{end_dt}/{item_code}"
        )

    def crawl_stat(
        self,
        stat_code: str,
        stat_name: str,
        cycle: str,
        item_code: str,
        start_dt: str,
        end_dt: str,
    ) -> List[EcosStatItem]:
        """단일 통계 항목 수집"""
        if not self.api_key:
            raise RuntimeError("ECOS_API_KEY 환경변수가 필요합니다. ecos.bok.or.kr/openapi 에서 발급받으세요.")

        url = self._build_url(stat_code, item_code, cycle, start_dt, end_dt)
        logger.info("ECOS 수집: %s (%s) %s ~ %s", stat_name, stat_code, start_dt, end_dt)
        now = datetime.now(timezone.utc).isoformat()

        try:
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("ECOS API 호출 실패: %s", exc)
            return []

        rows = data.get("StatisticSearch", {}).get("row", [])
        if not rows:
            logger.warning("데이터 없음: %s %s", stat_code, data.get("RESULT", {}).get("MESSAGE", ""))
            return []

        items = []
        for row in rows:
            items.append(EcosStatItem(
                stat_code=stat_code,
                stat_name=stat_name,
                item_code=row.get("ITEM_CODE1", item_code),
                item_name=row.get("ITEM_NAME1", stat_name),
                time=row.get("TIME", ""),
                data_value=row.get("DATA_VALUE", ""),
                unit_name=row.get("UNIT_NAME", ""),
                crawled_at=now,
            ))

        logger.info("  %s: %d건 수집", stat_name, len(items))
        return items

    def crawl_all(
        self,
        stat_codes: Optional[List[tuple]] = None,
        start_dt: str = "",
        end_dt: str = "",
    ) -> List[EcosStatItem]:
        """주요 거시경제 지표 전체 수집"""
        if stat_codes is None:
            stat_codes = DEFAULT_STAT_CODES

        now = datetime.now(timezone.utc)
        if not start_dt:
            start_dt = f"{now.year - 3}01"
        if not end_dt:
            end_dt = now.strftime("%Y%m")

        all_items: List[EcosStatItem] = []
        for stat_code, stat_name, cycle in stat_codes:
            item_code = ITEM_CODE_MAP.get(stat_code, "")
            sd = start_dt if cycle != "DD" else start_dt + "01"
            ed = end_dt if cycle != "DD" else end_dt + "28"
            try:
                items = self.crawl_stat(stat_code, stat_name, cycle, item_code, sd, ed)
                all_items.extend(items)
            except RuntimeError:
                raise
            except Exception as exc:
                logger.error("[%s] 수집 실패: %s", stat_name, exc)
            time.sleep(self.delay)

        logger.info("ECOS 전체 수집 완료: %d건", len(all_items))
        return all_items

    def to_json(self, items: List[EcosStatItem]) -> str:
        return json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2)

    def to_xml(self, items: List[EcosStatItem]) -> str:
        root = ET.Element("EcosStatList")
        for item in items:
            node = ET.SubElement(root, "Stat")
            for k, v in asdict(item).items():
                child = ET.SubElement(node, k)
                child.text = str(v)
        return ET.tostring(root, encoding="unicode", xml_declaration=False)


def main():
    parser = argparse.ArgumentParser(description="한국은행 ECOS 경제통계 크롤러")
    parser.add_argument("--stat-codes", nargs="+",
                        help="통계 코드 목록 (기본: 주요 8개)")
    parser.add_argument("--start-date", help="시작일 YYYYMM (기본: 3년 전)")
    parser.add_argument("--end-date", help="종료일 YYYYMM (기본: 현재)")
    parser.add_argument("--output-format", choices=["json", "xml", "table"], default="table")
    parser.add_argument("--limit", type=int, default=30, help="테이블 출력 최대 줄 수")
    args = parser.parse_args()

    crawl_codes = None
    if args.stat_codes:
        crawl_codes = [
            (code, ITEM_CODE_MAP.get(code, code), "MM")
            for code in args.stat_codes
        ]

    crawler = ECOSCrawler()
    items = crawler.crawl_all(
        stat_codes=crawl_codes,
        start_dt=args.start_date or "",
        end_dt=args.end_date or "",
    )

    if args.output_format == "json":
        print(crawler.to_json(items))
    elif args.output_format == "xml":
        print(crawler.to_xml(items))
    else:
        print(f"\n{'통계명':<20} {'기간':<10} {'값':>15} {'단위':<10}")
        print("-" * 60)
        for item in items[:args.limit]:
            print(f"{item.stat_name:<20} {item.time:<10} {item.data_value:>15} {item.unit_name:<10}")
        print(f"\n총 {len(items)}건")


if __name__ == "__main__":
    main()
