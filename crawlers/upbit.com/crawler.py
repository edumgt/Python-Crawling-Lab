"""
업비트 (Upbit) 암호화폐 시세 크롤러
Public REST API 기반, 인증 불필요.

Usage:
    python crawler.py [--markets KRW BTC USDT] [--output-format FORMAT]

Examples:
    python crawler.py
    python crawler.py --markets KRW --output-format json
"""

import argparse
import json
import logging
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
logger = logging.getLogger("upbit.crawler")

UPBIT_BASE = "https://api.upbit.com/v1"
MARKET_ALL_URL = f"{UPBIT_BASE}/market/all"
TICKER_URL = f"{UPBIT_BASE}/ticker"
CANDLE_DAY_URL = f"{UPBIT_BASE}/candles/days"

REQUEST_DELAY = 0.11   # Upbit API rate limit: ~10 req/sec

API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


@dataclass
class CryptoInfo:
    """업비트 암호화폐 데이터 모델"""

    market: str = ""           # ex) KRW-BTC
    base_currency: str = ""    # ex) BTC
    quote_currency: str = ""   # ex) KRW
    korean_name: str = ""
    english_name: str = ""
    trade_price: float = 0.0          # 현재가
    change: str = ""                  # RISE / FALL / EVEN
    change_rate: float = 0.0          # 전일 대비 등락률
    change_price: float = 0.0         # 전일 대비 변화액
    signed_change_rate: float = 0.0   # 부호 있는 등락률
    high_price: float = 0.0           # 고가
    low_price: float = 0.0            # 저가
    prev_closing_price: float = 0.0   # 전일 종가
    acc_trade_volume_24h: float = 0.0 # 24시간 누적 거래량
    acc_trade_price_24h: float = 0.0  # 24시간 누적 거래대금
    market_cap: float = 0.0           # 시가총액 (원화 마켓 한정)
    crawled_at: str = ""


class UpbitCrawler:
    """업비트 암호화폐 시세 크롤러 (Public API)"""

    def __init__(self, delay: float = REQUEST_DELAY):
        self.delay = delay
        self._session = requests.Session()
        self._session.headers.update(API_HEADERS)

    def _get_market_list(self, quote_currencies: List[str]) -> List[dict]:
        """거래 가능한 마켓 목록 조회"""
        resp = self._session.get(MARKET_ALL_URL, params={"isDetails": "false"}, timeout=10)
        resp.raise_for_status()
        all_markets = resp.json()

        filtered = [
            m for m in all_markets
            if m["market"].split("-")[0] in [q.upper() for q in quote_currencies]
        ]
        logger.info("마켓 목록: 전체 %d개 → 필터 %d개", len(all_markets), len(filtered))
        return filtered

    def _get_tickers(self, market_codes: List[str]) -> List[dict]:
        """종목 현재가 일괄 조회 (최대 100개씩 배치)"""
        results = []
        batch_size = 100
        for i in range(0, len(market_codes), batch_size):
            batch = market_codes[i:i + batch_size]
            params = {"markets": ",".join(batch)}
            try:
                resp = self._session.get(TICKER_URL, params=params, timeout=10)
                resp.raise_for_status()
                results.extend(resp.json())
            except Exception as exc:
                logger.warning("배치 %d 조회 실패: %s", i // batch_size, exc)
            time.sleep(self.delay)
        return results

    def crawl(self, quote_currencies: Optional[List[str]] = None) -> List[CryptoInfo]:
        """암호화폐 시세 전체 수집"""
        if quote_currencies is None:
            quote_currencies = ["KRW"]

        logger.info("업비트 크롤링 시작: quote=%s", quote_currencies)
        markets = self._get_market_list(quote_currencies)
        market_info = {m["market"]: m for m in markets}
        market_codes = list(market_info.keys())

        tickers = self._get_tickers(market_codes)
        now = datetime.now(timezone.utc).isoformat()
        items: List[CryptoInfo] = []

        for t in tickers:
            code = t.get("market", "")
            parts = code.split("-", 1)
            quote = parts[0] if len(parts) == 2 else ""
            base = parts[1] if len(parts) == 2 else code
            info = market_info.get(code, {})

            items.append(CryptoInfo(
                market=code,
                base_currency=base,
                quote_currency=quote,
                korean_name=info.get("korean_name", ""),
                english_name=info.get("english_name", ""),
                trade_price=float(t.get("trade_price", 0) or 0),
                change=t.get("change", ""),
                change_rate=float(t.get("change_rate", 0) or 0),
                change_price=float(t.get("change_price", 0) or 0),
                signed_change_rate=float(t.get("signed_change_rate", 0) or 0),
                high_price=float(t.get("high_price", 0) or 0),
                low_price=float(t.get("low_price", 0) or 0),
                prev_closing_price=float(t.get("prev_closing_price", 0) or 0),
                acc_trade_volume_24h=float(t.get("acc_trade_volume_24h", 0) or 0),
                acc_trade_price_24h=float(t.get("acc_trade_price_24h", 0) or 0),
                crawled_at=now,
            ))

        logger.info("업비트 수집 완료: %d종목", len(items))
        return items

    def to_json(self, items: List[CryptoInfo]) -> str:
        return json.dumps([asdict(c) for c in items], ensure_ascii=False, indent=2)

    def to_xml(self, items: List[CryptoInfo]) -> str:
        root = ET.Element("CryptoList")
        for item in items:
            node = ET.SubElement(root, "Crypto")
            for k, v in asdict(item).items():
                child = ET.SubElement(node, k)
                child.text = str(v)
        return ET.tostring(root, encoding="unicode", xml_declaration=False)


def main():
    parser = argparse.ArgumentParser(description="Upbit Crypto Crawler")
    parser.add_argument("--markets", nargs="+", default=["KRW"],
                        help="Quote currency filter: KRW BTC USDT")
    parser.add_argument("--output-format", choices=["json", "xml", "table"],
                        default="table")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY)
    args = parser.parse_args()

    crawler = UpbitCrawler(delay=args.delay)
    items = crawler.crawl(quote_currencies=args.markets)

    if args.output_format == "json":
        print(crawler.to_json(items))
    elif args.output_format == "xml":
        print(crawler.to_xml(items))
    else:
        print(f"\n{'마켓':<15} {'한글명':<20} {'현재가':>15} {'등락률':>8} {'24h 거래대금':>20}")
        print("-" * 82)
        for c in sorted(items, key=lambda x: -x.acc_trade_price_24h):
            sign = "▲" if c.change == "RISE" else "▼" if c.change == "FALL" else "-"
            print(f"{c.market:<15} {c.korean_name:<20} {c.trade_price:>15,.2f} "
                  f"{sign}{abs(c.change_rate * 100):>6.2f}% {c.acc_trade_price_24h:>20,.0f}")
        print(f"\n총 {len(items)}종목")


if __name__ == "__main__":
    main()
