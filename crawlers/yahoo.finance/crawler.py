"""
Yahoo Finance 글로벌 시장 크롤러
yfinance 라이브러리 기반, API 키 불필요.

수집 대상:
    - 글로벌 주요 지수 (S&P500, NASDAQ, NIKKEI, FTSE 등)
    - 글로벌 ETF (SPY, QQQ, EWY 등)
    - 미국 주요 테크주 (FAANG 등)
    - 원자재 (원유, 금, 은)
    - 환율 (USD/KRW, EUR/KRW 등)

Usage:
    python crawler.py [--categories indices etfs stocks] [--period 5d]

Examples:
    python crawler.py --categories indices
    python crawler.py --output-format json --period 1mo
"""

import argparse
import json
import logging
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("yahoo.finance.crawler")

# 글로벌 주요 지수
GLOBAL_INDICES = [
    ("^GSPC",  "S&P 500",          "미국"),
    ("^IXIC",  "NASDAQ Composite", "미국"),
    ("^DJI",   "Dow Jones",        "미국"),
    ("^RUT",   "Russell 2000",     "미국"),
    ("^N225",  "Nikkei 225",       "일본"),
    ("^HSI",   "Hang Seng",        "홍콩"),
    ("000001.SS", "Shanghai Composite", "중국"),
    ("^FTSE",  "FTSE 100",         "영국"),
    ("^GDAXI", "DAX",              "독일"),
    ("^KS11",  "KOSPI",            "한국"),
    ("^KQ11",  "KOSDAQ",           "한국"),
    ("^VIX",   "VIX 공포지수",      "미국"),
]

# 글로벌 ETF
GLOBAL_ETFS = [
    ("SPY",  "SPDR S&P 500 ETF",       "미국"),
    ("QQQ",  "Invesco QQQ Trust",       "미국"),
    ("IWM",  "iShares Russell 2000",    "미국"),
    ("EFA",  "iShares MSCI EAFE",       "선진국"),
    ("EEM",  "iShares MSCI EM",         "신흥국"),
    ("EWY",  "iShares MSCI South Korea","한국"),
    ("GLD",  "SPDR Gold Shares",        "원자재"),
    ("TLT",  "iShares 20Y+ Treasury",   "채권"),
    ("VNQ",  "Vanguard Real Estate",    "부동산"),
    ("ARKK", "ARK Innovation ETF",      "테크"),
]

# 미국 주요 테크주
US_TECH_STOCKS = [
    ("AAPL",  "Apple",          "테크"),
    ("MSFT",  "Microsoft",      "테크"),
    ("GOOGL", "Alphabet",       "테크"),
    ("AMZN",  "Amazon",         "테크"),
    ("META",  "Meta",           "테크"),
    ("NVDA",  "NVIDIA",         "반도체"),
    ("TSLA",  "Tesla",          "전기차"),
    ("AVGO",  "Broadcom",       "반도체"),
    ("JPM",   "JPMorgan Chase", "금융"),
    ("BRK-B", "Berkshire Hathaway", "금융"),
]

# 원자재 선물
COMMODITIES = [
    ("CL=F",  "WTI 원유",   "에너지"),
    ("BZ=F",  "Brent 원유", "에너지"),
    ("GC=F",  "금 선물",    "귀금속"),
    ("SI=F",  "은 선물",    "귀금속"),
    ("HG=F",  "구리 선물",  "산업금속"),
    ("NG=F",  "천연가스",   "에너지"),
]

# 주요 환율
FX_PAIRS = [
    ("USDKRW=X", "USD/KRW", "환율"),
    ("EURKRW=X", "EUR/KRW", "환율"),
    ("JPYKRW=X", "JPY/KRW", "환율"),
    ("CNYKRW=X", "CNY/KRW", "환율"),
    ("DX-Y.NYB", "달러인덱스", "환율"),
    ("EURUSD=X", "EUR/USD",  "환율"),
]

CATEGORY_SYMBOLS = {
    "indices":    GLOBAL_INDICES,
    "etfs":       GLOBAL_ETFS,
    "stocks":     US_TECH_STOCKS,
    "commodities": COMMODITIES,
    "fx":         FX_PAIRS,
}


@dataclass
class GlobalMarketInfo:
    """글로벌 시장 데이터 모델"""

    ticker: str = ""
    name: str = ""
    category: str = ""
    region: str = ""
    currency: str = ""
    current_price: float = 0.0
    previous_close: float = 0.0
    open_price: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    change: float = 0.0
    change_percent: float = 0.0
    volume: int = 0
    market_cap: float = 0.0
    fifty_two_week_high: float = 0.0
    fifty_two_week_low: float = 0.0
    pe_ratio: float = 0.0
    dividend_yield: float = 0.0
    beta: float = 0.0
    crawled_at: str = ""


class YahooFinanceCrawler:
    """Yahoo Finance 글로벌 시장 크롤러 (yfinance)"""

    def __init__(self):
        try:
            import yfinance as yf
            self._yf = yf
        except ImportError:
            raise ImportError(
                "yfinance 패키지가 필요합니다: pip install yfinance"
            )

    def _fetch_ticker(self, symbol: str, name: str,
                      category: str, region: str) -> Optional[GlobalMarketInfo]:
        try:
            ticker = self._yf.Ticker(symbol)
            info = ticker.info
            now = datetime.now(timezone.utc).isoformat()

            current_price = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("ask")
                or info.get("bid")
                or 0.0
            )

            return GlobalMarketInfo(
                ticker=symbol,
                name=name or info.get("shortName", symbol),
                category=category,
                region=region,
                currency=info.get("currency", ""),
                current_price=float(current_price or 0),
                previous_close=float(info.get("previousClose") or info.get("regularMarketPreviousClose") or 0),
                open_price=float(info.get("open") or info.get("regularMarketOpen") or 0),
                day_high=float(info.get("dayHigh") or info.get("regularMarketDayHigh") or 0),
                day_low=float(info.get("dayLow") or info.get("regularMarketDayLow") or 0),
                change=float(info.get("regularMarketChange") or 0),
                change_percent=float(info.get("regularMarketChangePercent") or 0),
                volume=int(info.get("volume") or info.get("regularMarketVolume") or 0),
                market_cap=float(info.get("marketCap") or 0),
                fifty_two_week_high=float(info.get("fiftyTwoWeekHigh") or 0),
                fifty_two_week_low=float(info.get("fiftyTwoWeekLow") or 0),
                pe_ratio=float(info.get("trailingPE") or info.get("forwardPE") or 0),
                dividend_yield=float(info.get("dividendYield") or 0),
                beta=float(info.get("beta") or 0),
                crawled_at=now,
            )
        except Exception as exc:
            logger.warning("[%s] 수집 실패: %s", symbol, exc)
            return None

    def crawl(self, categories: Optional[List[str]] = None) -> List[GlobalMarketInfo]:
        """글로벌 시장 데이터 수집"""
        if categories is None:
            categories = list(CATEGORY_SYMBOLS.keys())

        all_items: List[GlobalMarketInfo] = []
        for cat in categories:
            symbols = CATEGORY_SYMBOLS.get(cat, [])
            logger.info("카테고리 '%s' 수집: %d종목", cat, len(symbols))
            for symbol, name, region in symbols:
                item = self._fetch_ticker(symbol, name, cat, region)
                if item:
                    all_items.append(item)

        logger.info("Yahoo Finance 수집 완료: %d종목", len(all_items))
        return all_items

    def crawl_history(self, ticker: str, period: str = "1mo",
                      interval: str = "1d") -> List[dict]:
        """특정 종목 히스토리 데이터 수집"""
        try:
            t = self._yf.Ticker(ticker)
            hist = t.history(period=period, interval=interval)
            now = datetime.now(timezone.utc).isoformat()
            rows = []
            for dt, row in hist.iterrows():
                rows.append({
                    "ticker": ticker,
                    "date": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": float(row.get("Close", 0)),
                    "volume": int(row.get("Volume", 0)),
                    "dividends": float(row.get("Dividends", 0)),
                    "stock_splits": float(row.get("Stock Splits", 0)),
                    "crawled_at": now,
                })
            return rows
        except Exception as exc:
            logger.error("[%s] 히스토리 수집 실패: %s", ticker, exc)
            return []

    def to_json(self, items: List[GlobalMarketInfo]) -> str:
        return json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2)

    def to_xml(self, items: List[GlobalMarketInfo]) -> str:
        root = ET.Element("GlobalMarketList")
        for item in items:
            node = ET.SubElement(root, "Market")
            for k, v in asdict(item).items():
                child = ET.SubElement(node, k)
                child.text = str(v)
        return ET.tostring(root, encoding="unicode", xml_declaration=False)


def main():
    parser = argparse.ArgumentParser(description="Yahoo Finance 글로벌 시장 크롤러")
    parser.add_argument("--categories", nargs="+",
                        choices=list(CATEGORY_SYMBOLS.keys()),
                        default=list(CATEGORY_SYMBOLS.keys()),
                        help="수집할 카테고리 (indices etfs stocks commodities fx)")
    parser.add_argument("--output-format", choices=["json", "xml", "table"], default="table")
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()

    crawler = YahooFinanceCrawler()
    items = crawler.crawl(categories=args.categories)

    if args.output_format == "json":
        print(crawler.to_json(items))
    elif args.output_format == "xml":
        print(crawler.to_xml(items))
    else:
        print(f"\n{'티커':<12} {'이름':<25} {'카테고리':<12} {'현재가':>15} {'등락률':>8}")
        print("-" * 78)
        for item in items[:args.limit]:
            sign = "▲" if item.change_percent > 0 else "▼" if item.change_percent < 0 else "-"
            print(f"{item.ticker:<12} {item.name:<25} {item.category:<12} "
                  f"{item.current_price:>15,.2f} {sign}{abs(item.change_percent):>6.2f}%")
        print(f"\n총 {len(items)}종목")


if __name__ == "__main__":
    main()
