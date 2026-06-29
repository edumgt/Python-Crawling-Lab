"""
Finance Crawler Job — stdout JSON line stream + Data Lake Bronze 기록

백엔드가 docker run --rm 으로 실행하는 단일 실행 스크립트.
크롤링 진행 상황을 JSON 라인으로 stdout에 출력하고 완료 후 종료합니다.

지원 소스:
    naver   네이버 금융 (국내 주식 KOSPI/KOSDAQ)
    daum    다음 금융  (국내 주식 KOSPI/KOSDAQ)
    krx     한국거래소 ETF
    upbit   업비트 암호화폐 (Public API)
    dart    DART 전자공시 (DART_API_KEY 필요)
    ecos    한국은행 경제통계 (ECOS_API_KEY 필요)
    yahoo   Yahoo Finance 글로벌 시장 (yfinance)

출력 형식 (JSON Lines):
    {"type": "start",    "source": "naver", "market": "KOSPI"}
    {"type": "page",     "source": "naver", "market": "KOSPI", "page": 1, "count": 50}
    {"type": "enrich",   "source": "naver", "market": "KOSPI", "done": 20, "total": 200}
    {"type": "upload",   "source": "naver", "market": "KOSPI", "batch": 100, "total": 200}
    {"type": "bronze",   "source": "upbit", "path": "s3://...", "count": 300}
    {"type": "done",     "source": "naver", "market": "KOSPI", "uploaded": 200}
    {"type": "complete", "total_uploaded": 800, "duration_seconds": 42.3}
    {"type": "error",    "source": "naver", "market": "KOSPI", "message": "..."}

Usage:
    python pipeline/crawler_job.py --sources naver daum krx upbit yahoo --markets KOSPI KOSDAQ
"""

import argparse
import hashlib
import importlib.util
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import List

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams

ROOT = Path(__file__).parent.parent   # repo root
CRAWLERS = ROOT / "crawlers"

COLLECTION_NAME = "korean_stocks"
VECTOR_DIM = 128

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("crawler.job")


def _emit(event: dict) -> None:
    print(json.dumps(event, ensure_ascii=False), flush=True)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_vector(code: str, name: str, market: str) -> List[float]:
    combined = f"{market}:{code}:{name}"
    vec = np.zeros(VECTOR_DIM, dtype=np.float32)
    padded = f"<{combined}>"
    for i in range(len(padded) - 1):
        gram = padded[i:i + 2]
        h = int(hashlib.md5(gram.encode()).hexdigest(), 16)
        vec[h % VECTOR_DIM] += 1.0
    norm = np.linalg.norm(vec)
    return (vec / norm).tolist() if norm > 0 else vec.tolist()


def _make_point_id(source: str, code: str) -> int:
    return int(hashlib.sha256(f"{source}:{code}".encode()).hexdigest()[:15], 16)


def _ensure_collection(client: QdrantClient, name: str, dim: int) -> None:
    existing = [c.name for c in client.get_collections().collections]
    if name not in existing:
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )


def _try_bronze_write(records: list, source: str, data_type: str) -> str:
    """Bronze 레이어에 쓰기 시도 (실패 시 경고만)"""
    try:
        from data_lake.bronze import BronzeLayer
        bronze = BronzeLayer()
        path = bronze.write(records, source=source, data_type=data_type)
        return path
    except Exception as exc:
        logger.warning("Bronze 저장 실패 (건너뜀): %s", exc)
        return ""


# ── 국내 주식 (naver / daum) ──────────────────────────────────────────────

def _upload_stocks(client: QdrantClient, stocks, source: str,
                   code_field: str, market: str) -> int:
    if not stocks:
        return 0
    points = []
    for stock in stocks:
        code = getattr(stock, code_field, "") or ""
        points.append(PointStruct(
            id=_make_point_id(source, code),
            vector=_make_vector(code, stock.name or "", stock.market or ""),
            payload={
                "source": source,
                "code": code,
                "name": stock.name or "",
                "market": stock.market or "",
                "current_price": getattr(stock, "current_price", 0),
                "change_rate": getattr(stock, "change_rate", 0.0),
                "volume": getattr(stock, "volume", 0),
                "trading_value": getattr(stock, "trading_value", 0),
                "market_cap": getattr(stock, "market_cap", 0),
                "per": getattr(stock, "per", 0.0),
                "pbr": getattr(stock, "pbr", 0.0),
                "eps": getattr(stock, "eps", 0),
                "roe": getattr(stock, "roe", 0.0),
                "dividend_yield": getattr(stock, "dividend_yield", 0.0),
                "foreign_ratio": getattr(stock, "foreign_ratio", 0.0),
                "crawled_at": getattr(stock, "crawled_at", ""),
            },
        ))

    uploaded = 0
    for i in range(0, len(points), 100):
        batch = points[i:i + 100]
        client.upsert(collection_name=COLLECTION_NAME, points=batch)
        uploaded += len(batch)
        _emit({"type": "upload", "source": source, "market": market,
               "batch": len(batch), "total": uploaded})
    return uploaded


def _crawl_naver_with_progress(crawler, market: str, max_pages: int, source: str):
    from urllib.parse import urlencode
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    NAVER_MARKET_URL = "https://finance.naver.com/sise/sise_market_sum.naver"
    market_map = {"KOSPI": "0", "KOSDAQ": "1"}
    sosok = market_map.get(market.upper(), "1")
    all_stocks = []
    driver = crawler._get_driver()

    try:
        for page in range(1, max_pages + 1):
            url = f"{NAVER_MARKET_URL}?{urlencode({'sosok': sosok, 'page': page})}"
            driver.get(url)
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.type_2"))
                )
            except Exception:
                break
            items = crawler._extract_stocks_from_html(driver.page_source, market.upper())
            if not items:
                break
            all_stocks.extend(items)
            _emit({"type": "page", "source": source, "market": market,
                   "page": page, "count": len(items), "total": len(all_stocks)})
            if page < max_pages:
                time.sleep(crawler.delay)
    finally:
        crawler.close()

    all_stocks = _enrich_naver(crawler, all_stocks, source, market)
    return all_stocks


def _enrich_naver(crawler, stocks, source: str, market: str):
    import requests as _req
    NAVER_ITEM_API = "https://api.finance.naver.com/service/itemSummary.nhn"
    session = _req.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.naver.com/",
    })
    for i, stock in enumerate(stocks):
        code = getattr(stock, "symbol_code", None) or getattr(stock, "code", "")
        if not code:
            continue
        try:
            resp = session.get(NAVER_ITEM_API, params={"itemcode": code}, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                stock.pbr = float(data.get("pbr", 0) or 0)
                stock.eps = int(data.get("eps", 0) or 0)
                stock.dividend_yield = float(data.get("dividendYield", 0) or 0)
                stock.trading_value = int(data.get("amount", 0) or 0)
        except Exception:
            pass
        if (i + 1) % 20 == 0:
            _emit({"type": "enrich", "source": source, "market": market,
                   "done": i + 1, "total": len(stocks)})
        time.sleep(crawler.delay if hasattr(crawler, "delay") else 0.2)
    return stocks


# ── 업비트 암호화폐 ────────────────────────────────────────────────────────

def _run_upbit(client: QdrantClient, quote_currencies: List[str]) -> int:
    mod = _load_module("upbit_crawler", CRAWLERS / "upbit.com" / "crawler.py")
    crawler = mod.UpbitCrawler()
    items = crawler.crawl(quote_currencies=quote_currencies)

    path = _try_bronze_write(items, source="upbit", data_type="crypto")
    if path:
        _emit({"type": "bronze", "source": "upbit", "path": path, "count": len(items)})

    CRYPTO_COLLECTION = "crypto_assets"
    _ensure_collection(client, CRYPTO_COLLECTION, VECTOR_DIM)

    points = []
    for item in items:
        points.append(PointStruct(
            id=_make_point_id("upbit", item.market),
            vector=_make_vector(item.market, item.korean_name or item.english_name, item.quote_currency),
            payload={
                "source": "upbit",
                "market": item.market,
                "base_currency": item.base_currency,
                "quote_currency": item.quote_currency,
                "korean_name": item.korean_name,
                "english_name": item.english_name,
                "trade_price": item.trade_price,
                "change": item.change,
                "change_rate": item.change_rate,
                "acc_trade_price_24h": item.acc_trade_price_24h,
                "high_price": item.high_price,
                "low_price": item.low_price,
                "crawled_at": item.crawled_at,
            },
        ))

    uploaded = 0
    for i in range(0, len(points), 100):
        batch = points[i:i + 100]
        client.upsert(collection_name=CRYPTO_COLLECTION, points=batch)
        uploaded += len(batch)
        _emit({"type": "upload", "source": "upbit", "market": "CRYPTO",
               "batch": len(batch), "total": uploaded})
    return uploaded


# ── DART 공시 ──────────────────────────────────────────────────────────────

def _run_dart(corp_type: str = "Y", max_pages: int = 3) -> int:
    import os
    if not os.getenv("DART_API_KEY"):
        _emit({"type": "skip", "source": "dart",
               "reason": "DART_API_KEY 환경변수 미설정"})
        return 0

    mod = _load_module("dart_crawler", CRAWLERS / "dart.fss.or.kr" / "crawler.py")
    crawler = mod.DARTCrawler()
    items = crawler.crawl_disclosures(corp_type=corp_type, max_pages=max_pages)

    path = _try_bronze_write(items, source="dart", data_type="disclosures")
    if path:
        _emit({"type": "bronze", "source": "dart", "path": path, "count": len(items)})
    return len(items)


# ── 한국은행 ECOS ───────────────────────────────────────────────────────────

def _run_ecos() -> int:
    import os
    if not os.getenv("ECOS_API_KEY"):
        _emit({"type": "skip", "source": "ecos",
               "reason": "ECOS_API_KEY 환경변수 미설정"})
        return 0

    mod = _load_module("ecos_crawler", CRAWLERS / "ecos.bok.or.kr" / "crawler.py")
    crawler = mod.ECOSCrawler()
    items = crawler.crawl_all()

    path = _try_bronze_write(items, source="ecos", data_type="macro")
    if path:
        _emit({"type": "bronze", "source": "ecos", "path": path, "count": len(items)})
    return len(items)


# ── Yahoo Finance 글로벌 ────────────────────────────────────────────────────

def _run_yahoo(client: QdrantClient, categories: List[str]) -> int:
    mod = _load_module("yahoo_crawler", CRAWLERS / "yahoo.finance" / "crawler.py")
    try:
        crawler = mod.YahooFinanceCrawler()
    except ImportError as exc:
        _emit({"type": "skip", "source": "yahoo", "reason": str(exc)})
        return 0

    items = crawler.crawl(categories=categories)

    path = _try_bronze_write(items, source="yahoo", data_type="global")
    if path:
        _emit({"type": "bronze", "source": "yahoo", "path": path, "count": len(items)})

    GLOBAL_COLLECTION = "global_markets"
    _ensure_collection(client, GLOBAL_COLLECTION, VECTOR_DIM)

    points = []
    for item in items:
        points.append(PointStruct(
            id=_make_point_id("yahoo", item.ticker),
            vector=_make_vector(item.ticker, item.name, item.category),
            payload={
                "source": "yahoo",
                "ticker": item.ticker,
                "name": item.name,
                "category": item.category,
                "region": item.region,
                "currency": item.currency,
                "current_price": item.current_price,
                "change_percent": item.change_percent,
                "volume": item.volume,
                "market_cap": item.market_cap,
                "pe_ratio": item.pe_ratio,
                "crawled_at": item.crawled_at,
            },
        ))

    uploaded = 0
    for i in range(0, len(points), 100):
        batch = points[i:i + 100]
        client.upsert(collection_name=GLOBAL_COLLECTION, points=batch)
        uploaded += len(batch)
        _emit({"type": "upload", "source": "yahoo", "market": "GLOBAL",
               "batch": len(batch), "total": uploaded})
    return uploaded


# ── 메인 실행 ──────────────────────────────────────────────────────────────

def run(sources: List[str], markets: List[str], max_pages: int,
        qdrant_url: str, upbit_markets: List[str],
        yahoo_categories: List[str]) -> int:
    _emit({"type": "init", "sources": sources, "markets": markets,
           "max_pages": max_pages, "qdrant_url": qdrant_url})

    try:
        client = QdrantClient(url=qdrant_url, timeout=10)
        _ensure_collection(client, COLLECTION_NAME, VECTOR_DIM)
        _emit({"type": "qdrant_ok", "url": qdrant_url})
    except Exception as exc:
        _emit({"type": "error", "source": "qdrant", "market": "", "message": str(exc)})
        return 1

    total_uploaded = 0
    krx_done = False
    start = time.time()

    for source in sources:
        try:
            # ── 국내 주식 소스
            if source in ("naver", "daum"):
                for market in markets:
                    _emit({"type": "start", "source": source, "market": market})
                    if source == "naver":
                        mod = _load_module("naver_crawler",
                                           CRAWLERS / "finance.naver.com" / "crawler2.py")
                        crawler = mod.NaverStockCrawler()
                        stocks = _crawl_naver_with_progress(crawler, market, max_pages, source)
                        code_field = "code"
                    else:
                        mod = _load_module("daum_crawler",
                                           CRAWLERS / "finance.daum.net" / "crawler2.py")
                        crawler = mod.DaumStockCrawler(delay=0.2)
                        stocks = _crawl_naver_with_progress(crawler, market, max_pages, source)
                        code_field = "symbol_code"

                    path = _try_bronze_write(stocks, source=source, data_type="stocks")
                    if path:
                        _emit({"type": "bronze", "source": source, "market": market,
                               "path": path, "count": len(stocks)})

                    n = _upload_stocks(client, stocks, source=source,
                                       code_field=code_field, market=market)
                    total_uploaded += n
                    _emit({"type": "done", "source": source, "market": market, "uploaded": n})

            # ── KRX ETF
            elif source == "krx":
                if krx_done:
                    continue
                _emit({"type": "start", "source": "krx", "market": "ETF"})
                mod = _load_module("krx_crawler",
                                   CRAWLERS / "krx.co.kr" / "crawler2.py")
                crawler = mod.KRXStockCrawler()
                stocks = crawler.crawl_market(tab=0)

                path = _try_bronze_write(stocks, source="krx", data_type="etf")
                if path:
                    _emit({"type": "bronze", "source": "krx", "market": "ETF",
                           "path": path, "count": len(stocks)})

                n = _upload_stocks(client, stocks, source="krx",
                                   code_field="code", market="ETF")
                total_uploaded += n
                krx_done = True
                _emit({"type": "done", "source": "krx", "market": "ETF", "uploaded": n})

            # ── 업비트 암호화폐
            elif source == "upbit":
                _emit({"type": "start", "source": "upbit", "market": "CRYPTO"})
                n = _run_upbit(client, upbit_markets)
                total_uploaded += n
                _emit({"type": "done", "source": "upbit", "market": "CRYPTO", "uploaded": n})

            # ── DART 공시
            elif source == "dart":
                _emit({"type": "start", "source": "dart", "market": "DISCLOSURE"})
                n = _run_dart(max_pages=max_pages)
                _emit({"type": "done", "source": "dart", "market": "DISCLOSURE",
                       "uploaded": n, "note": "Bronze 저장 (Qdrant 미사용)"})

            # ── 한국은행 ECOS
            elif source == "ecos":
                _emit({"type": "start", "source": "ecos", "market": "MACRO"})
                n = _run_ecos()
                _emit({"type": "done", "source": "ecos", "market": "MACRO",
                       "uploaded": n, "note": "Bronze 저장 (Qdrant 미사용)"})

            # ── Yahoo Finance 글로벌
            elif source == "yahoo":
                _emit({"type": "start", "source": "yahoo", "market": "GLOBAL"})
                n = _run_yahoo(client, yahoo_categories)
                total_uploaded += n
                _emit({"type": "done", "source": "yahoo", "market": "GLOBAL", "uploaded": n})

        except Exception as exc:
            logger.error("[%s] 오류: %s", source, exc, exc_info=True)
            _emit({"type": "error", "source": source, "market": "", "message": str(exc)})

    duration = round(time.time() - start, 2)
    _emit({"type": "complete", "total_uploaded": total_uploaded, "duration_seconds": duration})
    return 0


def main():
    import os
    parser = argparse.ArgumentParser(description="Finance Crawler Job (stdout JSON lines)")
    parser.add_argument("--sources", nargs="+",
                        default=["naver", "daum", "krx"],
                        choices=["naver", "daum", "krx", "upbit", "dart", "ecos", "yahoo"])
    parser.add_argument("--markets", nargs="+", default=["KOSPI", "KOSDAQ"])
    parser.add_argument("--max-pages", type=int, default=4)
    parser.add_argument("--qdrant-url", type=str,
                        default=os.getenv("QDRANT_URL", "http://qdrant:6333"))
    parser.add_argument("--upbit-markets", nargs="+", default=["KRW"],
                        help="업비트 마켓 (KRW BTC USDT)")
    parser.add_argument("--yahoo-categories", nargs="+",
                        default=["indices", "etfs", "stocks", "commodities", "fx"],
                        help="Yahoo 카테고리")
    args = parser.parse_args()
    sys.exit(run(
        args.sources, args.markets, args.max_pages, args.qdrant_url,
        args.upbit_markets, args.yahoo_categories,
    ))


if __name__ == "__main__":
    main()
