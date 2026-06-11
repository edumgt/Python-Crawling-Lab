"""
Finance Crawler Job — stdout JSON line stream

백엔드가 docker run --rm 으로 실행하는 단일 실행 스크립트.
크롤링 진행 상황을 JSON 라인으로 stdout에 출력하고 완료 후 종료합니다.

출력 형식 (JSON Lines):
    {"type": "start",    "source": "naver", "market": "KOSPI"}
    {"type": "page",     "source": "naver", "market": "KOSPI", "page": 1, "count": 50}
    {"type": "enrich",   "source": "naver", "market": "KOSPI", "done": 20, "total": 200}
    {"type": "upload",   "source": "naver", "market": "KOSPI", "batch": 100, "total": 200}
    {"type": "done",     "source": "naver", "market": "KOSPI", "uploaded": 200}
    {"type": "complete", "total_uploaded": 800, "duration_seconds": 42.3}
    {"type": "error",    "source": "naver", "market": "KOSPI", "message": "..."}

Usage:
    python crawler_job.py --sources naver daum krx --markets KOSPI KOSDAQ
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

ROOT = Path(__file__).parent
COLLECTION_NAME = "korean_stocks"
VECTOR_DIM = 128

# stdout에는 JSON Lines만, stderr에는 일반 로그
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


def _ensure_collection(client: QdrantClient) -> None:
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )


def _upload(client: QdrantClient, stocks, source: str, code_field: str, market: str) -> int:
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


def run(sources: List[str], markets: List[str], max_pages: int, qdrant_url: str) -> int:
    _emit({"type": "init", "sources": sources, "markets": markets,
           "max_pages": max_pages, "qdrant_url": qdrant_url})

    try:
        client = QdrantClient(url=qdrant_url, timeout=10)
        _ensure_collection(client)
        _emit({"type": "qdrant_ok", "url": qdrant_url})
    except Exception as exc:
        _emit({"type": "error", "source": "qdrant", "market": "", "message": str(exc)})
        return 1

    total_uploaded = 0
    krx_done = False
    start = time.time()

    for source in sources:
        for market in markets:
            try:
                _emit({"type": "start", "source": source, "market": market})

                if source == "naver":
                    mod = _load_module("naver_crawler", ROOT / "finance.naver.com" / "crawler2.py")
                    crawler = mod.NaverStockCrawler()
                    stocks = _crawl_with_progress(crawler, market, max_pages, source)
                    code_field = "code"

                elif source == "daum":
                    mod = _load_module("daum_crawler", ROOT / "finance.daum.net" / "crawler2.py")
                    crawler = mod.DaumStockCrawler(delay=0.2)
                    stocks = _crawl_with_progress(crawler, market, max_pages, source)
                    code_field = "symbol_code"

                elif source == "krx":
                    if krx_done:
                        continue
                    mod = _load_module("krx_crawler", ROOT / "krx.co.kr" / "crawler2.py")
                    crawler = mod.KRXStockCrawler()
                    stocks = crawler.crawl_market(tab=0)
                    code_field = "code"
                    krx_done = True
                else:
                    continue

                n = _upload(client, stocks, source=source, code_field=code_field, market=market)
                total_uploaded += n
                _emit({"type": "done", "source": source, "market": market, "uploaded": n})

            except Exception as exc:
                logger.error("[%s/%s] 오류: %s", source, market, exc, exc_info=True)
                _emit({"type": "error", "source": source, "market": market, "message": str(exc)})

    duration = round(time.time() - start, 2)
    _emit({"type": "complete", "total_uploaded": total_uploaded, "duration_seconds": duration})
    return 0


def _crawl_with_progress(crawler, market: str, max_pages: int, source: str):
    """페이지별 진행 이벤트를 emit하며 크롤링 (원본 crawl_market 래퍼)"""
    market_map = {"KOSPI": "0", "KOSDAQ": "1"}
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from urllib.parse import urlencode

    NAVER_MARKET_URL = "https://finance.naver.com/sise/sise_market_sum.naver"
    sosok = market_map.get(market.upper(), "1")

    all_stocks = []
    driver = crawler._get_driver()

    try:
        for page in range(1, max_pages + 1):
            params = {"sosok": sosok, "page": page}
            url = f"{NAVER_MARKET_URL}?{urlencode(params)}"
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
                import time as _t
                _t.sleep(crawler.delay)
    finally:
        crawler.close()

    # API 보강 진행 이벤트
    all_stocks = _enrich_with_progress(crawler, all_stocks, source, market)
    return all_stocks


def _enrich_with_progress(crawler, stocks, source: str, market: str):
    import time as _t
    import requests

    NAVER_ITEM_API = "https://api.finance.naver.com/service/itemSummary.nhn"
    API_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    }
    session = requests.Session()
    session.headers.update(API_HEADERS)

    for i, stock in enumerate(stocks):
        if not stock.symbol_code if hasattr(stock, "symbol_code") else not getattr(stock, "code", ""):
            continue
        code = getattr(stock, "symbol_code", None) or getattr(stock, "code", "")
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
        _t.sleep(crawler.delay if hasattr(crawler, "delay") else 0.2)

    return stocks


def main():
    import os
    parser = argparse.ArgumentParser(description="Finance Crawler Job (stdout JSON lines)")
    parser.add_argument("--sources", nargs="+", default=["naver", "daum", "krx"])
    parser.add_argument("--markets", nargs="+", default=["KOSPI", "KOSDAQ"])
    parser.add_argument("--max-pages", type=int, default=4)
    parser.add_argument("--qdrant-url", type=str,
                        default=os.getenv("QDRANT_URL", "http://qdrant:6333"))
    args = parser.parse_args()
    sys.exit(run(args.sources, args.markets, args.max_pages, args.qdrant_url))


if __name__ == "__main__":
    main()
