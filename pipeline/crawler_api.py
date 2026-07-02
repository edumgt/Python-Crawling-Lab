"""
Finance Crawler Backend API

FE에서 호출하면 Docker 컨테이너로 크롤링 잡을 실행하고,
실행 상태를 SSE(Server-Sent Events)로 FE에 실시간 스트리밍합니다.

Endpoints:
    GET  /health                  상태 확인
    POST /crawl/start             크롤링 잡 시작 → job_id 반환
    GET  /crawl/events/{job_id}   SSE 스트림 (진행 상황 실시간)
    GET  /crawl/status/{job_id}   잡 최종 상태 조회
    GET  /lake/catalog            데이터 레이크 카탈로그 조회
    GET  /lake/stats              데이터 레이크 수집 통계
    GET  /lake/history            수집 이력 조회
    POST /lake/gold/build         Gold 집계 레이어 빌드 트리거
    GET  /lake/recommendations    추천 종목 조회 (크롤링 완료 시 자동 갱신)
"""

import asyncio
import json
import logging
import os
import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("crawler.backend")

app = FastAPI(
    title="Finance Crawler Backend",
    description="금융 데이터 레이크 크롤링 BE — Docker 잡 컨테이너 실행 + SSE 스트리밍",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# 잡 상태 저장소 (인메모리)
# ---------------------------------------------------------------------------

@dataclass
class CrawlJob:
    job_id: str
    status: str = "pending"          # pending | running | completed | failed
    events: List[dict] = field(default_factory=list)
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    total_uploaded: int = 0
    error: Optional[str] = None


_jobs: Dict[str, CrawlJob] = {}


# ---------------------------------------------------------------------------
# Request / Response 모델
# ---------------------------------------------------------------------------

VALID_SOURCES = ["naver", "daum", "krx", "upbit", "dart", "ecos", "yahoo"]
VALID_YAHOO_CATEGORIES = ["indices", "etfs", "stocks", "commodities", "fx"]
VALID_UPBIT_MARKETS = ["KRW", "BTC", "USDT"]


class CrawlRequest(BaseModel):
    sources: List[str] = Field(
        default=["naver", "daum", "krx"],
        description="수집 소스: naver daum krx upbit dart ecos yahoo",
    )
    markets: List[str] = Field(
        default=["KOSPI", "KOSDAQ"],
        description="국내 주식 시장: KOSPI KOSDAQ",
    )
    max_pages: int = Field(default=4, ge=1, le=50)
    qdrant_url: str = Field(default=os.getenv("QDRANT_URL", "http://qdrant:6333"))
    upbit_markets: List[str] = Field(
        default=["KRW"],
        description="업비트 마켓: KRW BTC USDT",
    )
    yahoo_categories: List[str] = Field(
        default=["indices", "etfs", "stocks", "commodities", "fx"],
        description="Yahoo Finance 카테고리",
    )


class StartResponse(BaseModel):
    job_id: str
    stream_url: str


# ---------------------------------------------------------------------------
# Docker 컨테이너 실행
# ---------------------------------------------------------------------------

CRAWLER_IMAGE = os.getenv("CRAWLER_IMAGE", "finance-crawler-job:latest")
USE_DOCKER = os.getenv("USE_DOCKER", "true").lower() == "true"


async def _build_recommendations(job: CrawlJob) -> None:
    """크롤링 완료 후 Gold 레이어를 재빌드하여 추천 종목을 갱신합니다."""
    await job.queue.put({"type": "gold_build_start", "message": "추천 종목 분석 중..."})
    try:
        from data_lake.gold import GoldLayer

        def _build():
            gold = GoldLayer()
            gold.build_all()
            gold.close()

        await asyncio.to_thread(_build)
        await job.queue.put({"type": "gold_build_done", "message": "추천 종목 분석 완료"})
    except Exception as exc:
        logger.warning("추천 종목 분석 실패: %s", exc)
        await job.queue.put({"type": "gold_build_done", "message": f"추천 종목 분석 실패: {exc}"})


async def _run_job_container(job: CrawlJob, req: CrawlRequest) -> None:
    job.status = "running"

    base_cmd = [
        "--sources", *req.sources,
        "--markets", *req.markets,
        "--max-pages", str(req.max_pages),
        "--qdrant-url", req.qdrant_url,
        "--upbit-markets", *req.upbit_markets,
        "--yahoo-categories", *req.yahoo_categories,
    ]

    if USE_DOCKER:
        cmd = [
            "docker", "run", "--rm",
            "--name", f"crawler-{job.job_id[:8]}",
            "--network", os.getenv("DOCKER_NETWORK", "shared-net"),
            "-e", f"QDRANT_URL={req.qdrant_url}",
            "-e", f"MINIO_ENDPOINT={os.getenv('MINIO_ENDPOINT', 'http://minio:9000')}",
            "-e", f"MINIO_ACCESS_KEY={os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}",
            "-e", f"MINIO_SECRET_KEY={os.getenv('MINIO_SECRET_KEY', 'minioadmin')}",
            "-e", f"DART_API_KEY={os.getenv('DART_API_KEY', '')}",
            "-e", f"ECOS_API_KEY={os.getenv('ECOS_API_KEY', '')}",
            CRAWLER_IMAGE,
            "python", "pipeline/crawler_job.py",
            *base_cmd,
        ]
    else:
        root = Path(__file__).parent.parent
        cmd = ["python", str(root / "pipeline" / "crawler_job.py"), *base_cmd]

    logger.info("잡 시작: %s → %s", job.job_id, " ".join(cmd[:6]) + " ...")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def _read_stdout():
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    event = {"type": "log", "message": line}

                job.events.append(event)
                await job.queue.put(event)

                if event.get("type") == "complete":
                    job.total_uploaded = event.get("total_uploaded", 0)

        async def _read_stderr():
            async for raw in proc.stderr:
                line = raw.decode("utf-8", errors="replace").strip()
                if line:
                    logger.debug("[job %s] STDERR: %s", job.job_id[:8], line)

        await asyncio.gather(_read_stdout(), _read_stderr())
        await proc.wait()

        job.status = "completed" if proc.returncode == 0 else "failed"
        if proc.returncode != 0:
            job.error = f"컨테이너 종료 코드: {proc.returncode}"
            await job.queue.put({"type": "error", "message": job.error})
        elif job.total_uploaded > 0:
            await _build_recommendations(job)

    except Exception as exc:
        logger.error("잡 실행 오류: %s", exc, exc_info=True)
        job.status = "failed"
        job.error = str(exc)
        await job.queue.put({"type": "error", "message": str(exc)})
    finally:
        await job.queue.put(None)


# ---------------------------------------------------------------------------
# SSE 이벤트 제너레이터
# ---------------------------------------------------------------------------

async def _sse_generator(job: CrawlJob) -> AsyncGenerator[str, None]:
    for event in job.events:
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    while True:
        event = await job.queue.get()
        if event is None:
            yield "data: {\"type\": \"stream_end\"}\n\n"
            break
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


# ---------------------------------------------------------------------------
# API Endpoints — Crawl
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "active_jobs": len(_jobs), "version": "3.0.0"}


@app.post("/crawl/start", response_model=StartResponse, tags=["crawl"])
async def start_crawl(req: CrawlRequest):
    """크롤링 잡을 시작합니다 (naver/daum/krx/upbit/dart/ecos/yahoo 지원)."""
    invalid = [s for s in req.sources if s not in VALID_SOURCES]
    if invalid:
        raise HTTPException(status_code=422, detail=f"지원하지 않는 소스: {invalid}")

    job_id = str(uuid.uuid4())
    job = CrawlJob(job_id=job_id)
    _jobs[job_id] = job

    asyncio.create_task(_run_job_container(job, req))

    return StartResponse(job_id=job_id, stream_url=f"/crawl/events/{job_id}")


@app.get("/crawl/events/{job_id}", tags=["crawl"])
async def crawl_events(job_id: str):
    """크롤링 진행 상황을 SSE로 스트리밍합니다."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="잡을 찾을 수 없습니다.")

    return StreamingResponse(
        _sse_generator(_jobs[job_id]),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/crawl/status/{job_id}", tags=["crawl"])
def crawl_status(job_id: str):
    """잡 최종 상태를 조회합니다."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="잡을 찾을 수 없습니다.")

    job = _jobs[job_id]
    return {
        "job_id": job_id,
        "status": job.status,
        "total_uploaded": job.total_uploaded,
        "error": job.error,
        "event_count": len(job.events),
    }


# ---------------------------------------------------------------------------
# API Endpoints — Data Lake
# ---------------------------------------------------------------------------

@app.get("/lake/catalog", tags=["data-lake"])
def lake_catalog():
    """데이터 레이크 카탈로그 (소스별 데이터셋 정의) 조회"""
    try:
        from data_lake.catalog import DataCatalog, DATASET_DEFINITIONS
        cat = DataCatalog()
        return {
            "datasets": DATASET_DEFINITIONS,
            "stats": cat.get_stats(),
            "sources": cat.list_sources(),
        }
    except Exception as exc:
        return {"datasets": {}, "error": str(exc)}


@app.get("/lake/stats", tags=["data-lake"])
def lake_stats():
    """데이터 레이크 수집 통계 조회"""
    try:
        from data_lake.catalog import DataCatalog
        cat = DataCatalog()
        return cat.get_stats()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/lake/history", tags=["data-lake"])
def lake_history(source: Optional[str] = None, limit: int = 50):
    """수집 실행 이력 조회"""
    try:
        from data_lake.catalog import DataCatalog
        cat = DataCatalog()
        return {"runs": cat.get_run_history(source=source, limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/lake/gold/build", tags=["data-lake"])
async def build_gold():
    """Gold 집계 레이어 빌드를 백그라운드로 트리거합니다."""
    async def _build():
        try:
            from data_lake.gold import GoldLayer
            gold = GoldLayer()
            gold.build_all()
            gold.close()
            logger.info("Gold 빌드 완료")
        except Exception as exc:
            logger.error("Gold 빌드 실패: %s", exc)

    asyncio.create_task(_build())
    return {"status": "triggered", "message": "Gold 레이어 빌드가 백그라운드에서 시작되었습니다."}


@app.get("/lake/recommendations", tags=["data-lake"])
def lake_recommendations(market: Optional[str] = None, limit: int = 15):
    """추천 종목 조회 (Gold 레이어 stock_recommendations 테이블)"""
    try:
        from data_lake.gold import GoldLayer
        gold = GoldLayer()
        sql = "SELECT * FROM stock_recommendations"
        params: List[str | int] = []
        if market:
            sql += " WHERE market = ?"
            params.append(market)
        sql += " ORDER BY recommendation_score DESC LIMIT ?"
        params.append(limit)
        df = gold.query(sql, params)
        gold.close()
        return {"data": df.to_dict(orient="records"), "count": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/lake/gold/query", tags=["data-lake"])
def gold_query(sql: str, limit: int = 100):
    """Gold DuckDB 직접 쿼리 (읽기 전용)"""
    if any(kw in sql.upper() for kw in ("DROP", "DELETE", "UPDATE", "INSERT", "ALTER")):
        raise HTTPException(status_code=400, detail="읽기 전용: SELECT 쿼리만 허용됩니다.")
    try:
        from data_lake.gold import GoldLayer
        gold = GoldLayer()
        df = gold.query(f"SELECT * FROM ({sql}) t LIMIT {limit}")
        gold.close()
        return {"data": df.to_dict(orient="records"), "count": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
