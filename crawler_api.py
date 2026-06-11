"""
Finance Crawler Backend API

FE에서 호출하면 Docker 컨테이너로 크롤링 잡을 실행하고,
실행 상태를 SSE(Server-Sent Events)로 FE에 실시간 스트리밍합니다.

Endpoints:
    GET  /health                  상태 확인
    POST /crawl/start             크롤링 잡 시작 → job_id 반환
    GET  /crawl/events/{job_id}   SSE 스트림 (진행 상황 실시간)
    GET  /crawl/status/{job_id}   잡 최종 상태 조회
"""

import asyncio
import json
import logging
import os
import subprocess
import uuid
from dataclasses import dataclass, field
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
    description="금융 크롤링 BE — Docker 잡 컨테이너 실행 + SSE 스트리밍",
    version="2.0.0",
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

class CrawlRequest(BaseModel):
    sources: List[str] = Field(default=["naver", "daum", "krx"])
    markets: List[str] = Field(default=["KOSPI", "KOSDAQ"])
    max_pages: int = Field(default=4, ge=1, le=50)
    qdrant_url: str = Field(default=os.getenv("QDRANT_URL", "http://qdrant:6333"))


class StartResponse(BaseModel):
    job_id: str
    stream_url: str


# ---------------------------------------------------------------------------
# Docker 컨테이너 실행 및 stdout → 이벤트 큐
# ---------------------------------------------------------------------------

CRAWLER_IMAGE = os.getenv("CRAWLER_IMAGE", "finance-crawler-job:latest")
USE_DOCKER = os.getenv("USE_DOCKER", "true").lower() == "true"


async def _run_job_container(job: CrawlJob, req: CrawlRequest) -> None:
    """
    finance-crawler-job 컨테이너를 실행하고 stdout JSON Lines를
    job.queue에 넣어 SSE 스트림으로 전달합니다.

    USE_DOCKER=false 이면 로컬 Python으로 직접 실행합니다 (개발용).
    """
    job.status = "running"

    if USE_DOCKER:
        cmd = [
            "docker", "run", "--rm",
            "--name", f"crawler-{job.job_id[:8]}",
            "--network", os.getenv("DOCKER_NETWORK", "shared-net"),
            "-e", f"QDRANT_URL={req.qdrant_url}",
            CRAWLER_IMAGE,
            "--sources", *req.sources,
            "--markets", *req.markets,
            "--max-pages", str(req.max_pages),
            "--qdrant-url", req.qdrant_url,
        ]
    else:
        cmd = [
            "python", "crawler_job.py",
            "--sources", *req.sources,
            "--markets", *req.markets,
            "--max-pages", str(req.max_pages),
            "--qdrant-url", req.qdrant_url,
        ]

    logger.info("잡 시작: %s → %s", job.job_id, " ".join(cmd))

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

        if proc.returncode == 0:
            job.status = "completed"
        else:
            job.status = "failed"
            job.error = f"컨테이너 종료 코드: {proc.returncode}"
            await job.queue.put({"type": "error", "message": job.error})

    except Exception as exc:
        logger.error("잡 실행 오류: %s", exc, exc_info=True)
        job.status = "failed"
        job.error = str(exc)
        await job.queue.put({"type": "error", "message": str(exc)})

    finally:
        # SSE 스트림 종료 신호
        await job.queue.put(None)


# ---------------------------------------------------------------------------
# SSE 이벤트 제너레이터
# ---------------------------------------------------------------------------

async def _sse_generator(job: CrawlJob) -> AsyncGenerator[str, None]:
    # 이미 누적된 이벤트 먼저 전송 (늦게 연결한 경우)
    for event in job.events:
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    # 새 이벤트 실시간 전송
    while True:
        event = await job.queue.get()
        if event is None:           # 종료 신호
            yield "data: {\"type\": \"stream_end\"}\n\n"
            break
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "active_jobs": len(_jobs)}


@app.post("/crawl/start", response_model=StartResponse, tags=["crawl"])
async def start_crawl(req: CrawlRequest):
    """
    크롤링 잡을 시작합니다.

    - Docker 컨테이너(finance-crawler-job)를 실행합니다.
    - job_id와 SSE 스트림 URL을 반환합니다.
    - FE는 stream_url로 GET 요청하여 실시간 진행 상황을 수신합니다.
    """
    job_id = str(uuid.uuid4())
    job = CrawlJob(job_id=job_id)
    _jobs[job_id] = job

    # 백그라운드에서 컨테이너 실행
    asyncio.create_task(_run_job_container(job, req))

    stream_url = f"/crawl/events/{job_id}"
    logger.info("크롤링 잡 생성: %s", job_id)
    return StartResponse(job_id=job_id, stream_url=stream_url)


@app.get("/crawl/events/{job_id}", tags=["crawl"])
async def crawl_events(job_id: str):
    """
    크롤링 진행 상황을 SSE로 스트리밍합니다.

    FE에서 EventSource('/crawl/events/{job_id}')로 연결하세요.

    이벤트 타입:
        init       - 잡 파라미터 확인
        qdrant_ok  - Qdrant 연결 성공
        start      - 소스/시장 크롤링 시작
        page       - 페이지 수집 완료
        enrich     - API 보강 진행
        upload     - Qdrant 업로드 진행
        done       - 소스/시장 완료
        complete   - 전체 완료 (total_uploaded, duration_seconds)
        error      - 오류 발생
        stream_end - SSE 스트림 종료
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="잡을 찾을 수 없습니다.")

    job = _jobs[job_id]
    return StreamingResponse(
        _sse_generator(job),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",     # Nginx 버퍼링 비활성화
        },
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
