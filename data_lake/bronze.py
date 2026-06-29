"""
Bronze Layer — 원본 데이터 착지 영역 (Raw Zone)

수집된 원본 데이터를 변형 없이 JSON Lines 형태로 MinIO(S3)에 저장합니다.
경로 규칙: bronze/{source}/{data_type}/{year}/{month}/{day}/{timestamp}.jsonl

MinIO 없이 로컬 파일시스템에도 쓸 수 있습니다 (DATA_LAKE_LOCAL=true).
"""

import io
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Union

logger = logging.getLogger("data_lake.bronze")

BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "data-lake-bronze")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATA_LAKE_LOCAL = os.getenv("DATA_LAKE_LOCAL", "false").lower() == "true"
LOCAL_ROOT = Path(os.getenv("LOCAL_DATA_ROOT", "/data/lake"))


class BronzeLayer:
    """
    원본 데이터 저장 레이어.

    MinIO가 없으면 DATA_LAKE_LOCAL=true 환경변수로 로컬 파일시스템을 사용하세요.
    """

    def __init__(self):
        self._client = None
        self._use_local = DATA_LAKE_LOCAL

        if not self._use_local:
            try:
                self._client = self._build_minio_client()
                self._ensure_bucket(BUCKET_BRONZE)
            except Exception as exc:
                logger.warning("MinIO 연결 실패 — 로컬 파일시스템으로 전환: %s", exc)
                self._use_local = True

    def _build_minio_client(self):
        from minio import Minio
        endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        secure = MINIO_ENDPOINT.startswith("https")
        return Minio(endpoint, access_key=MINIO_ACCESS_KEY,
                     secret_key=MINIO_SECRET_KEY, secure=secure)

    def _ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info("MinIO 버킷 생성: %s", bucket)

    def _make_object_key(self, source: str, data_type: str,
                         ts: datetime) -> str:
        return (
            f"{source}/{data_type}/"
            f"{ts.year:04d}/{ts.month:02d}/{ts.day:02d}/"
            f"{ts.strftime('%H%M%S%f')}.jsonl"
        )

    def write(
        self,
        records: List[Any],
        source: str,
        data_type: str,
        metadata: Optional[dict] = None,
    ) -> str:
        """
        레코드 목록을 Bronze 레이어에 기록합니다.

        Parameters
        ----------
        records   : dataclass 또는 dict 목록
        source    : 데이터 출처 (naver / upbit / dart / ecos / yahoo)
        data_type : 데이터 종류 (stocks / crypto / disclosures / macro / global)
        metadata  : 추가 메타데이터

        Returns
        -------
        저장 경로 (S3 URI 또는 로컬 경로)
        """
        ts = datetime.now(timezone.utc)
        object_key = self._make_object_key(source, data_type, ts)

        lines = []
        for rec in records:
            row = asdict(rec) if hasattr(rec, "__dataclass_fields__") else rec
            row.setdefault("_source", source)
            row.setdefault("_data_type", data_type)
            row.setdefault("_ingested_at", ts.isoformat())
            if metadata:
                row["_meta"] = metadata
            lines.append(json.dumps(row, ensure_ascii=False))

        content = "\n".join(lines) + "\n"

        if self._use_local:
            path = LOCAL_ROOT / "bronze" / object_key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            logger.info("Bronze[local] 저장: %s (%d건)", path, len(records))
            return str(path)
        else:
            data = content.encode("utf-8")
            self._client.put_object(
                BUCKET_BRONZE,
                object_key,
                io.BytesIO(data),
                length=len(data),
                content_type="application/x-ndjson",
            )
            uri = f"s3://{BUCKET_BRONZE}/{object_key}"
            logger.info("Bronze[minio] 저장: %s (%d건)", uri, len(records))
            return uri

    def list_files(self, source: str = "", data_type: str = "",
                   date_prefix: str = "") -> List[str]:
        """Bronze 레이어 파일 목록 조회"""
        prefix = "/".join(filter(None, [source, data_type, date_prefix])) + "/"

        if self._use_local:
            base = LOCAL_ROOT / "bronze" / prefix.strip("/")
            if not base.exists():
                return []
            return [str(p) for p in sorted(base.rglob("*.jsonl"))]
        else:
            return [
                obj.object_name
                for obj in self._client.list_objects(
                    BUCKET_BRONZE, prefix=prefix, recursive=True
                )
            ]

    def read(self, path: str) -> List[dict]:
        """Bronze 파일 읽기 (로컬 또는 S3)"""
        if self._use_local or path.startswith("/"):
            lines = Path(path).read_text(encoding="utf-8").splitlines()
        else:
            object_key = path.replace(f"s3://{BUCKET_BRONZE}/", "")
            resp = self._client.get_object(BUCKET_BRONZE, object_key)
            lines = resp.read().decode("utf-8").splitlines()

        return [json.loads(line) for line in lines if line.strip()]
