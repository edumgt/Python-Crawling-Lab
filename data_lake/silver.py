"""
Silver Layer — 정제·정규화 영역 (Processed Zone)

Bronze 원본 데이터를 읽어 타입 변환, 컬럼 정규화, 중복 제거 후
Parquet 형식으로 MinIO(S3)에 저장합니다.
경로 규칙: silver/{data_type}/{year}/{month}/{day}/data.parquet
"""

import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("data_lake.silver")

BUCKET_SILVER = os.getenv("BUCKET_SILVER", "data-lake-silver")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATA_LAKE_LOCAL = os.getenv("DATA_LAKE_LOCAL", "false").lower() == "true"
LOCAL_ROOT = Path(os.getenv("LOCAL_DATA_ROOT", "/data/lake"))


# Silver 스키마 정의 — 소스별 컬럼 매핑
SCHEMA_MAP = {
    "stocks": {
        "required": ["code", "name", "market", "current_price", "change_rate",
                     "volume", "market_cap", "crawled_at"],
        "numeric": ["current_price", "change_rate", "volume", "trading_value",
                    "market_cap", "foreign_ratio", "per", "pbr", "eps",
                    "roe", "dividend_yield"],
        "dedup_key": ["code", "market"],
    },
    "crypto": {
        "required": ["market", "base_currency", "quote_currency",
                     "trade_price", "change_rate", "crawled_at"],
        "numeric": ["trade_price", "change_rate", "change_price",
                    "high_price", "low_price", "acc_trade_volume_24h",
                    "acc_trade_price_24h"],
        "dedup_key": ["market"],
    },
    "disclosures": {
        "required": ["corp_code", "corp_name", "report_nm", "rcept_dt", "crawled_at"],
        "numeric": [],
        "dedup_key": ["rcept_no"],
    },
    "macro": {
        "required": ["stat_code", "stat_name", "time", "data_value", "crawled_at"],
        "numeric": ["data_value"],
        "dedup_key": ["stat_code", "time"],
    },
    "global": {
        "required": ["ticker", "name", "category", "current_price", "crawled_at"],
        "numeric": ["current_price", "change", "change_percent", "volume",
                    "market_cap", "pe_ratio", "dividend_yield"],
        "dedup_key": ["ticker"],
    },
}


class SilverLayer:
    """정제된 데이터 저장 레이어"""

    def __init__(self):
        self._client = None
        self._use_local = DATA_LAKE_LOCAL

        if not self._use_local:
            try:
                self._client = self._build_minio_client()
                self._ensure_bucket(BUCKET_SILVER)
            except Exception as exc:
                logger.warning("MinIO 연결 실패 — 로컬 전환: %s", exc)
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

    def _make_object_key(self, data_type: str, ts: datetime) -> str:
        return (
            f"{data_type}/"
            f"{ts.year:04d}/{ts.month:02d}/{ts.day:02d}/data.parquet"
        )

    def transform(self, records: List[dict], data_type: str) -> "pd.DataFrame":
        """Bronze 레코드를 Silver 스키마로 변환 (pandas DataFrame 반환)"""
        import pandas as pd

        schema = SCHEMA_MAP.get(data_type, {})
        numeric_cols = schema.get("numeric", [])
        dedup_key = schema.get("dedup_key", [])

        df = pd.DataFrame(records)

        # 내부 메타 컬럼 분리 보존
        meta_cols = [c for c in df.columns if c.startswith("_")]
        for col in meta_cols:
            df[col] = df[col].astype(str)

        # 숫자 컬럼 강제 변환
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 문자열 컬럼 정리
        str_cols = [c for c in df.columns if c not in numeric_cols and c not in meta_cols]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # crawled_at 파싱
        if "crawled_at" in df.columns:
            df["crawled_at"] = pd.to_datetime(df["crawled_at"], errors="coerce", utc=True)

        # 중복 제거 (최신 데이터 유지)
        if dedup_key and all(k in df.columns for k in dedup_key):
            if "crawled_at" in df.columns:
                df = df.sort_values("crawled_at", ascending=False)
            df = df.drop_duplicates(subset=dedup_key, keep="first")

        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("Silver 변환: data_type=%s, rows=%d", data_type, len(df))
        return df

    def write(self, records: List[dict], data_type: str,
              partition_dt: Optional[datetime] = None) -> str:
        """변환된 데이터를 Silver 레이어에 Parquet으로 저장"""
        import pyarrow as pa
        import pyarrow.parquet as pq

        ts = partition_dt or datetime.now(timezone.utc)
        object_key = self._make_object_key(data_type, ts)
        df = self.transform(records, data_type)
        table = pa.Table.from_pandas(df)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        data = buf.getvalue()

        if self._use_local:
            path = LOCAL_ROOT / "silver" / object_key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            logger.info("Silver[local] 저장: %s (%d행)", path, len(df))
            return str(path)
        else:
            self._client.put_object(
                BUCKET_SILVER, object_key,
                io.BytesIO(data), length=len(data),
                content_type="application/octet-stream",
            )
            uri = f"s3://{BUCKET_SILVER}/{object_key}"
            logger.info("Silver[minio] 저장: %s (%d행)", uri, len(df))
            return uri

    def read(self, data_type: str, date_range: Optional[tuple] = None) -> "pd.DataFrame":
        """Silver 레이어 Parquet 읽기 (날짜 범위 선택적)"""
        import pandas as pd
        import pyarrow.parquet as pq

        if self._use_local:
            base = LOCAL_ROOT / "silver" / data_type
            paths = sorted(base.rglob("data.parquet")) if base.exists() else []
            if not paths:
                return pd.DataFrame()
            frames = [pq.read_table(str(p)).to_pandas() for p in paths]
            df = pd.concat(frames, ignore_index=True)
        else:
            prefix = f"{data_type}/"
            objects = list(self._client.list_objects(
                BUCKET_SILVER, prefix=prefix, recursive=True
            ))
            frames = []
            for obj in objects:
                resp = self._client.get_object(BUCKET_SILVER, obj.object_name)
                buf = io.BytesIO(resp.read())
                frames.append(pq.read_table(buf).to_pandas())
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        if date_range and "crawled_at" in df.columns:
            start, end = date_range
            df = df[(df["crawled_at"] >= start) & (df["crawled_at"] <= end)]

        return df
