"""
Data Catalog — 데이터 레이크 메타데이터 관리

수집 이력, 데이터셋 통계, 스키마 정보를 JSON 파일로 관리합니다.
경로: /data/lake/catalog/catalog.json
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("data_lake.catalog")

CATALOG_PATH = Path(os.getenv("CATALOG_PATH", "/data/lake/catalog/catalog.json"))

# 데이터셋 정의 (소스 → 데이터 타입 → 스키마 요약)
DATASET_DEFINITIONS = {
    "naver": {
        "data_type": "stocks",
        "description": "네이버 금융 국내 주식 시세 (KOSPI/KOSDAQ)",
        "source_url": "https://finance.naver.com",
        "update_frequency": "daily",
        "key_fields": ["code", "name", "market", "current_price", "change_rate"],
        "requires_auth": False,
        "layer": ["bronze", "silver", "qdrant"],
    },
    "daum": {
        "data_type": "stocks",
        "description": "다음 금융 국내 주식 시세 (KOSPI/KOSDAQ)",
        "source_url": "https://finance.daum.net",
        "update_frequency": "daily",
        "key_fields": ["symbol_code", "name", "market", "current_price"],
        "requires_auth": False,
        "layer": ["bronze", "silver", "qdrant"],
    },
    "krx": {
        "data_type": "etf",
        "description": "한국거래소 ETF 정보",
        "source_url": "https://krx.co.kr",
        "update_frequency": "daily",
        "key_fields": ["code", "name", "market", "current_price"],
        "requires_auth": False,
        "layer": ["bronze", "silver", "qdrant"],
    },
    "upbit": {
        "data_type": "crypto",
        "description": "업비트 암호화폐 시세 (KRW/BTC/USDT 마켓)",
        "source_url": "https://api.upbit.com/v1",
        "update_frequency": "realtime",
        "key_fields": ["market", "base_currency", "trade_price", "change_rate"],
        "requires_auth": False,
        "layer": ["bronze", "silver", "gold"],
    },
    "dart": {
        "data_type": "disclosures",
        "description": "금융감독원 DART 전자공시 (사업보고서, 주요사항보고 등)",
        "source_url": "https://opendart.fss.or.kr",
        "update_frequency": "daily",
        "key_fields": ["corp_code", "corp_name", "report_nm", "rcept_dt"],
        "requires_auth": True,
        "auth_env": "DART_API_KEY",
        "layer": ["bronze", "silver"],
    },
    "ecos": {
        "data_type": "macro",
        "description": "한국은행 경제통계 (기준금리, GDP, CPI, 환율 등)",
        "source_url": "https://ecos.bok.or.kr/api",
        "update_frequency": "monthly",
        "key_fields": ["stat_code", "stat_name", "time", "data_value"],
        "requires_auth": True,
        "auth_env": "ECOS_API_KEY",
        "layer": ["bronze", "silver", "gold"],
    },
    "yahoo": {
        "data_type": "global",
        "description": "Yahoo Finance 글로벌 시장 (지수/ETF/미국주식/원자재/환율)",
        "source_url": "https://finance.yahoo.com",
        "update_frequency": "daily",
        "key_fields": ["ticker", "name", "category", "current_price", "change_percent"],
        "requires_auth": False,
        "layer": ["bronze", "silver", "gold"],
    },
}


class DataCatalog:
    """데이터 레이크 카탈로그"""

    def __init__(self, path: Optional[Path] = None):
        self._path = path or CATALOG_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"datasets": DATASET_DEFINITIONS.copy(), "runs": [], "stats": {}}

    def _save(self):
        self._path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def record_run(
        self,
        source: str,
        data_type: str,
        record_count: int,
        storage_path: str,
        status: str = "success",
        error: Optional[str] = None,
        duration_seconds: float = 0.0,
    ) -> None:
        """수집 실행 이력 기록"""
        entry = {
            "source": source,
            "data_type": data_type,
            "record_count": record_count,
            "storage_path": storage_path,
            "status": status,
            "error": error,
            "duration_seconds": round(duration_seconds, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._data.setdefault("runs", []).append(entry)

        # 소스별 통계 갱신
        stats = self._data.setdefault("stats", {})
        src_stats = stats.setdefault(source, {})
        src_stats["last_run"] = entry["timestamp"]
        src_stats["last_status"] = status
        src_stats["total_runs"] = src_stats.get("total_runs", 0) + 1
        if status == "success":
            src_stats["last_record_count"] = record_count
            src_stats["total_records"] = src_stats.get("total_records", 0) + record_count

        self._save()
        logger.info("카탈로그 기록: source=%s, %d건, status=%s", source, record_count, status)

    def get_dataset_info(self, source: str) -> Optional[dict]:
        return self._data.get("datasets", {}).get(source)

    def get_run_history(self, source: Optional[str] = None, limit: int = 20) -> list:
        runs = self._data.get("runs", [])
        if source:
            runs = [r for r in runs if r.get("source") == source]
        return sorted(runs, key=lambda r: r.get("timestamp", ""), reverse=True)[:limit]

    def get_stats(self) -> dict:
        return self._data.get("stats", {})

    def list_sources(self) -> list:
        return list(DATASET_DEFINITIONS.keys())

    def print_summary(self) -> None:
        stats = self.get_stats()
        print(f"\n{'소스':<12} {'마지막 수집':<26} {'상태':<10} {'레코드수':>10}")
        print("-" * 62)
        for source, info in DATASET_DEFINITIONS.items():
            s = stats.get(source, {})
            last = s.get("last_run", "-")[:19] if s.get("last_run") else "-"
            status = s.get("last_status", "-")
            count = f"{s.get('last_record_count', 0):,}" if s.get("last_record_count") else "-"
            auth = " [API키필요]" if info.get("requires_auth") else ""
            print(f"{source + auth:<12} {last:<26} {status:<10} {count:>10}")
        print()
