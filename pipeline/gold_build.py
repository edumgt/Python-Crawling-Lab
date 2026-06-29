"""
Gold Layer 집계 빌드 스크립트 (독립 실행용)

Silver 레이어 데이터를 읽어 DuckDB Gold 집계 테이블을 빌드합니다.

Usage:
    python pipeline/gold_build.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("gold.build")


def main():
    try:
        from data_lake.gold import GoldLayer
        gold = GoldLayer()
        gold.build_all()
        tables = gold.list_tables()
        logger.info("빌드된 테이블: %s", tables)
        gold.close()
        logger.info("Gold 빌드 완료")
    except ImportError as exc:
        logger.error("의존성 오류 — duckdb/pyarrow 설치 필요: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Gold 빌드 실패: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
