"""
Gold Layer — 분석 집계 영역 (Analytics Zone)

Silver 데이터를 읽어 투자 분석에 바로 쓸 수 있는 집계 테이블을 DuckDB에 생성합니다.
DuckDB 파일: /data/lake/gold/analytics.duckdb (로컬) 또는 MinIO Parquet

주요 집계 테이블:
    market_summary        시장별 종목 요약 (KOSPI/KOSDAQ/ETF/CRYPTO)
    top_movers            등락률 상·하위 종목
    sector_heatmap        섹터별 평균 수익률
    macro_snapshot        최신 거시경제 지표 스냅샷
    global_market_pulse   글로벌 시장 현황 대시보드
    stock_recommendations 저평가·우량·모멘텀 기반 추천 종목
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("data_lake.gold")

GOLD_DB_PATH = Path(os.getenv("GOLD_DB_PATH", "/data/lake/gold/analytics.duckdb"))
DATA_LAKE_LOCAL = os.getenv("DATA_LAKE_LOCAL", "false").lower() == "true"
LOCAL_ROOT = Path(os.getenv("LOCAL_DATA_ROOT", "/data/lake"))

BUCKET_SILVER = os.getenv("BUCKET_SILVER", "data-lake-silver")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


class GoldLayer:
    """분석 집계 레이어 — DuckDB 기반"""

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or GOLD_DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            try:
                import duckdb
                self._conn = duckdb.connect(str(self._db_path))
                self._setup_extensions()
            except ImportError:
                raise ImportError("duckdb 패키지가 필요합니다: pip install duckdb")
        return self._conn

    def _setup_extensions(self):
        conn = self._conn
        conn.execute("INSTALL parquet; LOAD parquet;")
        if not DATA_LAKE_LOCAL:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"""
                SET s3_endpoint='{MINIO_ENDPOINT.replace("http://", "").replace("https://", "")}';
                SET s3_access_key_id='{MINIO_ACCESS_KEY}';
                SET s3_secret_access_key='{MINIO_SECRET_KEY}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)

    def _silver_glob(self, data_type: str) -> str:
        if DATA_LAKE_LOCAL:
            base = LOCAL_ROOT / "silver" / data_type
            return str(base / "**" / "*.parquet")
        return f"s3://{BUCKET_SILVER}/{data_type}/**/*.parquet"

    def build_market_summary(self) -> None:
        """시장별 종목 요약 집계"""
        conn = self._get_conn()
        path = self._silver_glob("stocks")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE market_summary AS
                SELECT
                    market,
                    COUNT(*) AS stock_count,
                    AVG(current_price) AS avg_price,
                    AVG(change_rate) AS avg_change_rate,
                    SUM(market_cap) AS total_market_cap,
                    AVG(per) AS avg_per,
                    AVG(pbr) AS avg_pbr,
                    AVG(roe) AS avg_roe,
                    AVG(dividend_yield) AS avg_dividend_yield,
                    MAX(crawled_at) AS last_updated
                FROM read_parquet('{path}', union_by_name=true)
                WHERE market IS NOT NULL AND market != ''
                GROUP BY market
                ORDER BY total_market_cap DESC
            """)
            logger.info("Gold 집계: market_summary 완료")
        except Exception as exc:
            logger.warning("market_summary 빌드 실패 (데이터 없음): %s", exc)

    def build_top_movers(self, top_n: int = 20) -> None:
        """등락률 상·하위 종목"""
        conn = self._get_conn()
        path = self._silver_glob("stocks")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE top_movers AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY market ORDER BY change_rate DESC) AS rank_up,
                        ROW_NUMBER() OVER (PARTITION BY market ORDER BY change_rate ASC)  AS rank_down
                    FROM read_parquet('{path}', union_by_name=true)
                    WHERE change_rate IS NOT NULL
                )
                SELECT market, code, name, current_price, change_rate,
                       volume, market_cap, crawled_at,
                       CASE WHEN rank_up <= {top_n} THEN 'GAINER' ELSE 'LOSER' END AS direction
                FROM ranked
                WHERE rank_up <= {top_n} OR rank_down <= {top_n}
                ORDER BY market, direction, ABS(change_rate) DESC
            """)
            logger.info("Gold 집계: top_movers 완료")
        except Exception as exc:
            logger.warning("top_movers 빌드 실패: %s", exc)

    def build_stock_recommendations(self, top_n: int = 15) -> None:
        """저평가(PER/PBR) + 우량(ROE) + 배당 + 모멘텀 기반 추천 종목 스코어링"""
        conn = self._get_conn()
        path = self._silver_glob("stocks")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE stock_recommendations AS
                WITH base AS (
                    SELECT *
                    FROM read_parquet('{path}', union_by_name=true)
                    WHERE per IS NOT NULL AND per > 0
                      AND pbr IS NOT NULL AND pbr > 0
                      AND roe IS NOT NULL
                      AND market IS NOT NULL AND market != ''
                ),
                ranked AS (
                    SELECT *,
                        PERCENT_RANK() OVER (PARTITION BY market ORDER BY per ASC) AS per_rank,
                        PERCENT_RANK() OVER (PARTITION BY market ORDER BY pbr ASC) AS pbr_rank,
                        PERCENT_RANK() OVER (PARTITION BY market ORDER BY roe DESC) AS roe_rank,
                        PERCENT_RANK() OVER (PARTITION BY market ORDER BY COALESCE(dividend_yield, 0) DESC) AS div_rank,
                        PERCENT_RANK() OVER (PARTITION BY market ORDER BY COALESCE(change_rate, 0) DESC) AS momentum_rank
                    FROM base
                ),
                scored AS (
                    SELECT *,
                        ROUND(
                            (1 - per_rank) * 25
                          + (1 - pbr_rank) * 20
                          + (1 - roe_rank) * 25
                          + (1 - div_rank) * 15
                          + (1 - momentum_rank) * 15
                        , 1) AS recommendation_score,
                        TRIM(
                            CASE WHEN per_rank <= 0.2 THEN '저PER ' ELSE '' END ||
                            CASE WHEN pbr_rank <= 0.2 THEN '저PBR ' ELSE '' END ||
                            CASE WHEN roe_rank <= 0.2 THEN '고ROE ' ELSE '' END ||
                            CASE WHEN div_rank <= 0.2 THEN '고배당 ' ELSE '' END ||
                            CASE WHEN momentum_rank <= 0.2 THEN '상승모멘텀' ELSE '' END
                        ) AS reason
                    FROM ranked
                ),
                final AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY market ORDER BY recommendation_score DESC) AS rnk
                    FROM scored
                )
                SELECT market, code, name, current_price, change_rate, per, pbr, roe,
                       dividend_yield, market_cap, volume, recommendation_score, reason, crawled_at
                FROM final
                WHERE rnk <= {top_n}
                ORDER BY market, recommendation_score DESC
            """)
            logger.info("Gold 집계: stock_recommendations 완료")
        except Exception as exc:
            logger.warning("stock_recommendations 빌드 실패: %s", exc)

    def build_macro_snapshot(self) -> None:
        """최신 거시경제 지표 스냅샷"""
        conn = self._get_conn()
        path = self._silver_glob("macro")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE macro_snapshot AS
                WITH latest AS (
                    SELECT stat_code, stat_name, unit_name,
                           MAX(time) AS latest_time
                    FROM read_parquet('{path}', union_by_name=true)
                    GROUP BY stat_code, stat_name, unit_name
                )
                SELECT m.stat_code, m.stat_name, m.time, m.data_value,
                       m.unit_name, m.crawled_at
                FROM read_parquet('{path}', union_by_name=true) m
                JOIN latest l ON m.stat_code = l.stat_code
                    AND m.time = l.latest_time
                ORDER BY m.stat_code
            """)
            logger.info("Gold 집계: macro_snapshot 완료")
        except Exception as exc:
            logger.warning("macro_snapshot 빌드 실패: %s", exc)

    def build_global_market_pulse(self) -> None:
        """글로벌 시장 현황 대시보드"""
        conn = self._get_conn()
        path = self._silver_glob("global")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE global_market_pulse AS
                SELECT
                    ticker, name, category, region, currency,
                    current_price, change_percent,
                    CASE WHEN change_percent > 2  THEN 'strong_up'
                         WHEN change_percent > 0  THEN 'up'
                         WHEN change_percent < -2 THEN 'strong_down'
                         WHEN change_percent < 0  THEN 'down'
                         ELSE 'flat' END AS signal,
                    volume, market_cap, pe_ratio, crawled_at
                FROM read_parquet('{path}', union_by_name=true)
                ORDER BY category, ABS(change_percent) DESC
            """)
            logger.info("Gold 집계: global_market_pulse 완료")
        except Exception as exc:
            logger.warning("global_market_pulse 빌드 실패: %s", exc)

    def build_crypto_summary(self) -> None:
        """암호화폐 시장 요약"""
        conn = self._get_conn()
        path = self._silver_glob("crypto")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE crypto_summary AS
                SELECT
                    quote_currency AS market,
                    COUNT(*) AS coin_count,
                    SUM(acc_trade_price_24h) AS total_trade_value_24h,
                    AVG(change_rate) AS avg_change_rate,
                    MAX(CASE WHEN change_rate = MAX(change_rate) OVER ()
                             THEN base_currency END) AS top_gainer,
                    MAX(CASE WHEN change_rate = MIN(change_rate) OVER ()
                             THEN base_currency END) AS top_loser,
                    MAX(crawled_at) AS last_updated
                FROM read_parquet('{path}', union_by_name=true)
                GROUP BY quote_currency
                ORDER BY total_trade_value_24h DESC NULLS LAST
            """)
            logger.info("Gold 집계: crypto_summary 완료")
        except Exception as exc:
            logger.warning("crypto_summary 빌드 실패: %s", exc)

    def build_all(self) -> None:
        """모든 Gold 집계 테이블 빌드"""
        logger.info("Gold Layer 전체 빌드 시작")
        self.build_market_summary()
        self.build_top_movers()
        self.build_stock_recommendations()
        self.build_macro_snapshot()
        self.build_global_market_pulse()
        self.build_crypto_summary()
        logger.info("Gold Layer 전체 빌드 완료: %s", self._db_path)

    def query(self, sql: str, params: Optional[list] = None) -> "pd.DataFrame":
        """Gold DB 직접 쿼리 (params 전달 시 파라미터 바인딩 쿼리)"""
        conn = self._get_conn()
        if params:
            return conn.execute(sql, params).df()
        return conn.execute(sql).df()

    def list_tables(self) -> list:
        conn = self._get_conn()
        return [r[0] for r in conn.execute("SHOW TABLES").fetchall()]

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
