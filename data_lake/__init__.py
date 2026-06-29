"""
Data Lake — Bronze / Silver / Gold 3-layer 아키텍처

Bronze : 원본 JSON 그대로 저장 (MinIO S3)
Silver : 정제·정규화된 Parquet 저장 (MinIO S3)
Gold   : 분석용 집계 테이블 (DuckDB / Parquet)
"""

from data_lake.bronze import BronzeLayer
from data_lake.silver import SilverLayer
from data_lake.gold import GoldLayer
from data_lake.catalog import DataCatalog

__all__ = ["BronzeLayer", "SilverLayer", "GoldLayer", "DataCatalog"]
