<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.2/css/all.min.css" />

# <i class="fa-solid fa-chart-line"></i> Python Finance Crawling Lab

금융·주식 투자 데이터를 수집하고, **데이터 레이크(Data Lake) 아키텍처** 기반으로 저장·분석하는 실습 저장소입니다.  
국내 주식(네이버/다음/KRX), 암호화폐(업비트), DART 공시, 한국은행 거시경제 지표, 글로벌 시장(Yahoo Finance)을  
**Bronze → Silver → Gold** 3-레이어 파이프라인으로 처리합니다.

---

## <i class="fa-solid fa-rocket"></i> 빠른 시작

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 국내 주식 크롤링 + Qdrant 저장
python pipeline/crawler_job.py --sources naver krx --markets KOSPI KOSDAQ

# 업비트 암호화폐 추가
python pipeline/crawler_job.py --sources upbit --upbit-markets KRW

# 글로벌 시장 (yfinance)
python pipeline/crawler_job.py --sources yahoo --yahoo-categories indices etfs

# Docker 전체 스택 실행 (MinIO + Qdrant + UI + API)
docker compose up -d
```

> Qdrant 대시보드: http://localhost:6333/dashboard  
> MinIO 콘솔: http://localhost:9001 (minioadmin/minioadmin)  
> Crawler API: http://localhost:8400/docs  
> UI: http://localhost:8899

---

## <i class="fa-solid fa-database"></i> 데이터 레이크 아키텍처

```
원본 수집                  정제·정규화                 분석 집계
─────────────────────────────────────────────────────────────────
크롤러        →  Bronze      →  Silver        →  Gold
(JSON 원본)      (MinIO S3      (Parquet,          (DuckDB,
                 JSONL)         MinIO S3)          집계 테이블)
```

### Bronze Layer (원본 착지)
- 수집 원본을 **변형 없이** JSONL 형태로 MinIO에 저장
- 경로: `s3://data-lake-bronze/{source}/{data_type}/{YYYY}/{MM}/{DD}/{timestamp}.jsonl`
- 재처리/감사(Audit) 기반

### Silver Layer (정제·정규화)
- 타입 변환, 컬럼 정규화, 중복 제거 후 **Parquet** 저장
- 경로: `s3://data-lake-silver/{data_type}/{YYYY}/{MM}/{DD}/data.parquet`
- 스키마 정의: `data_lake/silver.py::SCHEMA_MAP`

### Gold Layer (분석 집계)
- Silver Parquet을 DuckDB로 집계 → 즉시 쿼리 가능한 뷰
- 주요 집계 테이블:

| 테이블 | 설명 |
|--------|------|
| `market_summary` | 시장별 종목 요약 (KOSPI/KOSDAQ) |
| `top_movers` | 등락률 상·하위 종목 |
| `macro_snapshot` | 최신 거시경제 지표 스냅샷 |
| `global_market_pulse` | 글로벌 시장 현황 |
| `crypto_summary` | 암호화폐 시장 요약 |

```python
from data_lake.gold import GoldLayer
gold = GoldLayer()
gold.build_all()
df = gold.query("SELECT * FROM top_movers WHERE direction='GAINER' LIMIT 10")
```

---

## <i class="fa-solid fa-spider"></i> 크롤링 소스

| 소스 | 대상 | 인증 | 저장 레이어 |
|------|------|------|------------|
| `naver` | 네이버 금융 KOSPI/KOSDAQ 주식 | 불필요 | Bronze → Silver → Qdrant |
| `daum` | 다음 금융 KOSPI/KOSDAQ 주식 | 불필요 | Bronze → Silver → Qdrant |
| `krx` | 한국거래소 ETF 정보 | 불필요 | Bronze → Silver → Qdrant |
| `upbit` | 업비트 암호화폐 (KRW/BTC/USDT) | 불필요 | Bronze → Silver → Gold → Qdrant |
| `dart` | 금융감독원 DART 전자공시 | `DART_API_KEY` | Bronze → Silver |
| `ecos` | 한국은행 경제통계 (금리/GDP/CPI/환율) | `ECOS_API_KEY` | Bronze → Silver → Gold |
| `yahoo` | 글로벌 지수/ETF/테크주/원자재/환율 | 불필요 | Bronze → Silver → Gold → Qdrant |

### API 키 발급
- **DART**: [opendart.fss.or.kr](https://opendart.fss.or.kr) → 인증키 신청
- **ECOS**: [ecos.bok.or.kr/openapi](https://ecos.bok.or.kr/openapi) → 서비스 신청

---

## <i class="fa-solid fa-folder-tree"></i> 저장소 구조

```
python-crawling-lab/
│
├── crawlers/                     # 도메인별 크롤러
│   ├── finance.naver.com/        # 네이버 금융 (국내 주식)
│   ├── finance.daum.net/         # 다음 금융 (국내 주식)
│   ├── krx.co.kr/                # 한국거래소 (ETF)
│   ├── upbit.com/                # 업비트 (암호화폐)
│   ├── dart.fss.or.kr/           # 금융감독원 DART (공시)
│   ├── ecos.bok.or.kr/           # 한국은행 ECOS (거시경제)
│   └── yahoo.finance/            # Yahoo Finance (글로벌)
│
├── data_lake/                    # 데이터 레이크 레이어 모듈
│   ├── __init__.py
│   ├── bronze.py                 # Raw 착지 (MinIO JSONL)
│   ├── silver.py                 # 정제·정규화 (Parquet)
│   ├── gold.py                   # 집계 분석 (DuckDB)
│   └── catalog.py                # 메타데이터 카탈로그
│
├── pipeline/                     # 실행 파이프라인
│   ├── crawler_job.py            # 통합 크롤러 잡 (Docker 컨테이너 엔트리포인트)
│   ├── crawler_api.py            # FastAPI 백엔드 (SSE 스트리밍)
│   ├── gold_build.py             # Gold 집계 독립 실행 스크립트
│   └── load_to_qdrant.py         # Qdrant 직접 로딩 유틸
│
├── infra/                        # Docker 인프라 설정
│   ├── Dockerfile.crawler-job    # 크롤러 잡 컨테이너
│   ├── Dockerfile.crawler-api    # API 백엔드 컨테이너
│   └── Dockerfile.finance        # 금융 크롤러 공통 베이스
│
├── ui/                           # Next.js 검색 UI
├── opensanctions/                # OpenSanctions 제재 데이터
├── zavod/                        # OpenSanctions ETL 프레임워크
├── datasets/                     # OpenSanctions 데이터셋 정의
│
├── docker-compose.yml            # 전체 스택 (MinIO + Qdrant + API + UI)
├── requirements.txt              # Python 의존성
├── requirements-api.txt          # API 서버 의존성
├── .env.example                  # 환경변수 예시
├── Makefile
├── start.sh / start.ps1
└── README.md
```

---

## <i class="fa-brands fa-docker"></i> Docker 전체 스택 실행

```bash
cp .env.example .env
# .env 에서 DART_API_KEY, ECOS_API_KEY 설정 (선택)

docker compose up -d
```

서비스 목록:

| 서비스 | 포트 | 설명 |
|--------|------|------|
| `minio` | 9000 (API), 9001 (콘솔) | S3 호환 오브젝트 스토리지 |
| `crawler-api` | 8400 | FastAPI 크롤링 백엔드 |
| `web` | 8899 | Next.js 검색 UI |
| `zavod` | — | OpenSanctions ETL |

---

## <i class="fa-solid fa-play"></i> 크롤러 개별 실행

```bash
# 국내 주식 (네이버 + KRX ETF)
python crawlers/finance.naver.com/crawler2.py --market KOSPI --max-pages 4

# 업비트 암호화폐
python crawlers/upbit.com/crawler.py --markets KRW --output-format table

# DART 전자공시 (API 키 필요)
DART_API_KEY=xxx python crawlers/dart.fss.or.kr/crawler.py --corp-type Y --max-pages 2

# 한국은행 ECOS (API 키 필요)
ECOS_API_KEY=xxx python crawlers/ecos.bok.or.kr/crawler.py

# Yahoo Finance 글로벌
python crawlers/yahoo.finance/crawler.py --categories indices fx commodities

# 전체 파이프라인 (모든 소스)
python pipeline/crawler_job.py \
  --sources naver daum krx upbit yahoo \
  --markets KOSPI KOSDAQ \
  --upbit-markets KRW \
  --yahoo-categories indices etfs stocks commodities fx
```

---

## <i class="fa-solid fa-layer-group"></i> 데이터 레이크 직접 사용

```python
from data_lake import BronzeLayer, SilverLayer, GoldLayer, DataCatalog

# Bronze: 원본 쓰기
bronze = BronzeLayer()
bronze.write(records, source="upbit", data_type="crypto")

# Silver: 정제 후 Parquet 저장
silver = SilverLayer()
silver.write(raw_records, data_type="stocks")

# Gold: 집계 분석
gold = GoldLayer()
gold.build_all()
df = gold.query("SELECT * FROM market_summary ORDER BY total_market_cap DESC")
gold.close()

# Catalog: 수집 이력 확인
cat = DataCatalog()
cat.print_summary()
```

---

## <i class="fa-solid fa-circle-info"></i> OpenSanctions (기존)

**OpenSanctions**는 전 세계 제재(Sanctions) 및 KYC/AML 관련 데이터를 수집·정제·표준화하는 프로젝트입니다.  
`zavod/`, `datasets/`, `opensanctions/` 디렉터리를 참조하세요.

```bash
# Zavod ETL 실행
docker compose run --rm zavod bash -c "
  zavod crawl datasets/nl/terrorism_list/nl_terrorism_list.yml
  zavod export datasets/nl/terrorism_list/nl_terrorism_list.yml
  zavod load-db datasets/nl/terrorism_list/nl_terrorism_list.yml
"
```

---

## <i class="fa-brands fa-linux"></i> WSL2 개발 환경

```bash
# 1. WSL2 + Docker 확인
wsl --install
docker info

# 2. 의존성 설치
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. UI 실행 (Node.js 22 LTS)
cd ui && nvm use && npm install && npm run dev
```

---

## <i class="fa-solid fa-wrench"></i> Makefile 명령어

```bash
make build   # Docker 이미지 빌드
make shell   # 컨테이너 내부 진입
make crawl   # 기본 파이프라인 실행
make clean   # 임시 파일 정리
```
