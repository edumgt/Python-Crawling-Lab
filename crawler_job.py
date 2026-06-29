# pipeline/crawler_job.py 로 이동되었습니다.
# 하위호환성 유지용 래퍼
import runpy, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
runpy.run_path(str(Path(__file__).parent / "pipeline" / "crawler_job.py"), run_name="__main__")
