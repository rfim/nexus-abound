import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ["NEXUS_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from nexus.spark import get_spark
    return get_spark("nexus-tests")


def write_dms(path: Path, header: list[str], rows: list[list]) -> Path:
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return path
