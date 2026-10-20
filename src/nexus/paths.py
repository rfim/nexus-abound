"""Where things live, and the table naming rule. No Spark import, so pure-Python modules can use it anywhere."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "target"
CATALOG = os.environ.get("NEXUS_CATALOG", "lake")   # a Hadoop catalog locally; glue_catalog on AWS Glue


def table(layer: str, name: str) -> str:
    """lake.<layer>.<name>; on Glue the same name resolves through the Glue Data Catalog."""
    return f"{CATALOG}.{layer}.{name}"
