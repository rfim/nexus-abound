"""List the files the site's code editor can open: assets/data/files.json. Run by CI."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GLOBS = ["examples/*.py", "src/nexus/*.py", "contracts/*.yml", "tests/*.py", "glue/*.py", "infra/dms/*.json",
         "infra/finops/*.sql", "infra/terraform/*.tf", "scripts/*.sh", "scripts/*.py", "docs/pdf-src/build.py",
         ".github/workflows/ci.yml", "requirements.txt"]
BROWSER = {"generate.py", "cdc.py", "contracts.py", "paths.py", "__init__.py"}
SPARK = {"bronze.py", "silver.py", "gold.py", "checks.py", "layout.py", "pipeline.py", "export.py", "spark.py",
         "test_bronze_silver.py", "test_layout.py", "conftest.py"}


def kind(rel: str, name: str) -> str:
    if rel.startswith("examples/") or (rel.startswith("src/") and name in BROWSER):
        return "browser"
    if name in SPARK:
        return "spark"
    if rel.startswith(("glue/", "infra/")):
        return "local"
    return "tests" if rel.startswith("tests/") else "view"


def manifest() -> list[dict]:
    out = []
    for g in GLOBS:
        for p in sorted(ROOT.glob(g)):
            rel = p.relative_to(ROOT).as_posix()
            out.append({"path": rel, "folder": rel.rsplit("/", 1)[0] if "/" in rel else ".", "runs": kind(rel, p.name),
                        "lines": len(p.read_text().splitlines())})
    return out


if __name__ == "__main__":
    (ROOT / "assets" / "data" / "files.json").write_text(json.dumps(manifest(), indent=1) + "\n")
    print(len(manifest()), "files")
