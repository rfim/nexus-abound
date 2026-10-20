"""Turn pytest's JUnit XML into assets/data/tests.json, the report the site shows."""
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone

SPARK = {"test_bronze_silver", "test_layout"}
LOCAL = set()
ABOUT = {
    "test_generate": "The simulated lender: deterministic, every planted problem really in the log",
    "test_cdc": "Plain-Python CDC rules reach exactly the Spark run's state",
    "test_bronze_silver": "Replay, change_seq ordering, tombstones, reloads, schema change, SCD2 (Spark + Iceberg)",
    "test_layout": "File pruning on partitioned, sorted, compacted Iceberg (Spark)",
    "test_data": "The published run keeps its promises",
    "test_site": "Pages link to real files; every element the scripts fill exists",
}


def main(xml_path: str, out: str) -> None:
    root = ET.parse(xml_path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    files = defaultdict(list)
    for tc in suite.iter("testcase"):
        mod = tc.get("classname", "").split(".")[-1]
        outcome = "failed" if tc.find("failure") is not None or tc.find("error") is not None else \
                  "skipped" if tc.find("skipped") is not None else "passed"
        files[mod].append({"name": tc.get("name"), "outcome": outcome, "seconds": round(float(tc.get("time", 0)), 3)})
    report = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "total": sum(len(v) for v in files.values()),
              **{k: sum(t["outcome"] == k for v in files.values() for t in v) for k in ("passed", "failed", "skipped")},
              "seconds": round(float(suite.get("time", 0)), 1),
              "suites": [{"file": f"tests/{m}.py", "about": ABOUT.get(m, ""), "spark": m in SPARK, "local": m in LOCAL, "tests": t}
                         for m, t in sorted(files.items(), key=lambda kv: list(ABOUT).index(kv[0]) if kv[0] in ABOUT else 99)]}
    with open(out, "w") as fh:
        json.dump(report, fh, indent=1)
    print(f"{report['passed']} passed, {report['failed']} failed, {report['skipped']} skipped → {out}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
