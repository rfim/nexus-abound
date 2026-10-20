"""The plain-Python CDC rules (src/nexus/cdc.py) reach the same state as the Spark pipeline did."""
import json
from pathlib import Path

from nexus import cdc
from nexus.generate import BATCHES, RELOAD_AT, generate, reload_table
from nexus.paths import ROOT

SITE = json.loads((Path(__file__).resolve().parents[1] / "assets/data/site.json").read_text())


def test_same_result_as_the_spark_run():
    generate()
    tables = cdc.lake()
    log = cdc.run_batches(tables, BATCHES)
    spark = {b["batch"]: b["tables"] for b in SITE["batches"]}
    for i, b in enumerate(log):
        for name, t in b["tables"].items():
            assert (t["new_changes"], t["duplicates"]) == (spark[i][name]["new_changes"], spark[i][name]["duplicates"]), (i, name)
    replay = sum(t.ingest([c for p in cdc.arrivals(n, BATCHES[-1]) for c in cdc.read_dms(p)])["new_changes"]
                 for n, t in tables.items())
    assert replay == 0
    recon = {n: cdc.reconcile(t) for n, t in tables.items()}
    assert [n for n, r in recon.items() if r["status"] == "fail"] == ["repayments"]
    assert sorted(recon["repayments"]["different"] + recon["repayments"]["missing_in_lake"]) == sorted(SITE["incident"]["keys"])
    rep = tables["repayments"]
    rep.apply(rep.ingest(cdc.read_dms(ROOT / reload_table("repayments", RELOAD_AT)))["fresh"], authoritative=True)
    assert cdc.reconcile(rep)["status"] == "pass"
    for n, t in tables.items():
        assert len(t.current()) == SITE["tables"][n]["silver_live"], n
