"""The whole run in plain Python: six batches by arrival, a full replay, reconciliation, the reload, and again.
Spark on Iceberg does the same in src/nexus/pipeline.py; tests/test_cdc_parity.py checks they agree."""
from nexus import cdc
from nexus.generate import BATCHES, DMS, RELOAD_AT, generate, reload_table
from nexus.paths import ROOT

if not DMS.exists():
    generate()
tables = cdc.lake()
for b in cdc.run_batches(tables, BATCHES):
    print(b["until"][:10], {k: (v["new_changes"], v["duplicates"]) for k, v in b["tables"].items()})

replay = sum(t.ingest([c for p in cdc.arrivals(n, BATCHES[-1]) for c in cdc.read_dms(p)])["new_changes"]
             for n, t in tables.items())
print("\nreplaying every file DMS ever wrote:", replay, "new changes")

print("\nreconciliation to Aurora:")
for t in tables.values():
    r = cdc.reconcile(t)
    print(f"  {r['table']:<16} {r['status']}  missing={len(r['missing_in_lake'])} different={len(r['different'])}")

rep = tables["repayments"]
path = ROOT / reload_table("repayments", RELOAD_AT)
ing = rep.ingest(cdc.read_dms(path))
print("\nreload:", path.name, rep.apply(ing["fresh"], authoritative=True))
print("repayments after the reload:", cdc.reconcile(rep)["status"])
