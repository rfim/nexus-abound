"""The silver rules on four hand-made changes: replay, same-timestamp ordering, a tombstone, a late update."""
from nexus.cdc import Table

t = Table({"table": "loans", "primary_key": ["loan_id"], "columns": {"loan_id": "string", "status": "string"}})


def ch(op, ts, n, key, status=None):
    return {"Op": op, "cdc_ts": ts, "change_seq": ts.replace("-", "").replace(" ", "").replace(":", "") + f"{n:021d}",
            "loan_id": key, "status": status}


steps = [
    ("load", [ch("I", "2026-07-01 00:00:00", 0, "L1", "active")]),
    ("two statements, one commit time", [ch("U", "2026-08-16 16:00:00", 938, "L1", "repaid"),
                                         ch("U", "2026-08-16 16:00:00", 937, "L1", "in_arrears")]),
    ("DMS restart re-sends the same changes", [ch("U", "2026-08-16 16:00:00", 938, "L1", "repaid")]),
    ("delete", [ch("D", "2026-09-10 11:00:00", 120, "L1")]),
    ("an older update arrives late", [ch("U", "2026-09-03 09:00:00", 70, "L1", "active")]),
]
for label, changes in steps:
    ing = t.ingest(changes)
    res = t.apply(ing["fresh"])
    print(f"{label:<40} new={ing['new_changes']} dup={ing['duplicates']}  silver: {res}")
    print(f"{'':<40} L1 = {t.silver['L1']['row'] if not t.silver['L1']['deleted'] else 'TOMBSTONE'}")
