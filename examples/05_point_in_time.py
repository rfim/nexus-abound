"""A model feature as it was known at decision time, from the change history, against the naive one."""
from nexus import cdc
from nexus.generate import BATCHES, DMS, generate

if not DMS.exists():
    generate()
tables = cdc.lake()
cdc.run_batches(tables, BATCHES)
txn, apps = tables["ob_transactions"], tables["applications"]
history = {}
for (k, _), c in sorted(txn.bronze.items(), key=lambda kv: kv[0][1]):
    history.setdefault(k, []).append(c)


def category_at(versions, when):
    known = [v for v in versions if v["cdc_ts"] <= when]
    return known[-1]["category"] if known else None


by_customer = {}
for k, row in txn.current().items():
    by_customer.setdefault(row["customer_id"], []).append((k, row))

differ, n = 0, 0
for a in apps.current().values():
    if not a["decided_at"] or a["decided_at"] <= "2026-07-01 00:00:00":
        continue
    n += 1
    when = a["decided_at"]
    pit = naive = 0.0
    for k, row in by_customer.get(a["customer_id"], []):
        if not ("" < row["booked_at"] < when) or row["fetched_at"] > when:
            continue
        naive += float(row["amount"]) if row["category"] == "salary" else 0
        pit += float(row["amount"]) if category_at(history[k], when) == "salary" else 0
    differ += abs(naive - pit) > 0.005
print(f"{differ} of {n} decisions since 1 July get a different income feature from today's labels.")
