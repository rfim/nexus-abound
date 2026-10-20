"""What DMS writes: a CSV with Op, cdc_ts and change_seq, then the row. A column added mid-stream shows up as a new header."""
from nexus.cdc import arrivals, read_dms
from nexus.generate import APR_ADDED_AT, BATCHES, DMS, generate

if not DMS.exists():
    generate()
files = arrivals("loans", BATCHES[-1])
before = next(p for p in files if "2026/07/31" in str(p))
after = next(p for p in files if "2026/08/02" in str(p))
for p in (before, after):
    rows = read_dms(p)
    print(p.relative_to(DMS))
    print("  header:", ",".join(rows[0]))
    print("  first :", rows[0], "\n")
print("ALTER TABLE loans ADD COLUMN apr_bps happened at", APR_ADDED_AT, "- bronze adds the column when it first sees it.")
