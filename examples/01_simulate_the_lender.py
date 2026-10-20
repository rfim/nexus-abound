"""Run the lender for a year and write what DMS would deliver to S3. The same code builds the site's data."""
from nexus.generate import generate

out = generate()
print("full load rows:", out["full_load"])
print("CDC files:", out["cdc_files"], " CDC changes:", out["cdc_changes"])
print("\nplanted problems:")
for k, v in out["planted"].items():
    print(f"  {k:<18} {v if not isinstance(v, list) else f'{len(v)} keys, e.g. {v[:3]}'}")
