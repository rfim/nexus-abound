"""The CDC rules in plain Python: the same as bronze.py and silver.py, without Spark.

It runs anywhere Python runs (the site's code editor runs it in your browser), and
tests/test_cdc_parity.py checks it reaches exactly the state the Spark pipeline reaches.

  bronze   one row per (primary key, change_seq); a copy already there is skipped
  silver   a change lands only if its change_seq is newer; a delete is a tombstone with values nulled;
           a reload is authoritative (keys it doesn't contain are tombstoned)
"""
import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from nexus.contracts import load_sources
from nexus.generate import DMS, SNAPSHOT

META = ("Op", "cdc_ts", "change_seq")


@dataclass
class Table:
    src: dict
    bronze: dict = field(default_factory=dict)     # (key, change_seq) -> change
    silver: dict = field(default_factory=dict)     # key -> {"seq", "deleted", "row"}
    columns: list = field(default_factory=list)

    @property
    def pk(self) -> str:
        return self.src["primary_key"][0]

    def ingest(self, changes: list[dict]) -> dict:
        fresh, dup = [], 0
        for c in changes:
            k = (c[self.pk], c["change_seq"])
            if k in self.bronze:
                dup += 1
                continue
            self.bronze[k] = c
            fresh.append(c)
        added = [c for c in (changes[0].keys() if changes else []) if c not in META and c not in self.columns]
        self.columns += added
        return {"fresh": fresh, "new_changes": len(fresh), "duplicates": dup, "columns_added": added}

    def apply(self, fresh: list[dict], authoritative: bool = False) -> dict:
        latest = {}
        for c in fresh:
            k = c[self.pk]
            if k not in latest or c["change_seq"] > latest[k]["change_seq"]:
                latest[k] = c
        out = {"inserted": 0, "updated": 0, "tombstoned": 0, "ignored": 0}
        for k, c in latest.items():
            t = self.silver.get(k)
            if t and not c["change_seq"] > t["seq"]:
                out["ignored"] += 1
                continue
            deleted = c["Op"] == "D"
            row = {col: (c.get(col) if not deleted or col == self.pk else None) for col in self.src["columns"]}
            self.silver[k] = {"seq": c["change_seq"], "deleted": deleted, "row": row}
            out["tombstoned" if deleted else "updated" if t else "inserted"] += 1
        if authoritative:
            for k, t in self.silver.items():
                if k not in latest and not t["deleted"]:
                    t["deleted"] = True
                    out["tombstoned"] += 1
        return out

    def current(self) -> dict:
        return {k: t["row"] for k, t in self.silver.items() if not t["deleted"]}


def read_dms(path: Path) -> list[dict]:
    """A DMS CSV with a header: empty strings are nulls, as Spark reads them."""
    with path.open() as f:
        return [{k: (v if v != "" else None) for k, v in r.items()} for r in csv.DictReader(f)]


def arrivals(table: str, until: datetime, since: datetime | None = None) -> list[Path]:
    """Files by arrival time (the modification time stands in for S3 LastModified)."""
    out = []
    for p in sorted((DMS / table).rglob("*.csv")):
        t = datetime.fromtimestamp(p.stat().st_mtime)
        if t <= until and (since is None or t > since):
            out.append(p)
    return out


def _norm(v, typ: str):
    """Compare like the typed Spark columns do: numbers as numbers, timestamps to the microsecond."""
    if v in (None, ""):
        return None
    if typ.startswith("decimal") or typ == "int":
        return round(float(v), 4)
    if typ == "timestamp":
        return v.ljust(26, "0")[:26] if "." in v else v + ".000000"
    return v


def reconcile(t: Table) -> dict:
    """Silver's current state against the source's own rows, every contract column."""
    with (SNAPSHOT / f"{t.src['table']}.csv").open() as f:
        source = {r[t.pk]: r for r in csv.DictReader(f)}
    lake = t.current()
    cols = t.src["columns"]
    same = lambda a, b: all(_norm(a.get(c), ty) == _norm(b.get(c), ty) for c, ty in cols.items())  # noqa: E731
    missing = sorted(set(source) - set(lake))
    extra = sorted(set(lake) - set(source))
    different = sorted(k for k in set(lake) & set(source) if not same(lake[k], source[k]))
    return {"table": t.src["table"], "rows_source": len(source), "rows_lake": len(lake), "missing_in_lake": missing,
            "extra_in_lake": extra, "different": different,
            "status": "pass" if not (missing or extra or different) else "fail"}


def lake() -> dict[str, Table]:
    return {s["table"]: Table(s) for s in load_sources()}


def run_batches(tables: dict[str, Table], batch_ends: list[datetime]) -> list[dict]:
    """Deliver every file in arrival order, one batch per window: bronze, then silver."""
    log, since = [], None
    for until in batch_ends:
        row = {"until": str(until), "tables": {}}
        for name, t in tables.items():
            files = arrivals(name, until, since)
            changes = [c for p in files for c in read_dms(p)]
            ing = t.ingest(changes)
            row["tables"][name] = {"files": len(files), "new_changes": ing["new_changes"], "duplicates": ing["duplicates"],
                                   "columns_added": ing["columns_added"], "silver": t.apply(ing["fresh"])}
        log.append(row)
        since = until
    return log
