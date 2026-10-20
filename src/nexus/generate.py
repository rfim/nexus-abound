"""Synthetic Aurora + DMS: a small consumer lender's database, and what DMS writes to S3 from it.

The generator runs the lender forward in time and keeps every change in order, so it can write
both sides of the story:

  landing/dms/lending/<table>/LOAD00000001.csv        the full load, taken at 2026-07-01 00:00
  landing/dms/lending/<table>/YYYY/MM/DD/<ts>.csv      CDC files, one per table per day
  landing/source_snapshot/<table>.csv                  the source's real final state, for reconciliation

File modification times are set to when DMS delivered each file, like S3 LastModified, so the
ingestion lists files by arrival exactly as it would in S3. The planted problems are the ones a
CDC pipeline meets in production; each is listed in PLANTED and tested.
"""
import csv
import json
import os
import random
import shutil
from datetime import date, datetime, timedelta

from nexus.paths import ROOT

LANDING = ROOT / "landing"
DMS = LANDING / "dms" / "lending"
SNAPSHOT = LANDING / "source_snapshot"

SEED = 7
HISTORY_FROM = datetime(2025, 10, 1)
FULL_LOAD_AT = datetime(2026, 7, 1)
END = datetime(2026, 9, 30, 23, 59, 59)
APR_ADDED_AT = datetime(2026, 8, 1)            # ALTER TABLE loans ADD COLUMN apr_bps
RESTART_AT = datetime(2026, 8, 12, 10, 0)      # DMS task restart: re-sends the previous two hours
GAP = (datetime(2026, 9, 20, 2, 0), datetime(2026, 9, 20, 4, 0))   # repayments changes DMS never wrote
LATE_FILE = ("customers", date(2026, 9, 3), datetime(2026, 9, 18, 9, 30))  # delivered 15 days late
ERASED_AT = datetime(2026, 9, 10, 11, 0)       # a declined applicant asks to be forgotten
# when the pipeline runs: six batches by arrival time, then the reload that fixes the gap
BATCHES = [datetime(2026, 7, 1, 2), datetime(2026, 8, 1, 6), datetime(2026, 8, 16, 6), datetime(2026, 9, 1, 6),
           datetime(2026, 9, 16, 6), datetime(2026, 10, 1, 6)]
RELOAD_AT = datetime(2026, 10, 1, 7, 30)

COLUMNS = {
    "customers": ["customer_id", "full_name", "email", "date_of_birth", "postcode", "created_at", "updated_at"],
    "applications": ["application_id", "customer_id", "amount_requested", "term_months", "pd_score", "decision",
                     "applied_at", "decided_at", "updated_at"],
    "loans": ["loan_id", "application_id", "customer_id", "principal", "term_months", "status", "apr_bps",
              "originated_at", "updated_at"],
    "repayments": ["repayment_id", "loan_id", "instalment", "due_date", "amount_due", "amount_paid", "status",
                   "updated_at"],
    "ob_transactions": ["txn_id", "customer_id", "booked_at", "amount", "category", "fetched_at", "updated_at"],
}
PK = {"customers": "customer_id", "applications": "application_id", "loans": "loan_id",
      "repayments": "repayment_id", "ob_transactions": "txn_id"}

FIRST = ["Amira", "Ben", "Chloe", "Dev", "Ewa", "Femi", "Grace", "Hassan", "Isla", "Jon", "Kemi", "Leo", "Maya",
         "Nadia", "Omar", "Priya", "Rhys", "Sana", "Tom", "Una", "Viktor", "Wen", "Yusuf", "Zoe"]
LAST = ["Adams", "Begum", "Clarke", "Dhillon", "Evans", "Fofana", "Green", "Hughes", "Iqbal", "Jones", "Khan",
        "Lewis", "Mensah", "Nowak", "Okafor", "Patel", "Quinn", "Reid", "Shah", "Taylor", "Usman", "Walsh"]
POSTCODES = ["N12", "E14", "SE15", "M1", "B5", "LS6", "BS3", "G41", "CF10", "NE6", "L8", "EH7"]


def _ts(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S.%f")


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return _ts(v)
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float):
        return f"{v:.2f}" if abs(v) >= 1 or v == 0 else f"{v:.4f}"
    return str(v)


def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    y, m = d.year + m // 12, m % 12 + 1
    return date(y, m, min(d.day, 28))


class Lender:
    """The source database. Every write goes through change(), which keeps the CDC log in commit order."""

    def __init__(self, rng: random.Random):
        self.rng = rng
        self.state = {t: {} for t in COLUMNS}
        self.log = []          # (cdc_ts, seq, table, op, row)
        self.seq = 0
        self.events = []       # pending (time, order, fn)

    def change(self, t: datetime, table: str, op: str, row: dict):
        self.seq += 1
        key = row[PK[table]]
        if op == "D":
            before = self.state[table].pop(key)
            row = {c: (before[c] if c == PK[table] else None) for c in COLUMNS[table]}
        else:
            row = {**self.state[table].get(key, {}), **row, "updated_at": t}
            self.state[table][key] = row
        self.log.append((t, self.seq, table, op, dict(row)))

    def at(self, t: datetime, fn):
        self.events.append((t, len(self.events), fn))

    def run(self):
        while self.events:
            self.events.sort(key=lambda e: (e[0], e[1]))
            t, _, fn = self.events.pop(0)
            if t > END:
                break
            fn(t)


def simulate(rng: random.Random) -> tuple[Lender, dict]:
    db = Lender(rng)
    facts = {"erased_customer": None, "same_txn_cures": 0, "recategorised": 0}
    n_customers = 520
    ids = iter(range(1, 10_000_000))

    def fetch_transactions(c, t, since):
        """An Open Banking pull: every transaction booked between since and t, inserted now."""
        day = since.date()
        while day < t.date():
            for cat, amount, p in c["pattern"]:
                # salary on a fixed payday, rent on the 1st, everything else at random
                due = {"salary": day.day == c["payday"], "rent": day.day == 1}.get(cat)
                if due if due is not None else rng.random() < p:
                    booked = datetime.combine(day, datetime.min.time()) + timedelta(hours=rng.randint(6, 21),
                                                                                    minutes=rng.randint(0, 59))
                    if booked >= t:
                        continue
                    value = round(amount * rng.uniform(0.85, 1.15), 2)
                    # the provider's first pass labels some salaries "other"; enrichment fixes it days later
                    first_cat = "other" if cat == "salary" and rng.random() < 0.35 else cat
                    txn = f"T{next(ids):07d}"
                    db.change(t, "ob_transactions", "I", {"txn_id": txn, "customer_id": c["id"], "booked_at": booked,
                                                          "amount": value, "category": first_cat, "fetched_at": t})
                    if first_cat != cat:
                        fix = t + timedelta(days=rng.randint(5, 25), hours=rng.randint(0, 6))
                        db.at(fix, lambda tt, txn=txn, cat=cat: txn in db.state["ob_transactions"] and (
                            db.change(tt, "ob_transactions", "U", {"txn_id": txn, "category": cat}),
                            facts.__setitem__("recategorised", facts["recategorised"] + 1)))
            day += timedelta(days=1)
        c["last_fetch"] = t

    def refresh(c):
        def go(t):
            if c["id"] in db.state["customers"]:
                fetch_transactions(c, t, c["last_fetch"])
                db.at(t + timedelta(days=30), go)
        return go

    def instalment_due(c, loan_id, rep_id, due, k):
        def go(t):
            rep = db.state["repayments"][rep_id]
            loan = db.state["loans"][loan_id]
            if loan["status"] in ("defaulted", "repaid"):
                return
            if rng.random() > c["miss_p"]:
                db.change(t, "repayments", "U", {"repayment_id": rep_id, "amount_paid": rep["amount_due"],
                                                 "status": "paid"})
                if loan["status"] == "in_arrears":
                    db.change(t, "loans", "U", {"loan_id": loan_id, "status": "active"})
                if k == loan["term_months"]:
                    db.change(t, "loans", "U", {"loan_id": loan_id, "status": "repaid"})
                return
            t3 = t + timedelta(days=3)

            def miss(tt):
                db.change(tt, "repayments", "U", {"repayment_id": rep_id, "status": "missed"})
                prev = f"{loan_id}-{k - 1:02d}"
                missed_before = k > 1 and db.state["repayments"][prev]["status"] == "missed"
                db.change(tt, "loans", "U", {"loan_id": loan_id,
                                             "status": "defaulted" if missed_before else "in_arrears"})
                if not missed_before and rng.random() < 0.4:
                    def cure(t2):
                        if db.state["repayments"][rep_id]["status"] != "missed":
                            return
                        # one transaction, two statements, the same commit timestamp: only change_seq orders them
                        db.change(t2, "repayments", "U", {"repayment_id": rep_id, "status": "paid_late",
                                                          "amount_paid": 0.0})
                        db.change(t2, "repayments", "U", {"repayment_id": rep_id, "amount_paid": rep["amount_due"]})
                        db.change(t2, "loans", "U", {"loan_id": loan_id, "status": "active"})
                        facts["same_txn_cures"] += 1
                    db.at(tt + timedelta(days=rng.randint(6, 12), hours=rng.randint(9, 17)), cure)
            db.at(t3, miss)
        return go

    def decide(c, app_id):
        def go(t):
            app = db.state["applications"][app_id]
            approved = float(app["pd_score"]) < 0.35 and rng.random() < 0.92
            db.change(t, "applications", "U", {"application_id": app_id, "decided_at": t,
                                               "decision": "approved" if approved else "declined"})
            c["declined"] = not approved
            if not approved:
                return
            loan_id = f"L{app_id[1:]}"
            t_loan = t + timedelta(minutes=rng.randint(20, 90))

            def originate(tt):
                row = {"loan_id": loan_id, "application_id": app_id, "customer_id": c["id"],
                       "principal": app["amount_requested"], "term_months": app["term_months"], "status": "active",
                       "originated_at": tt}
                if tt >= APR_ADDED_AT:
                    row["apr_bps"] = rng.choice([1990, 2490, 2990, 3490])
                db.change(tt, "loans", "I", row)
                monthly = round(app["amount_requested"] / app["term_months"] * 1.12, 2)
                for k in range(1, app["term_months"] + 1):
                    due = _add_months(tt.date(), k)
                    rep_id = f"{loan_id}-{k:02d}"
                    db.change(tt, "repayments", "I", {"repayment_id": rep_id, "loan_id": loan_id, "instalment": k,
                                                      "due_date": due, "amount_due": monthly, "amount_paid": 0.0,
                                                      "status": "due"})
                    # the direct-debit run is at 03:00 on the due date
                    db.at(datetime.combine(due, datetime.min.time()) + timedelta(hours=3),
                          instalment_due(c, loan_id, rep_id, due, k))
                db.at(tt + timedelta(days=30), refresh(c))
            db.at(t_loan, originate)
        return go

    def apply(c):
        def go(t):
            app_id = f"A{next(ids):07d}"
            fetch_transactions(c, t, t - timedelta(days=90))
            db.change(t, "applications", "I", {"application_id": app_id, "customer_id": c["id"],
                                               "amount_requested": float(rng.choice([1000, 1500, 2000, 3000, 5000])),
                                               "term_months": rng.choice([6, 12, 12, 18, 24]),
                                               "pd_score": c["pd_score"], "decision": "pending",
                                               "applied_at": t, "decided_at": None})
            db.at(t + timedelta(hours=rng.randint(1, 30)), decide(c, app_id))
        return go

    span = (END - timedelta(days=10) - HISTORY_FROM).total_seconds()
    customers = []
    for i in range(1, n_customers + 1):
        t = HISTORY_FROM + timedelta(seconds=rng.random() * span)
        risk = min(0.6, rng.lognormvariate(-2.6, 0.75))            # the true chance of missing an instalment
        # pd_score: the scorecard's chance of missing one of the first three instalments. It is well
        # calibrated at low risk and too optimistic at the top, which is what recalibration fixes.
        est = (risk if risk < 0.04 else 0.04 + (risk - 0.04) * 0.6) * rng.uniform(0.85, 1.15)
        pd_score = round(min(0.95, max(0.005, 1 - (1 - est) ** 3)), 4)
        income = rng.uniform(1700, 4600) * (1.15 - risk)
        c = {"id": f"C{i:05d}", "risk": risk, "pd_score": pd_score, "miss_p": risk, "declined": False,
             "payday": rng.choice([15, 25, 28]),
             "pattern": [("salary", round(income, 2), 1 / 30), ("rent", -round(income * 0.38, 2), 1 / 30),
                         ("groceries", -45.0, 0.22), ("transfer", -120.0, 0.04), ("other", -35.0, 0.08)]}
        customers.append(c)
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        db.at(t, lambda tt, c=c, name=name: db.change(tt, "customers", "I", {
            "customer_id": c["id"], "full_name": name,
            "email": f"{name.lower().replace(' ', '.')}{c['id'][-3:]}@example.com",
            "date_of_birth": date(rng.randint(1962, 2004), rng.randint(1, 12), rng.randint(1, 28)),
            "postcode": rng.choice(POSTCODES) + f" {rng.randint(1, 9)}{rng.choice('ABDEFGHJ')}{rng.choice('LNPQRS')}",
            "created_at": tt}))
        db.at(t + timedelta(days=rng.randint(0, 5), hours=rng.randint(1, 12)), apply(c))
        if rng.random() < 0.1:   # people move house and change email
            db.at(t + timedelta(days=rng.randint(20, 300)), lambda tt, c=c: c["id"] in db.state["customers"] and
                  db.change(tt, "customers", "U", {"customer_id": c["id"], "postcode": rng.choice(POSTCODES) + " 1AA"}))

    # a declined applicant updates their email on 3 Sep, then asks to be erased on 10 Sep
    def email_update(t):
        cid = min(c["id"] for c in customers if c["declined"] and c["id"] in db.state["customers"]
                  and db.state["customers"][c["id"]]["created_at"] < FULL_LOAD_AT)
        facts["erased_customer"] = cid
        db.change(t, "customers", "U", {"customer_id": cid, "email": "new." + db.state["customers"][cid]["email"]})

    def erase(t):
        cid = facts["erased_customer"]
        for txn in [k for k, v in db.state["ob_transactions"].items() if v["customer_id"] == cid]:
            db.change(t, "ob_transactions", "D", {"txn_id": txn})
        for app in [k for k, v in db.state["applications"].items() if v["customer_id"] == cid]:
            db.change(t, "applications", "D", {"application_id": app})
        db.change(t, "customers", "D", {"customer_id": cid})

    db.at(datetime(2026, 9, 3, 14, 5), email_update)
    db.at(ERASED_AT, erase)
    db.run()
    return db, facts


def _write(path, cols, rows, mtime: datetime):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    os.utime(path, (mtime.timestamp(), mtime.timestamp()))


def _seq(t: datetime, n: int) -> str:
    """AR_H_CHANGE_SEQ: the commit time then a counter, 35 characters, sortable as a string."""
    return t.strftime("%Y%m%d%H%M%S") + f"{n:021d}"


def _cols(table: str, t: datetime) -> list[str]:
    cols = COLUMNS[table]
    return [c for c in cols if not (table == "loans" and c == "apr_bps" and t < APR_ADDED_AT)]


def full_load_rows(table: str, rows: list[dict], at: datetime) -> tuple[list[str], list[list[str]]]:
    cols = _cols(table, at)
    seq = _seq(at, 0)
    out = [["I", _ts(at), seq] + [_fmt(r.get(c)) for c in cols] for r in sorted(rows, key=lambda r: r[PK[table]])]
    return ["Op", "cdc_ts", "change_seq"] + cols, out


def write_dms(db: Lender, facts: dict) -> dict:
    shutil.rmtree(LANDING, ignore_errors=True)
    # 1. the full load: the state of every table at FULL_LOAD_AT
    at_load = {t: {} for t in COLUMNS}
    for t, _, table, op, row in db.log:
        if t > FULL_LOAD_AT:
            break
        key = row[PK[table]]
        if op == "D":
            at_load[table].pop(key, None)
        else:
            at_load[table][key] = row
    out = {"full_load": {}, "cdc_files": 0, "cdc_changes": {}, "planted": {}}
    for table, rows in at_load.items():
        cols, data = full_load_rows(table, list(rows.values()), FULL_LOAD_AT)
        _write(DMS / table / "LOAD00000001.csv", cols, data, FULL_LOAD_AT + timedelta(minutes=40))
        out["full_load"][table] = len(data)

    # 2. CDC after the full load, one file per table per day, delivered just after midnight
    by_file, gap, restart = {}, [], []
    for t, n, table, op, row in db.log:
        if t <= FULL_LOAD_AT:
            continue
        rec = (t, n, table, op, row)
        if table == "repayments" and GAP[0] <= t < GAP[1]:
            gap.append(rec)
            continue                     # DMS never wrote these: the task resumed from the wrong position
        by_file.setdefault((table, t.date()), []).append(rec)
        if RESTART_AT - timedelta(hours=2) <= t < RESTART_AT:
            restart.append(rec)
    for (table, day), recs in sorted(by_file.items()):
        arrived = datetime.combine(day + timedelta(days=1), datetime.min.time()) + timedelta(minutes=5)
        if (table, day) == LATE_FILE[:2]:
            arrived = LATE_FILE[2]
        cols = _cols(table, datetime.combine(day, datetime.min.time()))
        rows = [[op, _ts(t), _seq(t, n)] + [_fmt(row.get(c)) for c in cols] for t, n, _, op, row in recs]
        name = f"{day:%Y%m%d}-235959000.csv"
        _write(DMS / table / f"{day:%Y/%m/%d}" / name, ["Op", "cdc_ts", "change_seq"] + cols, rows, arrived)
        out["cdc_files"] += 1
        out["cdc_changes"][table] = out["cdc_changes"].get(table, 0) + len(rows)

    # 3. the restart: DMS re-sends the last two hours of changes in a new file per table
    for table in sorted({r[2] for r in restart}):
        recs = [r for r in restart if r[2] == table]
        cols = _cols(table, RESTART_AT)
        rows = [[op, _ts(t), _seq(t, n)] + [_fmt(row.get(c)) for c in cols] for t, n, _, op, row in recs]
        _write(DMS / table / f"{RESTART_AT:%Y/%m/%d}" / f"{RESTART_AT:%Y%m%d}-100500000.csv",
               ["Op", "cdc_ts", "change_seq"] + cols, rows, RESTART_AT + timedelta(minutes=6))
        out["cdc_files"] += 1

    # 4. the source's real final state: what SELECT * on Aurora returns at END
    for table, rows in db.state.items():
        cols = COLUMNS[table]
        _write(SNAPSHOT / f"{table}.csv", cols,
               [[_fmt(r.get(c)) for c in cols] for r in sorted(rows.values(), key=lambda r: r[PK[table]])], END)

    out["planted"] = {
        "replayed_changes": len(restart),
        "restart_at": _ts(RESTART_AT),
        "gap_changes": len(gap),
        "gap_window": [_ts(GAP[0]), _ts(GAP[1])],
        "gap_repayments": sorted({r[4]["repayment_id"] for r in gap}),
        "late_file": {"table": LATE_FILE[0], "day": LATE_FILE[1].isoformat(), "arrived": _ts(LATE_FILE[2])},
        "erased_customer": facts["erased_customer"],
        "erased_at": _ts(ERASED_AT),
        "schema_change": {"table": "loans", "column": "apr_bps", "at": _ts(APR_ADDED_AT)},
        "same_txn_cures": facts["same_txn_cures"],
        "recategorised": facts["recategorised"],
    }
    out["source_rows"] = {t: len(r) for t, r in db.state.items()}
    out["source_changes"] = len(db.log)
    (LANDING / "generator.json").write_text(json.dumps(out, indent=1))
    return out


def reload_table(table: str, at: datetime) -> str:
    """DMS 'reload table': a fresh full load of one table, written next to its CDC files."""
    rows = list(csv.DictReader((SNAPSHOT / f"{table}.csv").open()))
    cols, data = full_load_rows(table, [{k: (v or None) for k, v in r.items()} for r in rows], at)
    path = DMS / table / f"LOAD{at:%Y%m%d%H%M}.csv"
    _write(path, cols, data, at + timedelta(minutes=10))
    return str(path.relative_to(ROOT))


def generate() -> dict:
    db, facts = simulate(random.Random(SEED))
    return write_dms(db, facts)


if __name__ == "__main__":
    print(json.dumps(generate(), indent=1))
