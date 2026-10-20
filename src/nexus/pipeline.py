"""End to end: DMS files → bronze → silver → checks → incident and reload → erasure → layout → gold.

The run replays three months of DMS deliveries in six batches by arrival time, the way a scheduled
Glue job would see them, then proves the lake against the source and fixes what it finds. Every write
is Iceberg, so the replay, the MERGEs, the reload and the compaction are all in the table history.
"""
import json
import os
import time
from datetime import datetime

from pyspark.sql import functions as F

from nexus import bronze, checks, export, gold, layout, silver
from nexus.contracts import load_quality, load_sources, source
from nexus.generate import generate, reload_table
from nexus.paths import ROOT
from nexus.spark import CATALOG, TARGET, get_spark

BATCHES = [datetime(2026, 7, 1, 2), datetime(2026, 8, 1, 6), datetime(2026, 8, 16, 6), datetime(2026, 9, 1, 6),
           datetime(2026, 9, 16, 6), datetime(2026, 10, 1, 6)]
RELOAD_AT = datetime(2026, 10, 1, 7, 30)
REPLAY_BATCH, RELOAD_BATCH = 99, len(BATCHES)


class Steps:
    """Records each task's duration, like an orchestrator run."""

    def __init__(self):
        self.tasks, self.t0 = [], time.time()

    def run(self, key: str, label: str, fn, *args):
        start = time.time()
        out = fn(*args)
        self.tasks.append({"key": key, "label": label, "start_s": round(start - self.t0, 1),
                           "duration_s": round(time.time() - start, 1)})
        return out


def _batch(spark, sources, b, until):
    out = {}
    for s in sources:
        ing = bronze.ingest(spark, s, bronze.new_files(spark, s, until), b)
        out[s["table"]] = ing | {"silver": silver.apply(spark, s, b)}
    return out


def _replay(spark, sources):
    """Re-ingest every file DMS ever delivered, as if the file log were lost. Nothing may change."""
    return {s["table"]: bronze.ingest(spark, s, bronze.list_arrivals(s, datetime(2100, 1, 1)), REPLAY_BATCH)
            | {"silver": silver.apply(spark, s, REPLAY_BATCH)} for s in sources}


def _check_all(spark, sources, erased):
    q = load_quality()
    out = [checks.reconcile(spark, s) for s in sources]
    out += [r for s in sources for r in checks.quality(spark, s)]
    out += [checks.freshness(spark, source(t), BATCHES[-1]) for t in q["freshness"]["tables"]]
    out.append(checks.erasure(spark, source("customers"), erased))
    return out


def investigate(spark, failing: dict) -> dict:
    """What a person on call would look at: which rows differ, when they changed at source, and whether
    bronze has any change at all from that window."""
    name = failing["table"]
    snap = checks.source_rows(spark, source(name)).filter(F.col(source(name)["primary_key"][0])
                                                         .isin(failing["bad_keys"]))
    win = snap.agg(F.min("updated_at").alias("lo"), F.max("updated_at").alias("hi")).first()
    hourly = (spark.table(bronze.bronze_name(source(name)))
              .filter(F.hour("cdc_ts") == win.lo.hour).groupBy(F.to_date("cdc_ts").alias("day"))
              .count().orderBy("day"))
    by_day = {str(r.day): r["count"] for r in hourly.collect()}
    day = str(win.lo.date())
    return {"table": name, "rows_affected": len(failing["bad_keys"]), "keys": failing["bad_keys"],
            "source_changed_from": str(win.lo), "source_changed_to": str(win.hi),
            "bronze_changes_in_that_hour_that_day": by_day.get(day, 0),
            "bronze_changes_in_that_hour_other_days_median": sorted(v for k, v in by_day.items() if k != day)[
                len(by_day) // 2 - 1] if len(by_day) > 1 else None,
            "diagnosis": f"DMS delivered no {name} changes between {win.lo:%H}:00 and {win.hi.hour + 1:02d}:00 on "
                         f"{day}, the hour of the daily direct-debit run. The source has them, so the task "
                         f"resumed from the wrong position after an interruption: the rows are missing, not late.",
            "remedy": f"Reload {name} from the source (DMS 'reload table'), apply it as authoritative, re-run the "
                      f"reconciliation. Then alert on hourly change volume per table so a gap pages before a "
                      f"month-end report finds it."}


def erase(spark, sources, customer_id: str) -> dict:
    """Right to erasure: the source deleted the customer; silver already holds tombstones. Purge the
    customer's changes from bronze, then expire old snapshots so time travel can't bring them back."""
    out = {"customer": customer_id, "bronze_rows_deleted": {}, "expired_tables": []}
    for s in sources:
        if "customer_id" not in s["columns"]:
            continue
        b = bronze.bronze_name(s)
        n = spark.table(b).filter(F.col("customer_id") == customer_id).count()
        if s["table"] != "customers":
            ids = [r[0] for r in spark.table(b).filter(F.col("customer_id") == customer_id)
                   .select(s["primary_key"][0]).distinct().collect()]
            # the delete markers carry only the key, so remove them by key too
            n = spark.table(b).filter(F.col(s["primary_key"][0]).isin(ids)).count()
            if ids:
                spark.table(b).filter(F.col(s["primary_key"][0]).isin(ids)).select(s["primary_key"][0]) \
                    .distinct().createOrReplaceTempView("erase_keys")
                spark.sql(f"DELETE FROM {b} WHERE {s['primary_key'][0]} IN (SELECT * FROM erase_keys)")
        else:
            spark.sql(f"DELETE FROM {b} WHERE customer_id = '{customer_id}'")
        out["bronze_rows_deleted"][s["table"]] = n
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for s in sources:
        for name in (bronze.bronze_name(s), silver.silver_name(s)):
            short = name.split(".", 1)[1]
            spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{short}', "
                      f"older_than => TIMESTAMP '{now}', retain_last => 1)")
            out["expired_tables"].append(short)
    return out


def _story(spark):
    """One repayment cured in a single transaction: two statements, one commit timestamp."""
    rep = source("repayments")
    b = spark.table(bronze.bronze_name(rep))
    ties = (b.filter("Op = 'U'").groupBy("repayment_id", "cdc_ts").count().filter("count > 1")
            .orderBy("repayment_id").first())
    rows = (b.filter((F.col("repayment_id") == ties.repayment_id)).dropDuplicates(["change_seq"])
            .orderBy("change_seq").select("Op", "cdc_ts", "change_seq", "status", "amount_paid", "_source_file")
            .collect())
    final = silver.current(spark, rep).filter(F.col("repayment_id") == ties.repayment_id).first()
    return {"repayment_id": ties.repayment_id,
            "changes": [{"op": r.Op, "cdc_ts": str(r.cdc_ts), "change_seq": r.change_seq, "status": r.status,
                         "amount_paid": r.amount_paid, "file": r._source_file.split("/lending/")[-1]} for r in rows],
            "silver": {"status": final.status, "amount_paid": float(final.amount_paid),
                       "change_seq": final.change_seq}}


def run() -> dict:
    os.environ.setdefault("TZ", "UTC")
    steps = Steps()
    spark = get_spark()
    sources = load_sources()
    log = {"generator": steps.run("generate", "Simulate Aurora and write what DMS delivers to S3", generate),
           "batches": []}
    erased = log["generator"]["planted"]["erased_customer"]
    for b, until in enumerate(BATCHES):
        label = "Full load" if b == 0 else f"CDC delivered by {until:%d %b}"
        res = steps.run(f"batch{b}", f"Batch {b}: {label} → bronze → silver", _batch, spark, sources, b, until)
        log["batches"].append({"batch": b, "until": str(until), "label": label, "tables": res})
        if b == 4:
            row = silver.current(spark, source("customers")).filter(F.col("customer_id") == erased).count()
            log["erased_live_after_delete"] = row
    log["resurrection"] = {"customer": erased, "late_file": log["generator"]["planted"]["late_file"],
                           "live_rows_after_late_file": silver.current(spark, source("customers"))
                           .filter(F.col("customer_id") == erased).count()}
    log["replay"] = steps.run("replay", "Replay every DMS file (should change nothing)", _replay, spark, sources)

    first = steps.run("check", "Reconcile to source, quality rules, freshness, erasure", _check_all,
                      spark, sources, erased)
    log["checks_first"] = first
    failing = [c for c in first if c["status"] == "fail" and c["check"].startswith("reconcile")]
    log["incident"] = steps.run("investigate", "Investigate the failing reconciliation", investigate, spark,
                                failing[0]) if failing else None
    if failing:
        path = reload_table(failing[0]["table"], RELOAD_AT)
        res = steps.run("reload", f"Reload {failing[0]['table']} from source, apply as authoritative", lambda: (
            bronze.ingest(spark, source(failing[0]["table"]), [ROOT / path], RELOAD_BATCH)
            | {"silver": silver.apply(spark, source(failing[0]["table"]), RELOAD_BATCH, authoritative=True)}))
        log["reload"] = {"file": path, **res}
    log["erasure"] = steps.run("erase", "Right to erasure: purge bronze, expire snapshots", erase, spark, sources,
                               erased)
    log["checks"] = steps.run("recheck", "Every check again", _check_all, spark, sources, erased)

    log["history"] = steps.run("history", "SCD2 history for loans and Open Banking rows", lambda: {
        s["table"]: silver.build_history(spark, s) for s in sources if s.get("history")})
    steps.run("naive", "Build the naive layout for comparison", layout.build_naive, spark)
    log["layout"] = steps.run("layout", "Plan the same queries on three layouts; compact", layout.compare, spark)
    log["gold"] = steps.run("gold", "Loan book, calibration set, point-in-time features", lambda: {
        f.__name__.replace("build_", ""): f(spark) for f in gold.PRODUCTS})
    log["story"] = _story(spark)
    log["tables"] = {s["table"]: {"bronze_changes": spark.table(bronze.bronze_name(s)).count(),
                                  "silver_live": silver.current(spark, s).count(),
                                  "silver_tombstones": spark.table(silver.silver_name(s)).filter("is_deleted").count(),
                                  "snapshots": spark.sql(f"SELECT count(*) FROM {silver.silver_name(s)}.snapshots")
                                  .first()[0]} for s in sources}
    log["tasks"] = steps.tasks
    log["seconds"] = round(time.time() - steps.t0, 1)
    TARGET.mkdir(exist_ok=True)
    (TARGET / "run_log.json").write_text(json.dumps(log, indent=1, default=str))
    export.export(spark, log)
    return log


if __name__ == "__main__":
    out = run()
    for c in out["checks"]:
        print(f"{c['status']:<5} {c['table']:<16} {c['check']}")
    print(json.dumps({k: out[k] for k in ("incident", "layout", "gold", "seconds")}, indent=1, default=str))
