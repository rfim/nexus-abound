"""Proof that the lake matches the source: reconciliation, quality rules, freshness and erasure.

Reconciliation compares silver's current state with the source's own rows (an export from Aurora),
row by row on every contract column. It is the check that catches what no key can: changes DMS
never delivered.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nexus.contracts import load_quality
from nexus.generate import SNAPSHOT
from nexus.silver import current, silver_name, typed


def source_rows(spark: SparkSession, src: dict) -> DataFrame:
    raw = spark.read.option("header", True).csv(str(SNAPSHOT / f"{src['table']}.csv"))
    return typed(raw.withColumn("Op", F.lit("I")).withColumn("cdc_ts", F.lit(None)).withColumn("change_seq", F.lit(None)),
                 src).drop("Op", "cdc_ts", "change_seq")


def reconcile(spark: SparkSession, src: dict) -> dict:
    pk, cols = src["primary_key"], list(src["columns"])
    lake = current(spark, src).select(*cols).alias("l")
    srcd = source_rows(spark, src).alias("s")
    j = lake.join(srcd, [lake[k] == srcd[k] for k in pk], "full_outer")
    in_lake, in_src = F.col(f"l.{pk[0]}").isNotNull(), F.col(f"s.{pk[0]}").isNotNull()
    same = F.lit(True)
    for c in cols:
        same = same & F.col(f"l.{c}").eqNullSafe(F.col(f"s.{c}"))
    j = j.select(F.coalesce(F.col(f"l.{pk[0]}"), F.col(f"s.{pk[0]}")).alias("key"),
                 F.when(~in_lake, "missing_in_lake").when(~in_src, "extra_in_lake")
                  .when(~same, "different").otherwise("match").alias("result"),
                 *[F.col(f"l.{c}").alias(f"lake_{c}") for c in cols[1:]],
                 *[F.col(f"s.{c}").alias(f"source_{c}") for c in cols[1:]])
    counts = {r.result: r.n for r in j.groupBy("result").agg(F.count("*").alias("n")).collect()}
    bad = j.filter("result <> 'match'").orderBy("key")
    sample = []
    for r in bad.limit(5).collect():
        diff = {c: [str(r[f"lake_{c}"]), str(r[f"source_{c}"])] for c in cols[1:]
                if str(r[f"lake_{c}"]) != str(r[f"source_{c}"])}
        sample.append({"key": r.key, "result": r.result, "differs": diff})
    total = f"sum({src['reconcile']['sum_of']})" if src["reconcile"].get("sum_of") else "0"
    lake_sum = current(spark, src).selectExpr(f"CAST({total} AS DOUBLE) s").first().s
    src_sum = source_rows(spark, src).selectExpr(f"CAST({total} AS DOUBLE) s").first().s
    n_bad = sum(v for k, v in counts.items() if k != "match")
    return {"table": src["table"], "check": "reconcile to source, row by row", "status": "pass" if n_bad == 0 else "fail",
            "rows_source": sum(v for k, v in counts.items() if k != "extra_in_lake"),
            "rows_lake": sum(v for k, v in counts.items() if k != "missing_in_lake"),
            "match": counts.get("match", 0), "missing_in_lake": counts.get("missing_in_lake", 0),
            "extra_in_lake": counts.get("extra_in_lake", 0), "different": counts.get("different", 0),
            "sum_of": src["reconcile"].get("sum_of"), "sum_lake": round(lake_sum or 0, 2),
            "sum_source": round(src_sum or 0, 2), "bad_keys": [r.key for r in bad.select("key").collect()],
            "sample": sample}


def quality(spark: SparkSession, src: dict) -> list[dict]:
    out = []
    for rule, cond in load_quality()["rules"].get(src["table"], {}).items():
        failing = current(spark, src).filter(f"NOT ({cond})")
        n = failing.count()
        out.append({"table": src["table"], "check": rule, "rule": cond, "status": "pass" if n == 0 else "fail",
                    "failing_rows": n})
    return out


def freshness(spark: SparkSession, src: dict, as_of) -> dict:
    newest = spark.table(silver_name(src)).agg(F.max("cdc_ts")).first()[0]
    lag = (as_of - newest).total_seconds() / 3600 if newest else None
    limit = load_quality()["freshness"]["max_lag_hours"]
    return {"table": src["table"], "check": f"freshness under {limit} h", "newest_change": str(newest),
            "as_of": str(as_of),
            "lag_hours": round(lag, 1) if lag is not None else None,
            "status": "pass" if lag is not None and lag <= limit else "fail"}


def erasure(spark: SparkSession, src: dict, customer_id: str) -> dict:
    """The erased customer's PII in any snapshot the table can still time-travel to."""
    name = silver_name(src)
    snaps = [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {name}.snapshots").collect()]
    found = [s for s in snaps if spark.sql(f"SELECT 1 FROM {name} VERSION AS OF {s} "
                                           f"WHERE customer_id = '{customer_id}' AND email IS NOT NULL").limit(1).count()]
    live = spark.table(name).filter(F.col("customer_id") == customer_id).first()
    return {"table": src["table"], "check": "erased PII absent from every snapshot", "customer": customer_id,
            "snapshots": len(snaps), "snapshots_with_pii": len(found),
            "tombstone": bool(live and live.is_deleted and live.email is None),
            "status": "pass" if not found and live and live.is_deleted else "fail"}
