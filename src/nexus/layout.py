"""Table design for speed, freshness and cost: what a query has to read, before and after.

Athena and Spark both plan an Iceberg scan the same way: drop the partitions the filter rules out,
then drop the files whose min/max column bounds can't match. Athena bills the bytes that survive
($5 per TB scanned), so the bytes a query plans to read are the cost. This module plans the same
queries against three layouts of the Open Banking transactions and reports files and bytes for each,
reading the bounds from Iceberg's own `files` metadata table.
"""
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nexus.spark import CATALOG, table

ATHENA_USD_PER_TB = 5.0
# 128 MB in production; scaled down with the data so a month here also spans several sorted files
TARGET_FILE_BYTES = 16 * 1024
QUERIES = {
    "one_customer_90d": {"label": "Data Science: one customer's last 90 days",
                         "customer": "C00042", "from": "2026-07-02", "to": "2026-09-30"},
    "one_month": {"label": "Reporting: every transaction in September",
                  "customer": None, "from": "2026-09-01", "to": "2026-09-30"},
}


def build_naive(spark: SparkSession) -> int:
    """The layout a per-file append job leaves: unpartitioned, unsorted, one small file per daily delivery."""
    name = table("lab", "ob_transactions_naive")
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    src = spark.table(table("silver", "ob_transactions")).filter(~F.col("is_deleted"))
    src.limit(0).writeTo(name).using("iceberg").create()
    # one file per delivery day, in one commit: the file count a per-delivery append ends up with
    days = src.select(F.to_date("updated_at")).distinct().count()
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    src.repartition(days, F.to_date("updated_at")).sortWithinPartitions("txn_id").writeTo(name).append()
    spark.conf.unset("spark.sql.adaptive.coalescePartitions.enabled")
    return len(spark.sql(f"SELECT 1 FROM {name}.files").collect())


def compact(spark: SparkSession) -> dict:
    """Rewrite the small files MERGE leaves behind into sorted ones, then drop what no snapshot needs."""
    name = "silver.ob_transactions"
    res = spark.sql(f"""CALL {CATALOG}.system.rewrite_data_files(table => '{name}', strategy => 'sort',
                         sort_order => 'customer_id ASC NULLS LAST, booked_at ASC NULLS LAST',
                         options => map('rewrite-all', 'true', 'target-file-size-bytes', '{TARGET_FILE_BYTES}'))""").first()
    return {"rewritten_files": res.rewritten_data_files_count, "added_files": res.added_data_files_count}


def files(spark: SparkSession, name: str):
    return spark.sql(f"""SELECT file_size_in_bytes AS bytes, record_count AS records,
                                readable_metrics.customer_id.lower_bound AS cust_lo,
                                readable_metrics.customer_id.upper_bound AS cust_hi,
                                readable_metrics.booked_at.lower_bound AS booked_lo,
                                readable_metrics.booked_at.upper_bound AS booked_hi
                         FROM {name}.files""").collect()


def plan(spark: SparkSession, name: str, q: dict) -> dict:
    """Files and bytes the scan must read once partitions and min/max bounds are applied."""
    lo, hi = datetime.fromisoformat(q["from"]), datetime.fromisoformat(q["to"] + "T23:59:59")
    fs = files(spark, name)
    # a file whose booked_at is all null (tombstones) can't match a range on it, so the scan skips it
    keep = [f for f in fs if f.booked_lo is not None and f.booked_lo <= hi and f.booked_hi >= lo
            and (q["customer"] is None or f.cust_lo <= q["customer"] <= f.cust_hi)]
    return {"files_total": len(fs), "bytes_total": sum(f.bytes for f in fs),
            "files_read": len(keep), "bytes_read": sum(f.bytes for f in keep), "rows_read": sum(f.records for f in keep)}


def answer(spark: SparkSession, name: str, q: dict) -> list:
    df = spark.table(name).filter(F.col("booked_at").between(q["from"], q["to"] + " 23:59:59"))
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    if q["customer"]:
        df = df.filter(F.col("customer_id") == q["customer"])
    return sorted(r.txn_id for r in df.select("txn_id").collect())


def compare(spark: SparkSession) -> dict:
    tuned = table("silver", "ob_transactions")
    naive = table("lab", "ob_transactions_naive")
    out = {"layouts": {"naive": "unpartitioned, unsorted, one file per daily append",
                       "merged": "months(booked_at), written by MERGE, before compaction",
                       "compacted": "months(booked_at), sorted by customer_id, booked_at, compacted"},
           "queries": {}}
    before = {k: plan(spark, tuned, q) for k, q in QUERIES.items()}
    out["compaction"] = compact(spark)
    for k, q in QUERIES.items():
        same = answer(spark, naive, q) == answer(spark, tuned, q)
        rows = {"naive": plan(spark, naive, q), "merged": before[k], "compacted": plan(spark, tuned, q)}
        out["queries"][k] = {"label": q["label"], "same_answer": same, **rows}
    return out
