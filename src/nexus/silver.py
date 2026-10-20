"""Silver: the current state of each source table, typed, applied from bronze by MERGE.

Three rules make it match the source whatever order the changes arrive in:
  1. change_seq decides. A change only lands if it is newer than the row's; ties on cdc_ts are normal
     (one transaction, several statements) and change_seq still orders them.
  2. Deletes are tombstones. A deleted row stays with is_deleted = true and its change_seq, so an older
     update that arrives late cannot bring it back. Its non-key columns are nulled: erasure means erasure.
  3. A reload is authoritative. Rows the reload doesn't contain are tombstoned (WHEN NOT MATCHED BY SOURCE).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nexus.bronze import bronze_name
from nexus.spark import table


def silver_name(src: dict) -> str:
    return table("silver", src["table"])


def typed(df: DataFrame, src: dict) -> DataFrame:
    cols = [F.col(c).cast(t).alias(c) if c in df.columns else F.lit(None).cast(t).alias(c)
            for c, t in src["columns"].items()]
    return df.select(*cols, "Op", "cdc_ts", "change_seq")


def latest(df: DataFrame, src: dict) -> DataFrame:
    w = Window.partitionBy(*src["primary_key"]).orderBy(F.col("change_seq").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    deleted = F.col("Op") == "D"
    keep = set(src["primary_key"])
    return df.select(*[F.col(c) if c in keep else F.when(deleted, F.lit(None)).otherwise(F.col(c)).alias(c)
                       for c in src["columns"]],
                     deleted.alias("is_deleted"), "cdc_ts", "change_seq")


def _create(spark: SparkSession, name: str, df: DataFrame, src: dict) -> None:
    w = df.limit(0).writeTo(name).using("iceberg").tableProperty("format-version", "2")
    layout = src.get("layout")
    if layout:
        col = layout["partition_by"].split("(")[1].rstrip(")")
        w = w.partitionedBy(F.months(col))
    w.create()
    if layout:
        spark.sql(f"ALTER TABLE {name} WRITE ORDERED BY {', '.join(layout['sort_by'])}")


def apply(spark: SparkSession, src: dict, batch_id: int, authoritative: bool = False) -> dict:
    name = silver_name(src)
    changes = spark.table(bronze_name(src)).filter(F.col("_batch_id") == batch_id)
    if authoritative:   # a reload: only the reload file's rows count
        changes = changes.filter(F.col("_source_file").contains("/LOAD"))
    batch = latest(typed(changes, src), src)
    if not spark.catalog.tableExists(name):
        _create(spark, name, batch, src)
    batch.createOrReplaceTempView("silver_batch")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in src["primary_key"])
    by_source = (" WHEN NOT MATCHED BY SOURCE AND NOT t.is_deleted THEN UPDATE SET t.is_deleted = true"
                 if authoritative else "")
    before = {r.k: r.n for r in spark.sql(f"SELECT is_deleted AS k, count(*) n FROM {name} GROUP BY 1").collect()}
    spark.sql(f"""MERGE INTO {name} t USING silver_batch s ON {on}
                  WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *{by_source}""")
    after = {r.k: r.n for r in spark.sql(f"SELECT is_deleted AS k, count(*) n FROM {name} GROUP BY 1").collect()}
    return {"changes": changes.count(), "keys": batch.count(),
            "live": after.get(False, 0), "tombstones": after.get(True, 0),
            "live_delta": after.get(False, 0) - before.get(False, 0)}


def current(spark: SparkSession, src: dict) -> DataFrame:
    return spark.table(silver_name(src)).filter(~F.col("is_deleted"))


def build_history(spark: SparkSession, src: dict) -> int:
    """Every version of every row with its validity window, from the bronze change log (SCD2).
    History starts at the full load: what changed before 1 July is not in the log."""
    w = Window.partitionBy(*src["primary_key"]).orderBy("change_seq")
    hist = (typed(spark.table(bronze_name(src)), src)
            .dropDuplicates(src["primary_key"] + ["change_seq"])
            .withColumn("valid_from", F.col("cdc_ts"))
            .withColumn("valid_to", F.coalesce(F.lead("cdc_ts").over(w), F.lit("9999-12-31").cast("timestamp")))
            .withColumn("is_deleted", F.col("Op") == "D")
            .drop("Op"))
    hist.writeTo(table("silver", f"{src['table']}_history")).using("iceberg").createOrReplace()
    return hist.count()
