"""Bronze: every change DMS delivered, kept as it arrived, in an Iceberg table per source table.

Files are listed by arrival time (S3 LastModified), not by the date in their path, so a file DMS
delivers late is picked up by the next run. Each change is keyed by (primary key, change_seq):
a change that DMS re-sends after a task restart, in a new file, is recognised and skipped.
"""
import csv
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nexus.generate import DMS
from nexus.spark import ROOT, table

META = ["Op", "cdc_ts", "change_seq"]


def bronze_name(src: dict) -> str:
    return table("bronze", f"{src['table']}_changes")


def list_arrivals(src: dict, until: datetime, since: datetime | None = None) -> list[Path]:
    """Like listing s3://bucket/lending/<table>/ and filtering on LastModified."""
    out = []
    for p in sorted((DMS / src["table"]).rglob("*.csv")):
        arrived = datetime.fromtimestamp(p.stat().st_mtime)
        if arrived <= until and (since is None or arrived > since):
            out.append(p)
    return out


def _rel(p: Path) -> str:
    return str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)


def _header(p: Path) -> tuple[str, ...]:
    with p.open() as f:
        return tuple(next(csv.reader(f)))


def read_files(spark: SparkSession, files: list[Path]) -> DataFrame:
    """Read DMS CSVs as strings. Files with different headers (a column added mid-stream) are read apart
    and unioned by name, because Spark takes one header for every file in a single read."""
    groups: dict[tuple, list[str]] = {}
    for p in files:
        groups.setdefault(_header(p), []).append(str(p))
    frames = [spark.read.option("header", True).option("inferSchema", False).csv(paths)
              .withColumn("_source_file", F.col("_metadata.file_path")) for paths in groups.values()]
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f, allowMissingColumns=True)
    return df


def _evolve(spark: SparkSession, name: str, df: DataFrame) -> list[str]:
    """New source columns become new bronze columns. Iceberg adds them as metadata only: no rewrite."""
    have = {f.name for f in spark.table(name).schema}
    added = [c for c in df.columns if c not in have]
    if added:
        spark.sql(f"ALTER TABLE {name} ADD COLUMNS ({', '.join(f'`{c}` STRING' for c in added)})")
    return added


def ingest(spark: SparkSession, src: dict, files: list[Path], batch_id: int) -> dict:
    name, pk = bronze_name(src), src["primary_key"]
    log = table("ops", "ingested_files")
    if not files:
        return {"files": 0, "rows_read": 0, "new_changes": 0, "duplicates": 0, "columns_added": []}
    df = (read_files(spark, files)
          .withColumn("cdc_ts", F.to_timestamp("cdc_ts"))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_ingested_at", F.current_timestamp()))
    rows_read = df.count()
    # one copy of each change inside the batch; MERGE then skips any copy bronze already has
    w = Window.partitionBy(*pk, "change_seq").orderBy("_source_file")
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    if not spark.catalog.tableExists(name):
        df.limit(0).writeTo(name).using("iceberg").partitionedBy(F.days("cdc_ts")) \
            .tableProperty("format-version", "2").create()
    added = _evolve(spark, name, df)
    before = spark.table(name).count()
    df.createOrReplaceTempView("batch")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pk + ["change_seq"])
    spark.sql(f"MERGE INTO {name} t USING batch s ON {on} WHEN NOT MATCHED THEN INSERT *")
    new = spark.table(name).count() - before

    rel = [_rel(p) for p in files]
    arrived = [datetime.fromtimestamp(p.stat().st_mtime) for p in files]
    entries = spark.createDataFrame([(src["table"], r, a, batch_id) for r, a in zip(rel, arrived)],
                                    "table STRING, path STRING, arrived_at TIMESTAMP, batch_id INT")
    if not spark.catalog.tableExists(log):
        entries.writeTo(log).using("iceberg").create()
    else:
        entries.writeTo(log).append()
    return {"files": len(files), "rows_read": rows_read, "new_changes": new, "duplicates": rows_read - new,
            "columns_added": added}


def already_ingested(spark: SparkSession, src: dict) -> set[str]:
    log = table("ops", "ingested_files")
    if not spark.catalog.tableExists(log):
        return set()
    return {r.path for r in spark.table(log).filter(F.col("table") == src["table"]).select("path").collect()}


def new_files(spark: SparkSession, src: dict, until: datetime) -> list[Path]:
    seen = already_ingested(spark, src)
    return [p for p in list_arrivals(src, until) if _rel(p) not in seen]
