"""AWS Glue 5.0 job (FLEX): weekly Iceberg maintenance, the part of the bill nobody sees until it grows.

Compaction keeps Athena scans small after MERGE and streaming leave small files; snapshot expiry and
orphan removal keep S3 storage and metadata from growing for ever, and make erasure final.
Job parameters: --JOB_NAME --warehouse s3://nexus-lake/warehouse/ --keep_days 7
"""
import sys
from datetime import datetime, timedelta, timezone

from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME", "warehouse", "keep_days"])
spark = (SparkSession.builder
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.glue_catalog.warehouse", args["warehouse"])
         .getOrCreate())

cutoff = (datetime.now(timezone.utc) - timedelta(days=int(args["keep_days"]))).strftime("%Y-%m-%d %H:%M:%S")
SORTED = {"silver.ob_transactions": "customer_id ASC NULLS LAST, booked_at ASC NULLS LAST"}

for db in ("bronze", "silver", "gold"):
    for t in spark.sql(f"SHOW TABLES IN glue_catalog.{db}").collect():
        name = f"{db}.{t.tableName}"
        if name in SORTED:   # sorted tables keep their order when compacted, so min/max pruning keeps working
            spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{name}', strategy => 'sort', "
                      f"sort_order => '{SORTED[name]}')")
        else:
            spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{name}')")
        spark.sql(f"CALL glue_catalog.system.rewrite_manifests(table => '{name}')")
        spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{name}', "
                  f"older_than => TIMESTAMP '{cutoff}', retain_last => 3)")
        spark.sql(f"CALL glue_catalog.system.remove_orphan_files(table => '{name}', "
                  f"older_than => TIMESTAMP '{cutoff}')")
