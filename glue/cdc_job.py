"""AWS Glue 5.0 job: land the DMS files that arrived since the last run, apply them to silver, prove it.

Glue 5.0 ships Spark 3.5 and Iceberg 1.7; with --datalake-formats iceberg the runtime is on the class path.
The job only configures the Glue Data Catalog as `glue_catalog`; everything else is the same Python
that runs locally and in CI (shipped with --extra-py-files nexus.zip).

Job parameters: --JOB_NAME --warehouse s3://nexus-lake/warehouse/ --until <ISO time, default now>
"""
import os
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME", "warehouse"])
os.environ["NEXUS_CATALOG"] = "glue_catalog"

spark = (SparkSession.builder
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.glue_catalog.warehouse", args["warehouse"])
         .getOrCreate())
glue = GlueContext(SparkContext.getOrCreate())
job = Job(glue)
job.init(args["JOB_NAME"], args)

from nexus import bronze, checks, silver  # noqa: E402  (after NEXUS_CATALOG is set)
from nexus.contracts import load_sources  # noqa: E402

until = datetime.now(timezone.utc).replace(tzinfo=None)
batch_id = int(until.strftime("%Y%m%d%H"))
failed = []
for src in load_sources():
    # on S3, bronze.list_arrivals is an s3 list filtered on LastModified; the file log makes it incremental
    files = bronze.new_files(spark, src, until)
    bronze.ingest(spark, src, files, batch_id)
    silver.apply(spark, src, batch_id)
    failed += [c for c in checks.quality(spark, src) if c["status"] == "fail"]

job.commit()
if failed:   # the job fails loudly; Step Functions routes it to on-call with the failing rule names
    raise SystemExit(f"quality rules failed: {[c['table'] + '.' + c['check'] for c in failed]}")
