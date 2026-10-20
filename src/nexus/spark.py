"""One SparkSession with Iceberg for local runs and CI. On AWS Glue 5.0 the job's session is reused."""
import os
import sys

from pyspark.sql import SparkSession

from nexus.paths import CATALOG, ROOT, TARGET, table  # noqa: F401  (re-exported for the Spark modules)

ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"   # what Glue 5.0 ships with Spark 3.5


def get_spark(app: str = "nexus") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # Glue: the job configures glue_catalog (see glue/cdc_job.py)
        return active
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse = os.environ.get("NEXUS_WAREHOUSE", str(TARGET / "warehouse"))
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.jars.packages", ICEBERG)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.driver.memory", "3g")
        # Iceberg's Arrow reader allocates off-heap through Netty; plain row reads are steadier on small data
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    for layer in ("bronze", "silver", "gold", "ops", "lab"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")
    return spark
