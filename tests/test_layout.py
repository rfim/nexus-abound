"""File pruning: the plan reads only files whose partition and min/max bounds can match."""
from pyspark.sql import functions as F

from nexus import layout


def test_sorted_partitioned_files_prune_and_answer_the_same(spark):
    rows = [(f"T{i:05d}", f"C{i % 40:05d}", f"2026-0{7 + i % 3}-{1 + i % 27:02d} 10:00:00", float(i), False)
            for i in range(2000)]
    df = spark.createDataFrame(rows, "txn_id STRING, customer_id STRING, booked_at STRING, amount DOUBLE, "
                                     "is_deleted BOOLEAN").withColumn("booked_at", F.to_timestamp("booked_at"))
    flat, tuned = "lake.lab.t_flat", "lake.lab.t_tuned"
    for t in (flat, tuned):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    df.repartition(8).writeTo(flat).using("iceberg").create()
    df.limit(0).writeTo(tuned).using("iceberg").partitionedBy(F.months("booked_at")).create()
    spark.sql(f"ALTER TABLE {tuned} WRITE ORDERED BY customer_id, booked_at")
    df.repartition(8).writeTo(tuned).append()
    spark.sql("""CALL lake.system.rewrite_data_files(table => 'lab.t_tuned', strategy => 'sort',
                  sort_order => 'customer_id, booked_at', options => map('rewrite-all', 'true',
                  'target-file-size-bytes', '4096'))""")
    q = {"customer": "C00007", "from": "2026-08-01", "to": "2026-08-31"}
    a, b = layout.plan(spark, flat, q), layout.plan(spark, tuned, q)
    assert b["files_read"] < b["files_total"] and b["bytes_read"] < a["bytes_read"]
    assert layout.answer(spark, flat, q) == layout.answer(spark, tuned, q) != []
