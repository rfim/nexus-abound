"""Write what the site shows: the run log plus the gold tables, read back from Iceberg."""
import json

from pyspark.sql import SparkSession

from nexus.paths import ROOT
from nexus.spark import table

SITE = ROOT / "assets" / "data" / "site.json"


def _rows(spark, name, order=None):
    df = spark.table(table("gold", name))
    return [{k: (float(v) if hasattr(v, "is_finite") else str(v) if not isinstance(v, (int, float, str)) else v)
             for k, v in r.asDict().items()} for r in (df.orderBy(order) if order else df).collect()]


def pit_summary(spark: SparkSession) -> dict:
    s = spark.sql(f"""SELECT count(*) AS applications,
                             count_if(income_90d_naive <> income_90d_pit) AS differ,
                             CAST(avg(income_90d_pit) AS DOUBLE) AS mean_pit,
                             CAST(avg(income_90d_naive) AS DOUBLE) AS mean_naive,
                             CAST(max(income_90d_naive - income_90d_pit) AS DOUBLE) AS max_gap
                      FROM {table('gold', 'ds_features_pit')}""").first().asDict()
    ex = spark.sql(f"""SELECT * FROM {table('gold', 'ds_features_pit')}
                       ORDER BY income_90d_naive - income_90d_pit DESC, application_id LIMIT 1""").first()
    s["example"] = {"application_id": ex.application_id, "decided_at": str(ex.decided_at),
                    "income_90d_pit": float(ex.income_90d_pit), "income_90d_naive": float(ex.income_90d_naive)}
    return s


def export(spark: SparkSession, log: dict) -> dict:
    site = {k: v for k, v in log.items()}
    site["gold_rows"] = {"loan_book": _rows(spark, "report_loan_book_monthly", ["month_end", "status"]),
                         "calibration_by_band": _rows(spark, "ds_calibration_by_band", "band"),
                         "features_pit": pit_summary(spark)}
    SITE.parent.mkdir(parents=True, exist_ok=True)
    SITE.write_text(json.dumps(site, indent=1, default=str))
    return site
