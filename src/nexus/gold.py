"""Gold: what reporting and Data Science read, built only from silver (never from the production database).

  report_loan_book_monthly   loans by status at each month end, from the status history
  ds_calibration             one row per loan: the score at decision and what happened next
  ds_calibration_by_band     predicted against observed by score band, what model calibration reads
  ds_features_pit            the income feature as it was known at decision time, next to the naive one
"""
from pyspark.sql import SparkSession

from nexus.spark import table

MONTH_ENDS = ["2026-07-31 23:59:59", "2026-08-31 23:59:59", "2026-09-30 23:59:59"]
BANDS = [(0.0, 0.10), (0.10, 0.20), (0.20, 1.0)]   # approval stops at 35%, so the top band starts at 20%
OBSERVED_TO = "DATE'2026-09-26'"   # a miss is recorded 3 days after the due date; the data ends on 30 Sep


def _publish(spark: SparkSession, name: str, sql: str) -> int:
    df = spark.sql(sql)
    df.writeTo(table("gold", name)).using("iceberg").createOrReplace()
    return spark.table(table("gold", name)).count()


def build_loan_book(spark: SparkSession) -> int:
    ends = ", ".join(f"TIMESTAMP'{e}'" for e in MONTH_ENDS)
    return _publish(spark, "report_loan_book_monthly", f"""
        WITH ends AS (SELECT explode(array({ends})) AS month_end)
        SELECT date(e.month_end) AS month_end, h.status, count(*) AS loans,
               CAST(sum(h.principal) AS DECIMAL(14,2)) AS principal
        FROM ends e JOIN {table('silver', 'loans_history')} h
          ON h.valid_from <= e.month_end AND e.month_end < h.valid_to AND NOT h.is_deleted
        GROUP BY ALL ORDER BY month_end, status""")


def build_calibration(spark: SparkSession) -> int:
    n = _publish(spark, "ds_calibration", f"""
        WITH first3 AS (
          SELECT loan_id, count(*) AS due_count,
                 max(CASE WHEN status IN ('missed', 'paid_late') THEN 1 ELSE 0 END) AS missed_any
          FROM {table('silver', 'repayments')} WHERE NOT is_deleted AND instalment <= 3 GROUP BY loan_id
          HAVING max(due_date) <= {OBSERVED_TO})
        SELECT l.loan_id, a.application_id, a.pd_score, a.decided_at, date_trunc('month', l.originated_at) AS cohort,
               f.missed_any AS label
        FROM {table('silver', 'loans')} l
        JOIN {table('silver', 'applications')} a USING (application_id)
        JOIN first3 f USING (loan_id)
        WHERE NOT l.is_deleted AND NOT a.is_deleted AND f.due_count = 3""")
    cases = " ".join(f"WHEN pd_score < {hi} THEN '{int(lo * 100)}–{int(hi * 100)}%'" for lo, hi in BANDS[:-1])
    _publish(spark, "ds_calibration_by_band", f"""
        SELECT CASE {cases} ELSE '{int(BANDS[-1][0] * 100)}%+' END AS band,
               count(*) AS loans, CAST(avg(pd_score) AS DOUBLE) AS predicted, CAST(avg(label) AS DOUBLE) AS observed
        FROM {table('gold', 'ds_calibration')} GROUP BY 1 ORDER BY 1""")
    return n


def build_features_pit(spark: SparkSession) -> int:
    """Income in the 90 days before the decision. Point in time means: only rows we had fetched by then,
    with the category they had then. The naive version uses today's categories, after enrichment."""
    return _publish(spark, "ds_features_pit", f"""
        WITH apps AS (
          SELECT application_id, customer_id, decided_at FROM {table('silver', 'applications')}
          WHERE NOT is_deleted AND decided_at > TIMESTAMP'2026-07-01 00:00:00'),
        pit AS (
          SELECT a.application_id, sum(h.amount) AS income_90d
          FROM apps a JOIN {table('silver', 'ob_transactions_history')} h
            ON h.customer_id = a.customer_id AND h.booked_at >= a.decided_at - INTERVAL 90 DAYS
           AND h.booked_at < a.decided_at AND h.fetched_at <= a.decided_at
           AND h.valid_from <= a.decided_at AND a.decided_at < h.valid_to AND NOT h.is_deleted
          WHERE h.category = 'salary' GROUP BY 1),
        naive AS (
          SELECT a.application_id, sum(t.amount) AS income_90d
          FROM apps a JOIN {table('silver', 'ob_transactions')} t
            ON t.customer_id = a.customer_id AND t.booked_at >= a.decided_at - INTERVAL 90 DAYS
           AND t.booked_at < a.decided_at
          WHERE NOT t.is_deleted AND t.category = 'salary' GROUP BY 1)
        SELECT a.application_id, a.customer_id, a.decided_at,
               CAST(coalesce(p.income_90d, 0) AS DECIMAL(12,2)) AS income_90d_pit,
               CAST(coalesce(n.income_90d, 0) AS DECIMAL(12,2)) AS income_90d_naive
        FROM apps a LEFT JOIN pit p USING (application_id) LEFT JOIN naive n USING (application_id)""")


PRODUCTS = [build_loan_book, build_calibration, build_features_pit]
