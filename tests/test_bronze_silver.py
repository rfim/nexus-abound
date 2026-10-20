"""CDC semantics on small hand-written DMS files: replay, ordering, tombstones, reloads, schema change."""
from conftest import write_dms
from pyspark.sql import functions as F

from nexus import bronze, silver

H = ["Op", "cdc_ts", "change_seq", "loan_id", "status", "principal"]


def src(name):
    return {"table": name, "primary_key": ["loan_id"],
            "columns": {"loan_id": "string", "status": "string", "principal": "decimal(12,2)"}}


def seq(ts, n):
    return ts.replace("-", "").replace(" ", "").replace(":", "") + f"{n:021d}"


def state(spark, s):
    return {r.loan_id: (r.status, r.is_deleted) for r in spark.table(silver.silver_name(s)).collect()}


def test_replayed_file_changes_nothing(spark, tmp_path):
    s = src("t_replay")
    rows = [["I", "2026-07-01 00:00:00", seq("2026-07-01 00:00:00", 0), "L1", "active", "1000.00"],
            ["U", "2026-07-02 03:00:00", seq("2026-07-02 03:00:00", 5), "L1", "in_arrears", "1000.00"]]
    f1 = write_dms(tmp_path / "a.csv", H, rows)
    f2 = write_dms(tmp_path / "restart.csv", H, rows)            # DMS re-sends the same changes in a new file
    first = bronze.ingest(spark, s, [f1], 0)
    again = bronze.ingest(spark, s, [f2, f1], 1)
    assert first["new_changes"] == 2 and again["new_changes"] == 0 and again["duplicates"] == 4
    silver.apply(spark, s, 0)
    silver.apply(spark, s, 1)
    assert state(spark, s) == {"L1": ("in_arrears", False)}


def test_change_seq_orders_changes_with_the_same_commit_time(spark, tmp_path):
    s = src("t_ties")
    ts = "2026-08-16 16:00:00"
    f = write_dms(tmp_path / "b.csv", H, [["U", ts, seq(ts, 8), "L2", "active", "50.00"],     # second statement
                                         ["U", ts, seq(ts, 7), "L2", "paid_late", "0.00"]])   # first statement
    bronze.ingest(spark, s, [f], 0)
    silver.apply(spark, s, 0)
    row = silver.current(spark, s).first()
    assert (row.status, float(row.principal)) == ("active", 50.0)


def test_a_delete_is_a_tombstone_a_late_update_cannot_undo(spark, tmp_path):
    s = src("t_tomb")
    load = write_dms(tmp_path / "load.csv", H, [["I", "2026-07-01 00:00:00", seq("2026-07-01 00:00:00", 0), "L3",
                                                 "active", "10.00"]])
    delete = write_dms(tmp_path / "del.csv", H, [["D", "2026-09-10 11:00:00", seq("2026-09-10 11:00:00", 9), "L3",
                                                  "", ""]])
    late = write_dms(tmp_path / "late.csv", H, [["U", "2026-09-03 14:05:00", seq("2026-09-03 14:05:00", 4), "L3",
                                                 "repaid", "10.00"]])
    for b, f in enumerate([load, delete, late]):
        bronze.ingest(spark, s, [f], b)
        silver.apply(spark, s, b)
    assert state(spark, s) == {"L3": (None, True)}               # deleted, values nulled, not resurrected
    assert silver.current(spark, s).count() == 0


def test_a_reload_is_authoritative(spark, tmp_path):
    s = src("t_reload")
    t0 = "2026-07-01 00:00:00"
    bronze.ingest(spark, s, [write_dms(tmp_path / "l.csv", H, [["I", t0, seq(t0, 0), "A", "active", "1.00"],
                                                                ["I", t0, seq(t0, 0), "B", "active", "2.00"]])], 0)
    silver.apply(spark, s, 0)
    t1 = "2026-10-01 07:30:00"     # the reload: B was deleted at source during a gap DMS never delivered
    bronze.ingest(spark, s, [write_dms(tmp_path / "LOAD202610010730.csv", H, [["I", t1, seq(t1, 0), "A", "repaid",
                                                                                "1.00"]])], 1)
    silver.apply(spark, s, 1, authoritative=True)
    assert state(spark, s) == {"A": ("repaid", False), "B": ("active", True)}


def test_a_new_source_column_becomes_a_new_bronze_column(spark, tmp_path):
    s = src("t_alter") | {"columns": {"loan_id": "string", "status": "string", "principal": "decimal(12,2)",
                                      "apr_bps": "int"}}
    t0, t1 = "2026-07-01 00:00:00", "2026-08-01 09:00:00"
    old = write_dms(tmp_path / "old.csv", H, [["I", t0, seq(t0, 0), "L4", "active", "5.00"]])
    new = write_dms(tmp_path / "new.csv", H + ["apr_bps"], [["I", t1, seq(t1, 3), "L5", "active", "6.00", "2490"]])
    bronze.ingest(spark, s, [old], 0)
    out = bronze.ingest(spark, s, [new], 1)
    assert out["columns_added"] == ["apr_bps"]
    silver.apply(spark, s, 0)
    silver.apply(spark, s, 1)
    got = {r.loan_id: r.apr_bps for r in silver.current(spark, s).collect()}
    assert got == {"L4": None, "L5": 2490}


def test_history_has_one_open_version_per_row(spark, tmp_path):
    s = src("t_hist")
    rows = [["I", "2026-07-01 00:00:00", seq("2026-07-01 00:00:00", 0), "L6", "active", "9.00"],
            ["U", "2026-08-10 03:00:00", seq("2026-08-10 03:00:00", 2), "L6", "in_arrears", "9.00"],
            ["U", "2026-08-20 10:00:00", seq("2026-08-20 10:00:00", 3), "L6", "active", "9.00"]]
    bronze.ingest(spark, s, [write_dms(tmp_path / "h.csv", H, rows)], 0)
    assert silver.build_history(spark, s) == 3
    h = spark.table("lake.silver.t_hist_history").orderBy("change_seq").collect()
    assert [r.status for r in h] == ["active", "in_arrears", "active"]
    assert h[0].valid_to == h[1].valid_from and str(h[-1].valid_to).startswith("9999")
    as_of_aug15 = spark.table("lake.silver.t_hist_history").filter(
        (F.col("valid_from") <= "2026-08-15") & (F.col("valid_to") > "2026-08-15")).first()
    assert as_of_aug15.status == "in_arrears"
