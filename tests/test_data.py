"""The published run: what the site claims must be what the run did."""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
D = json.loads((ROOT / "assets/data/site.json").read_text())


def test_replay_changed_nothing():
    assert sum(t["new_changes"] for t in D["replay"].values()) == 0
    assert all(t["silver"]["live_delta"] == 0 for t in D["replay"].values())


def test_the_restart_duplicates_were_skipped():
    assert sum(t["duplicates"] for b in D["batches"] for t in b["tables"].values()) >= D["generator"]["planted"][
        "replayed_changes"]


def test_first_pass_caught_the_gap_and_the_reload_fixed_it():
    recon = {c["table"]: c for c in D["checks_first"] if c["check"].startswith("reconcile")}
    assert [t for t, c in recon.items() if c["status"] == "fail"] == ["repayments"]
    bad = recon["repayments"]["missing_in_lake"] + recon["repayments"]["different"]
    assert bad == len(D["incident"]["keys"])
    assert set(D["incident"]["keys"]) == set(D["generator"]["planted"]["gap_repayments"])
    assert D["incident"]["bronze_changes_in_that_hour_that_day"] == 0


def test_every_check_passes_after_the_fix():
    assert [c for c in D["checks"] if c["status"] != "pass"] == []


def test_erasure_tombstoned_and_purged():
    assert D["resurrection"]["live_rows_after_late_file"] == 0
    first = next(c for c in D["checks_first"] if c["check"].startswith("erased"))
    final = next(c for c in D["checks"] if c["check"].startswith("erased"))
    assert first["snapshots_with_pii"] > 0 and final["snapshots_with_pii"] == 0 and final["tombstone"]


def test_lake_row_counts_equal_the_source():
    for t, n in D["generator"]["source_rows"].items():
        assert D["tables"][t]["silver_live"] == n


def test_the_schema_change_was_picked_up_once():
    added = [(b["batch"], t) for b in D["batches"] for t, v in b["tables"].items() if v["columns_added"]]
    assert added == [(2, "loans")]


def test_change_seq_ordered_the_same_transaction():
    ch = D["story"]["changes"]
    ties = [(a, b) for a, b in zip(ch, ch[1:]) if a["cdc_ts"] == b["cdc_ts"]]
    assert ties and all(a["change_seq"] < b["change_seq"] for a, b in ties)
    assert D["story"]["silver"]["amount_paid"] > 0


@pytest.mark.parametrize("q", ["one_customer_90d", "one_month"])
def test_layout_reads_less_with_the_same_answer(q):
    r = D["layout"]["queries"][q]
    assert r["same_answer"]
    assert r["compacted"]["bytes_read"] < r["naive"]["bytes_read"]
    assert r["compacted"]["files_read"] < r["compacted"]["files_total"]


def test_calibration_shows_the_top_band_underpredicted():
    bands = D["gold_rows"]["calibration_by_band"]
    assert sum(b["loans"] for b in bands) == D["gold"]["calibration"]
    assert bands[-1]["observed"] > bands[-1]["predicted"]
    assert abs(bands[0]["observed"] - bands[0]["predicted"]) < 0.06


def test_point_in_time_differs_from_naive():
    f = D["gold_rows"]["features_pit"]
    assert 0 < f["differ"] <= f["applications"] and f["mean_naive"] > f["mean_pit"]


def test_no_model_ids_or_secrets_in_the_published_data():
    text = (ROOT / "assets/data/site.json").read_text()
    assert not re.search(r"AKIA[0-9A-Z]{16}|claude-", text)
