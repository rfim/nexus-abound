"""The simulated lender: deterministic, and every planted problem is really in the change log."""
import functools
import random

from nexus import generate as g


def fresh():
    return g.simulate(random.Random(g.SEED))


@functools.lru_cache(maxsize=1)
def sim():
    """One simulated year shared by the read-only tests (each run takes seconds; in a browser, more)."""
    return fresh()


def test_deterministic():
    (a, fa), (b, fb) = sim(), fresh()
    assert len(a.log) == len(b.log) and a.log[-1][:4] == b.log[-1][:4] and fa == fb


def test_log_is_in_commit_order_and_seq_is_unique():
    db, _ = sim()
    assert [r[1] for r in db.log] == sorted(r[1] for r in db.log)
    assert all(x[0] <= y[0] for x, y in zip(db.log, db.log[1:]))
    seqs = [g._seq(t, n) for t, n, *_ in db.log]
    assert len(set(seqs)) == len(seqs) and seqs == sorted(seqs)      # sortable as strings, like AR_H_CHANGE_SEQ


def test_same_transaction_changes_share_a_commit_time():
    db, facts = sim()
    assert facts["same_txn_cures"] > 0
    pairs = [(x, y) for x, y in zip(db.log, db.log[1:]) if x[2] == y[2] == "repayments"
             and x[4]["repayment_id"] == y[4]["repayment_id"] and x[0] == y[0]]
    assert pairs and all(x[4]["amount_paid"] == 0.0 and y[4]["amount_paid"] > 0 for x, y in pairs)


def test_the_erased_customer_changed_before_being_deleted():
    db, facts = sim()
    cid = facts["erased_customer"]
    ops = [(t, op) for t, _, table, op, row in db.log if table == "customers" and row["customer_id"] == cid]
    assert ops[-1] == (g.ERASED_AT, "D")
    assert any(t.date() == g.LATE_FILE[1] and op == "U" for t, op in ops)
    assert cid not in db.state["customers"]


def test_the_gap_window_has_repayment_changes_to_lose():
    db, _ = sim()
    assert sum(1 for t, _, table, *_ in db.log if table == "repayments" and g.GAP[0] <= t < g.GAP[1]) > 5


def test_apr_appears_only_after_the_alter():
    db, _ = sim()
    loans = [row for t, _, table, op, row in db.log if table == "loans" and op == "I"]
    assert all(r.get("apr_bps") is None for r in loans if r["originated_at"] < g.APR_ADDED_AT)
    assert any(r.get("apr_bps") for r in loans if r["originated_at"] >= g.APR_ADDED_AT)
    assert "apr_bps" not in g._cols("loans", g.FULL_LOAD_AT) and "apr_bps" in g._cols("loans", g.APR_ADDED_AT)


def test_enrichment_relabels_salaries_after_the_fetch():
    db, facts = sim()
    assert facts["recategorised"] > 100
    ups = [(t, row) for t, _, table, op, row in db.log if table == "ob_transactions" and op == "U"]
    assert all(row["category"] == "salary" and t > row["fetched_at"] for t, row in ups)
