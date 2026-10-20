"""Write the three downloads (fireside deck, one page, companion note) as HTML, filled from the run.

    python docs/pdf-src/build.py          # writes deck.html, one_page.html, companion.html here
    node scripts/pdfs.js                  # renders them to assets/*.pdf with Chromium

Every run figure comes from assets/data/site.json; every Lifepal figure is from my CV.
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
D = json.loads((HERE.parents[1] / "assets/data/site.json").read_text())
G, P = D["generator"], D["generator"]["planted"]
SITE = "rfim.github.io/nexus-abound"
n = lambda v: f"{v:,}"   # noqa: E731
pct = lambda v: f"{v * 100:.0f}%"   # noqa: E731

replay_new = sum(t["new_changes"] for t in D["replay"].values())
replay_read = sum(t["rows_read"] for t in D["replay"].values())
final = D["checks"]
passed = sum(c["status"] == "pass" for c in final)
first_fail = [c for c in D["checks_first"] if c["status"] == "fail"]
recon = next(c for c in first_fail if c["check"].startswith("reconcile"))
inc = D["incident"]
q1, q2 = D["layout"]["queries"]["one_customer_90d"], D["layout"]["queries"]["one_month"]
less = lambda q: f"{(1 - q['compacted']['bytes_read'] / q['naive']['bytes_read']) * 100:.0f}%"   # noqa: E731
bands = D["gold_rows"]["calibration_by_band"]
pit = D["gold_rows"]["features_pit"]
dups = sum(t["duplicates"] for b in D["batches"] for t in b["tables"].values())

PROOF = ("<span><b>24h → &lt;5m</b> reporting latency</span><span><b>6h → 90m</b> nightly batch</span>"
         "<span><b>$11k</b> vs $19k projection</span><span><b>−60%</b> ad-hoc requests</span>"
         "<span><b>500+</b> checks in CI</span><span><b>−60%</b> data-trust escalations</span>")
TILES = '<div class="tiles"><span>Aurora</span><span>DMS</span><span>Glue · Spark</span><span>Iceberg</span><span>Athena</span></div>'


def head(title, css):
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{title}</title>'
            f'<link rel="stylesheet" href="style.css"><style>{(HERE / css).read_text()}</style></head><body>')


def slide(i, kicker, title, lede, body):
    return (f'<section class="slide"><div class="kicker">{kicker}</div><h2>{title}</h2><p class="lede">{lede}</p>{body}'
            f'<div class="foot"><span>Nexus · Lifepal best practices · applied to Abound</span><span>Vim · {i:02d}</span></div></section>')


def table(rows, cols):
    th = "".join(f"<th>{c}</th>" for c in cols)
    return "<table><tr>" + th + "</tr>" + "".join("<tr>" + "".join(f'<td class="{"k" if j == 0 else ""}">{v}</td>'
                                                                         for j, v in enumerate(r)) + "</tr>" for r in rows) + "</table>"


# ---------------------------------------------------------------- the deck
def deck():
    s = [head("Nexus for Abound · Fireside chat", "deck.css")]
    s.append(f'''<section class="slide cover">
  <div class="tag"><div class="t1">PRESENTED TO</div><div class="t2">Abound</div><div class="t3">PLATFORM · DATA ENGINEER</div></div>
  <div class="built"><div><div class="k2">PLATFORM BUILT IN</div><span class="lp"><img src="lifepal.png" alt="Lifepal"></span></div></div>
  <div class="kicker" style="margin-top:14mm">Data engineering · Fireside chat · Nexus best practices from Lifepal</div>
  <h1>Nexus</h1>
  <div class="sub">Every change in Aurora, in the lake exactly once, proven against the source</div>
  <p class="desc">Four years owning the data lake and pipelines behind risk and fraud models at Lifepal, a regulated insurance marketplace: CDC from PostgreSQL and MySQL into an S3 lake with AWS DMS and Debezium, Spark and Glue jobs that merge changes idempotently, and reporting, ad-hoc analytics and model training moved off the production database. This pack applies those practices to Abound's stack, Aurora → DMS → Glue → Iceberg → Athena, in a runnable repository with synthetic data. Every run figure here comes from that run; every Lifepal figure is from my CV.</p>
  <div class="meta"><div><b>PRESENTED BY</b>R. Firman Insan Muhammad (Vim)<br>Data Engineer · London</div><div><b>TO</b>Platform team<br>Abound</div>
    <div><b>ROLE</b>Data Engineer<br>London hybrid</div><div><b>STACK</b>AWS DMS · Glue · Spark<br>Iceberg · Athena · Aurora</div></div>
  <div class="foot"><span>Nexus · Lifepal best practices · applied to Abound</span><span>Vim · 01</span></div></section>''')
    s.append(slide(2, "Contents", "What I'd like to cover", "Fifteen minutes on what I've built, then the parts of the role I'd own first. Every page points to working code.",
        '<table class="toc">' + "".join(f'<tr><td class="n">{a}</td><td><b>{b}</b></td><td>{c}</td></tr>' for a, b, c in [
            ("01", "Measured outcomes", "What changed at Lifepal, in numbers"),
            ("02", "How my experience maps to the role", "Line by line against the job description"),
            ("03", "The path", "Capture, land, apply, prove, serve"),
            ("04", "Six CDC problems", "What DMS really delivers, and what each takes"),
            ("05", "Proving the lake against Aurora", "The reconciliation that caught a DMS gap"),
            ("06", "Table design for speed and cost", "Partitioning, sort order, compaction; Athena bytes"),
            ("07", "The bridge to Data Science", "Calibration on the lake, point in time"),
            ("08", "Governance and erasure", "Quality rules, tombstones, snapshot expiry"),
            ("09", "On AWS", "DMS, Glue 5.0, Iceberg, Lambda and ECS on Fargate"),
            ("10", "My first 90 days", "Moving workloads into the Data Lake")]) + "</table>"))
    s.append(slide(3, "01 · Measured outcomes", "What changed at Lifepal", "Figures from my CV and production work, described at the level I can share publicly.",
        '<div class="grid nums">' + "".join(f'<div class="card"><div class="big">{a}</div><p>{b}</p></div>' for a, b in [
            ("99.7%", "reporting latency cut: 24 h → under 5 min with CDC"), ("75%", "nightly batch cut: 6 h → 90 min by redesigning the layout"),
            ("$11k", "monthly platform cost against a $19k projection (≈42% under)"), ("−60%", "ad-hoc requests after self-serve datasets on the lake"),
            ("−60%", "data-trust escalations, with 500+ quality checks in CI")]) + '</div>'
        '<div class="grid two" style="margin-top:7mm"><div class="callout"><b>How:</b> DMS and Kafka + Debezium from PostgreSQL and MySQL into S3; Spark and Glue jobs merging idempotently; partition pruning, Z-ordering and compaction; feature pipelines for LightGBM and XGBoost risk and fraud models.</div>'
        '<div class="callout pink"><b>Team:</b> I mentor 3–5 engineers; two were promoted to Senior. Blameless post-mortems that end in durable fixes.</div></div>'))
    s.append(slide(4, "02 · The role, line by line", "How my experience maps to the role", "The job description's asks, what I've done, and where it shows in this repository.",
        table([("Own the Data Lake and its ETL", "Owned the S3 lake and pipelines end to end at Lifepal", "src/nexus"),
               ("CDC, ideally AWS DMS", "DMS and Debezium from PostgreSQL and MySQL, merged idempotently", "bronze.py · silver.py"),
               ("Spark and Glue; RDS workloads into pipelines", "Moved reporting, analytics and training off RDS", "glue/cdc_job.py"),
               ("Strong SQL on Aurora PostgreSQL / MySQL", "Query tuning and indexing on transactional RDS", "checks.py"),
               ("Data structures and clustering", "Partitioning, Z-order, compaction: 6 h → 90 min", "layout.py"),
               ("Lower AWS cost", "$11k a month against a $19k projection", "layout.py"),
               ("Bridge to Data Science", "Point-in-time training sets, parity, drift, recalibration", "gold.py"),
               ("Governance and standards", "500+ checks in CI, PII masking, RBAC, lineage", "contracts/"),
               ("SOLID in practice", "Connectors behind common interfaces, config injected, PyTest", "src/nexus · tests/"),
               ("Iceberg", "Delta in production; Iceberg in this repo, one to one", "silver.py · layout.py")],
              ["The role asks for", "What I've done", "Here"])))
    s.append(slide(5, "03 · The path", "Aurora to the lake, proven", f"Five steps from a commit in the lending database to a table a data scientist can trust. The run: a full load, then three months of CDC in {len(D['batches'])} batches by arrival time.",
        '<div class="flow">' + "".join(f'<div class="card"><div class="kicker">{a}</div><h3>{b}</h3><p>{c}</p></div>' for a, b, c in [
            ("01 · Capture", "DMS", "Full load, then CDC: Op, cdc_ts and AR_H_CHANGE_SEQ on every row."),
            ("02 · Land", "Bronze", f"Every change keyed on (key, change_seq). A full replay changed {replay_new} rows."),
            ("03 · Apply", "MERGE", "Newest change_seq wins; deletes are tombstones; a reload is authoritative."),
            ("04 · Prove", f"{passed}/{len(final)}", "Row by row against Aurora; quality rules; freshness; erasure."),
            ("05 · Serve", "Iceberg", "Partitioned and sorted for Athena; point in time for calibration.")]) + '</div>'
        f'<div class="proof" style="margin-top:8mm"><span class="kicker">The run</span><span><b>{n(G["source_changes"])}</b> source changes</span><span><b>{G["cdc_files"] + 5}</b> DMS files</span><span><b>{n(replay_read)}</b> changes re-read on replay, <b>{replay_new}</b> new</span><span><b>{recon["rows_source"] - recon["match"]}</b> rows caught by reconciliation</span><span><b>{D["seconds"]:.0f}s</b> end to end</span></div>'))
    s.append(slide(6, "04 · Six CDC problems", "What DMS really delivers", "Each is planted in the synthetic data, handled by the pipeline and covered by a test.",
        table([("Re-sent after a task restart", f"{P['replayed_changes']} changes re-sent in a new file", f"Bronze keys on (key, change_seq): {dups} skipped"),
               ("A file delivered late", f"customers file for {P['late_file']['day']} landed {P['late_file']['arrived'][:10]}", "List by arrival; order by change_seq"),
               ("A delete, then an older update", f"{P['erased_customer']} erased; an earlier update arrived after", f"Tombstone: {D['resurrection']['live_rows_after_late_file']} live rows after"),
               ("A column added mid-stream", "ALTER TABLE loans ADD COLUMN apr_bps", "ADD COLUMNS in bronze, metadata only"),
               ("Two changes, one timestamp", f"{P['same_txn_cures']} late payments, two statements per transaction", "AR_H_CHANGE_SEQ orders them"),
               ("Changes DMS never wrote", f"{P['gap_changes']} repayment changes, 20 Sep 02:00–04:00", "Reconciliation to source, then a reload")],
              ["Problem", "In the run", "What handles it"])))
    s.append(slide(7, "05 · Proving the lake", "A check failed. The run fixed it.", "No key can catch a row that never arrived. Only a comparison with the source can.",
        f'''<div class="grid two"><div><div class="card"><h3>{inc["rows_affected"]} {inc["table"]} rows didn't match Aurora</h3><p>{inc["diagnosis"]}</p>
          <p style="margin-top:3mm">Bronze had <b>{inc["bronze_changes_in_that_hour_that_day"]}</b> changes in that hour, against a median of <b>{inc["bronze_changes_in_that_hour_other_days_median"]}</b> on other days.</p></div>
          <div class="callout pink" style="margin-top:4mm"><b>Fix:</b> {inc["remedy"]}</div></div>
          <div><div class="fname"><span>src/nexus/silver.py</span><span>a reload is authoritative</span></div><pre>MERGE INTO lake.silver.repayments t
USING silver_batch s ON t.repayment_id = s.repayment_id
WHEN MATCHED AND s.change_seq &gt; t.change_seq
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND NOT t.is_deleted
  THEN UPDATE SET t.is_deleted = true</pre>
          <p class="src">After the reload: <b>{passed}/{len(final)}</b> checks pass, including reconciliation on every table.</p></div></div>'''))
    s.append(slide(8, "06 · Speed, freshness and cost", "Read only what you need", "Athena bills bytes scanned at $5 per TB. The same queries, planned from Iceberg's own file statistics on three layouts.",
        table([(q["label"], f'{q["naive"]["bytes_read"] // 1024} KB · {q["naive"]["files_read"]}/{q["naive"]["files_total"]} files',
                f'{q["merged"]["bytes_read"] // 1024} KB · {q["merged"]["files_read"]}/{q["merged"]["files_total"]}',
                f'{q["compacted"]["bytes_read"] // 1024} KB · {q["compacted"]["files_read"]}/{q["compacted"]["files_total"]}', less(q)) for q in (q1, q2)],
              ["Query", "Naive (unpartitioned)", "months(booked_at)", "+ sorted, compacted", "Less scanned"])
        + '<div class="grid three" style="margin-top:6mm">' + "".join(f'<div class="card"><h3>{a}</h3><p>{b}</p></div>' for a, b in [
            ("Hidden partitioning", "months(booked_at): queries filter on the column, and the spec can evolve without a rewrite."),
            ("Sort order", "WRITE ORDERED BY customer_id, booked_at: min/max bounds prune files for per-customer reads."),
            ("Maintenance", "rewrite_data_files after MERGE; expire_snapshots and remove_orphan_files keep storage honest.")]) + '</div>'
        '<p class="src" style="margin-top:3mm">At Lifepal the same ideas, on Delta (partition pruning, Z-ordering, compaction, AQE), cut the nightly batch from 6 h to 90 min.</p>'))
    s.append(slide(9, "07 · The bridge to Data Science", "Calibrate on the lake, as of the decision", "Calibration needs the score at decision time, what happened next, and every feature as it was known then.",
        '<div class="grid two"><div>' + table([(b["band"], b["loans"], pct(b["predicted"]), pct(b["observed"])) for b in bands],
                                               ["Score band", "Loans", "Predicted", "Observed"])
        + f'<p class="src">gold.ds_calibration_by_band: the scorecard is too optimistic above 10%, and that gap is what recalibration fixes.</p></div>'
        f'<div><div class="card"><h3>Point in time, or a leak</h3><p>Open Banking enrichment relabels salary credits days after the fetch. A feature built from today\'s labels differs for <b>{pit["differ"]} of {pit["applications"]}</b> decisions, adding <b>£{pit["mean_naive"] - pit["mean_pit"]:,.0f}</b> of income on average that the model never saw.</p></div>'
        '<div class="callout" style="margin-top:4mm"><b>At Lifepal:</b> feature pipelines behind the customer-risk and claims-fraud models: point-in-time training sets, training/serving parity, drift monitoring and recalibration.</div></div></div>'))
    s.append(slide(10, "08 · Governance and erasure", "Standards that run as code", "Quality rules from the contract, reconciliation, freshness and erasure: checked on every run, failing loudly.",
        '<div class="grid three">' + "".join(f'<div class="card"><h3>{a}</h3><p>{b}</p></div>' for a, b in [
            ("Contracts", "contracts/sources.yml: keys, types, PII columns, what to reconcile. quality.yml: the rules on silver."),
            ("Erasure", f"Deletes become tombstones with values nulled; bronze is purged; expire_snapshots on {len(D['erasure']['expired_tables'])} tables so time travel can't recover PII."),
            ("Freshness", "Measured on a table that changes every day; in production, the DMS heartbeat and CDCLatencySource.")]) + '</div>'
        '<div class="callout" style="margin-top:6mm"><b>At Lifepal:</b> 500+ quality checks in CI, freshness and volume monitoring with alerting, PII masking, role-based access and lineage. Data-trust escalations fell by 60%.</div>'))
    s.append(slide(11, "09 · On AWS", "The same code, deployed", "Written to be read and reviewed; not applied, since there's no AWS account behind a portfolio.",
        '<div class="grid four">' + "".join(f'<div class="card"><div class="kicker">{a}</div><h3>{b}</h3><p>{c}</p></div>' for a, b, c in [
            ("Capture", "infra/dms/", "S3 target: CSV with header, TimestampColumnName=cdc_ts, AR_H_CHANGE_SEQ → change_seq."),
            ("Apply", "glue/cdc_job.py", "Glue 5.0 (Spark 3.5, Iceberg 1.7) with the Glue Data Catalog; the same bronze and silver code."),
            ("Around it", "Lambda · ECS on Fargate", "Lambda on S3 events and failed checks; Fargate tasks for reloads, backfills and the reconciliation export."),
            ("Provision", "infra/terraform/", "KMS, S3, DMS instance, endpoints and task, Glue job and schedule, least-privilege IAM.")]) + '</div>'))
    s.append(slide(12, "10 · My first 90 days", "Moving workloads into the Data Lake", "The role's first 6–12 months, as I'd start them.",
        '<div class="grid four">' + "".join(f'<div class="card"><h3>{a}</h3><p>{b}</p></div>' for a, b in [
            ("Days 1–30 · Listen and ship", "Learn the lake, the DMS tasks and the models from their owners. A small fix in production early."),
            ("Days 31–60 · Prove it", "Row-by-row reconciliation from Aurora for the tables Data Science uses; a change-volume alert per table."),
            ("Days 61–90 · Move one workload", "Model calibration on the lake, point in time, with Data Science; the RDS query switched off."),
            ("Day 90 · Measured", "Fresher data, a smaller Athena bill per query, and one fewer heavy read on Aurora.")]) + '</div>'
        f'<div class="proof" style="margin-top:8mm"><span class="kicker">Proven at Lifepal</span>{PROOF}</div>'
        f'<p class="src" style="margin-top:6mm">Everything in this pack runs at <b>{SITE}</b>. Synthetic data; an independent application, not affiliated with or endorsed by Abound.</p>'))
    return "".join(s) + "</body></html>"


# ---------------------------------------------------------------- one page
def one_page():
    return head("Nexus for Abound · One page", "one_page.css") + f'''<div class="page">
<div class="kicker">Fireside chat · prepared for the Platform team · Data Engineer</div>
<div class="top"><div><h1>Nexus, <b>for Abound.</b></h1>
  <div class="tl">Every change in Aurora, in the lake exactly once. Proven against the source. Ready for model calibration.</div>
  <p><b>NEXUS</b> is the platform I built and ran at Lifepal, a regulated insurance marketplace: CDC from PostgreSQL and MySQL into an S3 lake with AWS DMS and Debezium, Spark and Glue merges that are idempotent under replay, and the feature pipelines behind risk and fraud models. Here it is applied to Abound's stack, Aurora → DMS → Glue → Iceberg → Athena: every change keyed on its change sequence, every delete a tombstone, every table reconciled to the source, and calibration data built as of the decision. Everything below runs at <b>{SITE}</b>.</p></div>
  <div>{TILES}<div class="built"><div><div class="k2">BUILT IN</div><span class="lp"><img src="lifepal.png" alt="Lifepal" style="height:30px"></span></div><div class="k2" style="margin-left:6px">→ PRESENTED TO <b style="font-size:12pt;color:var(--ink)">Abound</b></div></div></div></div>

<div class="sec"><div class="sh"><b>The path</b><i>what runs in the repository</i></div>
<div class="spheres" style="grid-template-columns:repeat(5,1fr)">''' + "".join(f'<div class="sph"><div class="ball"><small>{a}</small><b>{b}</b><i>{c}</i></div><p>{d}</p></div>' for a, b, c, d in [
        ("01 · CAPTURE", "DMS", "full load + CDC", "Op, cdc_ts and AR_H_CHANGE_SEQ on every row."),
        ("02 · LAND", "Bronze", f"replay: {replay_new} rows", "Every change keyed on (key, change_seq)."),
        ("03 · APPLY", "MERGE", "newest seq wins", "Deletes are tombstones; a reload is authoritative."),
        ("04 · PROVE", f"{passed}/{len(final)}", "checks", f"Reconciliation caught {inc['rows_affected']} rows DMS never sent."),
        ("05 · SERVE", "Iceberg", "Athena · DS", f"{less(q1)} less scanned for a per-customer read.")]) + f'''</div></div>

<div class="sec"><div class="proof"><span class="kicker">Proven at Lifepal</span>{PROOF}</div></div>

<div class="sec cols"><div><div class="sh"><b>Six CDC problems</b><i>planted, handled, tested</i></div>''' + table([
        ("Restart re-send", f"{P['replayed_changes']} changes in a new file → skipped in bronze"),
        ("Late file", f"{P['late_file']['day']} file landed {P['late_file']['arrived'][:10]} → ordered by change_seq"),
        ("Delete, then older update", "tombstone, so no resurrection"),
        ("ALTER TABLE ADD COLUMN", "metadata-only ADD COLUMNS in bronze"),
        ("Same commit timestamp", f"{P['same_txn_cures']} two-statement transactions → change_seq"),
        ("Changes never delivered", f"{P['gap_changes']} changes → reconciliation + reload")], ["Problem", "Handled"]) + f'''</div>
<div><div class="sh"><b>For Data Science</b><i>calibration, point in time</i></div>''' + table([(b["band"], b["loans"], pct(b["predicted"]), pct(b["observed"])) for b in bands],
                                                                                                 ["Score band", "Loans", "Predicted", "Observed"]) + f'''
<p class="src">Point in time: the naive income feature differs for {pit["differ"]} of {pit["applications"]} decisions, by £{pit["mean_naive"] - pit["mean_pit"]:,.0f} on average.</p></div></div>

<div class="sec"><div class="sh"><b>If I joined the team</b><i>the first 90 days</i></div><div class="days">''' + "".join(
        f'<div class="card"><h3><span class="n">{i}</span>{a}</h3><p>{b}</p></div>' for i, (a, b) in enumerate([
            ("Days 1–30 · Listen and ship", "Learn the lake, DMS tasks and models from their owners. Ship a small fix early."),
            ("Days 31–60 · Prove it", "Reconciliation from Aurora for the tables Data Science uses; change-volume alerts."),
            ("Days 61–90 · Move one workload", "Model calibration on the lake, point in time; one RDS read switched off."),
            ("Day 90 · Measured", "Fresher data, cheaper queries, less load on Aurora.")], 1)) + f'''</div></div>

<div class="sec"><div class="sh"><b>The role, feature by feature</b><i>built in Lifepal · applied here</i></div>''' + table([
        ("AWS DMS · CDC", "DMS and Debezium from PostgreSQL and MySQL into S3", "bronze.py: keyed on (key, change_seq)"),
        ("Spark · Glue", "Moved reporting, analytics and training off RDS", "glue/cdc_job.py on Glue 5.0"),
        ("Iceberg", "Delta in production; the ideas map one to one", "MERGE, hidden partitions, sort order, compaction"),
        ("Athena · cost", "$11k a month against a $19k projection", f"{less(q1)} and {less(q2)} less scanned"),
        ("Governance", "500+ checks, PII masking, RBAC, lineage", "reconciliation, quality rules, erasure"),
        ("Lambda · ECS · Fargate", "Services on Lambda and ECS", "events, reloads and backfills around the job")], ["Feature", "At Lifepal", "Here"]) + f'''</div>
<p class="src" style="margin-top:5mm">R. Firman Insan Muhammad (Vim) · Data Engineer · London · right to work in the UK · firman.insan.m@gmail.com · {SITE}. Synthetic data; an independent application, not affiliated with or endorsed by Abound.</p>
</div></body></html>'''


# ---------------------------------------------------------------- companion note
def companion():
    return head("Nexus for Abound · Companion note", "companion.css") + f'''<div class="page">
<div class="kicker">Companion note · prepared for the Platform team · Data Engineer</div>
<div class="top"><div><h1>Different product, <b>same plumbing.</b></h1>
  <div class="tl">Why Lifepal's regulated insurance marketplace maps onto Abound's consumer lending platform.</div>
  <p>Lifepal sold regulated insurance; Abound lends, using Open Banking data and AI to decide. Both run on a transactional database that the business, its reports and its risk models all depend on, and that nobody should query for analytics. In both, data changes after the fact, a customer can ask to be forgotten, and a real financial decision depends on the number being right. The problems that follow are the ones NEXUS was built to solve.</p></div>
  <div>{TILES}</div></div>

<div class="sec"><div class="sh"><b>One flow, two businesses</b><i>Abound side from public material</i></div>
<div class="lane" style="grid-template-columns:repeat(5,1fr)">''' + "".join(f'<div><b>{a}</b>Lifepal · {b}<br>Abound · {c}</div>' for a, b, c in [
        ("01 Source", "PostgreSQL and MySQL", "Aurora PostgreSQL / MySQL"),
        ("02 Capture", "DMS and Debezium", "AWS DMS"),
        ("03 Apply", "Spark and Glue on Delta", "Spark on Glue, Iceberg"),
        ("04 Prove", "500+ checks in CI", "reconciliation and rules"),
        ("05 Decide", "risk and fraud models", "underwriting and calibration")]) + '''</div></div>

<div class="sec"><div class="sh"><b>The same problem</b><i>how I handled it · where it shows here</i></div>''' + table([
        ("Analytics on the production DB", "Moved reporting, ad-hoc analytics and model training onto the lake", "The role's first 6–12 months; gold tables in gold.py"),
        ("Changes arrive twice, late, out of order", "Merged on the source log position, idempotent under replay", f"change_seq ordering; a full replay changed {replay_new} rows"),
        ("Rows that never arrive", "Reconciliation against a read replica, then a reload", f"{inc['rows_affected']} rows caught, reloaded, {passed}/{len(final)} checks pass"),
        ("Features as they were known", "Point-in-time training sets for risk and fraud", f"PIT income differs for {pit['differ']} of {pit['applications']} decisions"),
        ("Cost without losing reliability", "$11k a month against a $19k projection", f"{less(q1)} less scanned per customer read"),
        ("Right to erasure", "PII masking, role-based access, deletes through CDC", "tombstones, bronze purge, expire_snapshots")],
        ["The problem", "At Lifepal", "Here"]) + f'''</div>

<div class="sec cols"><div><div class="sh"><b>Stated plainly</b><i>what's real</i></div>
<div class="plain"><b>Real:</b> the Lifepal outcomes, from my CV and production work, and all the code in the repository, which runs end to end in CI.<br><br>
<b>Synthetic:</b> every customer, loan, repayment and transaction. The Abound side comes from the job description and public information; no Abound systems or data were used.<br><br>
<b>Depth:</b> DMS, Spark, Glue, Aurora/RDS PostgreSQL and MySQL, Lambda and ECS are production experience. Delta is my production table format; this repository is my Iceberg work. Athena is working level.</div></div>
<div><div class="sh"><b>Questions I'd bring</b><i>for the Platform team</i></div><ul>
<li>Which workloads still read Aurora directly, and which would Data Science move first?</li>
<li>How are DMS gaps and restarts detected today: heartbeat, volume, or reconciliation?</li>
<li>Where do model calibration and its features live now, and how fresh do they need to be?</li>
<li>Which AWS line items are growing fastest: Glue, Athena, S3 or DMS?</li>
<li>How do erasure requests reach the lake today?</li></ul></div></div>
<p class="src" style="margin-top:6mm">Described at the level I can share publicly; I'm happy to walk through details in conversation. {SITE}</p>
</div></body></html>'''


for name, fn in (("deck.html", deck), ("one_page.html", one_page), ("companion.html", companion)):
    (HERE / name).write_text(fn())
print("wrote deck.html, one_page.html, companion.html")
