"""Write the three downloads (fireside deck, one page, companion note) as HTML, filled from the run.

    python docs/pdf-src/build.py          # writes deck.html, one_page.html, companion.html here
    node scripts/pdfs.js                  # renders them to assets/*.pdf with Chromium

Every run figure comes from assets/data/site.json; every Lifepal figure is from my CV.
"""
import html
import json
import re
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
ROOT = HERE.parents[1]
KW = r"\b(def|return|if|not|for|in|else|and|or|import|from|with|lambda|MERGE|INTO|USING|ON|WHEN|MATCHED|THEN|UPDATE|SET|INSERT|SELECT|FROM|WHERE|JOIN|LEFT|AND|NOT|BY|SOURCE|GROUP|CALL|ALTER|TABLE|WRITE|ORDERED|PARTITIONED|resource)\b"


def _hl(line: str) -> str:
    """A small highlighter in the deck's colours: comments, strings, keywords."""
    code, com = line, ""
    m = re.search(r"(\s#\s.*|^\s*#.*|\s--\s.*|^\s*--.*)$", line)
    if m and line.count('"', 0, m.start()) % 2 == 0:
        code, com = line[:m.start()], line[m.start():]
    parts = re.split(r'(f?"[^"]*"|\'[^\']*\')', code)
    out = "".join(f'<span class="s">{html.escape(x)}</span>' if i % 2 else re.sub(KW, r'<span class="k">\1</span>', html.escape(x))
                  for i, x in enumerate(parts))
    return out + (f'<span class="c">{html.escape(com)}</span>' if com else "")


def code(path: str, start: str, end: str | None = None, drop=(), limit: int = 26) -> str:
    """Lines of a real repository file, from the line containing start up to the one containing end."""
    lines = (ROOT / path).read_text().splitlines()
    i = next(k for k, l in enumerate(lines) if start in l)
    j = next((k for k in range(i + 1, len(lines)) if end and end in lines[k]), len(lines))
    body = [l for l in lines[i:j] if not any(d in l for d in drop)]
    while body and not body[-1].strip():
        body.pop()
    return "\n".join(_hl(l) for l in body[:limit])


def code_slide(kicker, title, lede, path, tech, src, side, formula=""):
    left = (f'<div class="formula">{formula}</div>' if formula else "") + \
        f'<div class="fname"{" style=\"margin-top:6px\"" if formula else ""}><span>{path}</span><span>{tech}</span></div><pre>{src}</pre>'
    return slide(0, kicker, title, lede, f'<div class="grid split"><div>{left}</div><div class="side">{side}</div></div>')


def side(*blocks) -> str:
    return "".join(f"<h4>{h}</h4>" + "".join(f"<p>{x}</p>" for x in ps) for h, ps in blocks)


TILES = '<div class="tiles"><span>Aurora</span><span>DMS</span><span>Glue · Spark</span><span>Iceberg</span><span>Athena</span></div>'


def head(title, css):
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{title}</title>'
            f'<link rel="stylesheet" href="style.css"><style>{(HERE / css).read_text()}</style></head><body>')


_N = [1]


def slide(_i, kicker, title, lede, body):
    _N[0] += 1
    i = _N[0]
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
            ("04", "Six CDC problems, and the code", "What DMS really delivers; bronze and silver MERGE"),
            ("05", "Proving the lake against Aurora", "The reconciliation that caught a DMS gap, and its code"),
            ("06", "Table design and platform cost", "Layout, its code, and where the AWS bill goes"),
            ("07", "The bridge to Data Science", "Calibration on the lake; a point-in-time feature in SQL"),
            ("08", "Governance and erasure", "Quality rules, tombstones, snapshot expiry"),
            ("09", "On AWS", "DMS, the Glue 5.0 job, Lambda and ECS on Fargate"),
            ("10", "My first 90 days, and a summary", "Moving workloads into the Data Lake")]) + "</table>"))
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
    s.append(code_slide("04 · Land · PySpark on Iceberg", "Safe re-runs: bronze keyed on the change",
        "Every DMS file can arrive twice, late or out of order. Bronze keeps one row per change, so a re-run or a re-send changes nothing.",
        "src/nexus/bronze.py", "PySpark · Iceberg 1.7",
        code("src/nexus/bronze.py", "def ingest(", "rel = [_rel(p)"),
        side(("The guarantee", ["Each change is identified by its primary key and <b>AR_H_CHANGE_SEQ</b>, which DMS makes unique and increasing. A file name is not an identity: after a restart DMS re-sends the same changes in a <b>new</b> file.",
                                 "At-least-once delivery from DMS becomes an exactly-once effect on the table."]),
             ("At Lifepal", ["CDC from PostgreSQL and MySQL with DMS and with Kafka + Debezium, merged idempotently by Spark and Glue jobs. Reporting latency fell from 24 hours to under 5 minutes."]),
             ("In the run", [f"DMS re-sent <b>{P['replayed_changes']}</b> changes after a restart: skipped. Replaying every file DMS ever wrote, <b>{n(replay_read)}</b> changes, added <b>{replay_new}</b> rows.",
                             "A column added mid-stream (<b>apr_bps</b>) becomes ALTER TABLE ADD COLUMNS: metadata only, no rewrite."])),
        formula="ingest(ingest(B, F), F) = ingest(B, F) · identity = (key, change_seq)"))
    s.append(code_slide("04 · Apply · PySpark on Iceberg", "The newest change wins. Deletes stay deleted.",
        "Silver holds the current state of every Aurora table. Three rules make it right whatever order the changes arrive in.",
        "src/nexus/silver.py", "PySpark · Iceberg MERGE",
        code("src/nexus/silver.py", "def latest(", "def _create(") + "\n\n" + code("src/nexus/silver.py", "    on = ", "    after = ", drop=("before =",)),
        side(("Three rules", ["<b>change_seq decides.</b> Two statements in one transaction share a cdc_ts; only the sequence orders them.",
                              "<b>Deletes are tombstones</b> that keep their change_seq, so an older update arriving late can't resurrect the row. Non-key values are nulled: erasure means erasure.",
                              "<b>A reload is authoritative:</b> WHEN NOT MATCHED BY SOURCE tombstones what the source no longer has."]),
             ("In the run", [f"{P['same_txn_cures']} two-statement transactions ordered correctly; <b>{P['erased_customer']}</b>'s late update arrived after the delete and left <b>{D['resurrection']['live_rows_after_late_file']}</b> live rows."]))))
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
    s.append(code_slide("05 · Prove · PySpark", "Reconciliation: the check no key can replace",
        "Silver against the source's own rows, on every contract column. It finds what a CDC stream can't tell you: the changes that never came.",
        "src/nexus/checks.py", "PySpark",
        code("src/nexus/checks.py", "def reconcile(", "    counts = "),
        side(("Why it exists", ["Duplicates and late files are visible in the stream. A <b>missing</b> change isn't: keys look fine, counts look fine, and a month-end report is quietly wrong."]),
             ("In the run", [f"DMS skipped <b>{P['gap_changes']}</b> repayment changes on 20 September. Reconciliation found <b>{len(inc['keys'])}</b> rows that differ; the investigation showed <b>{inc['bronze_changes_in_that_hour_that_day']}</b> bronze changes in an hour that normally has <b>{inc['bronze_changes_in_that_hour_other_days_median']}</b>.",
                             f"Reload, apply as authoritative, check again: <b>{passed}/{len(final)}</b> pass."]),
             ("On AWS", ["Run it nightly against an Aurora <b>read replica</b>, never the writer, and alert on hourly change volume per table so a gap pages before a report finds it."]))))
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
    s.append(code_slide("06 · Table design · Iceberg", "Partition, sort, compact",
        "The layout is part of the table's definition, not a tuning afterthought, and maintenance runs on a schedule.",
        "src/nexus/silver.py · src/nexus/layout.py", "Iceberg 1.7",
        code("src/nexus/silver.py", "def _create(", "def apply(") + "\n\n" + code("src/nexus/layout.py", "def compact(", "def files("),
        side(("What each does", ["<b>months(booked_at)</b>: a hidden partition transform. Queries filter on booked_at; the spec can evolve without rewriting data.",
                                 "<b>WRITE ORDERED BY customer_id</b>: each file covers a narrow range of customers, so min/max bounds skip files.",
                                 "<b>rewrite_data_files</b>: MERGE and streaming leave small files; compaction puts them back in order."]),
             ("Measured here", [f"Per-customer read: <b>{less(q1)}</b> less scanned. Monthly report: <b>{less(q2)}</b> less. Same answers on every layout."]),
             ("At Lifepal", ["The same ideas on Delta (partition pruning, Z-ordering, compaction, AQE) cut the nightly batch from 6 h to 90 min, with the platform at $11k a month against a $19k projection."]))))
    s.append(code_slide("06 · Unit cost · AWS", "Managing platform cost",
        "Cost is a defect like any other: attributed to a team, visible every week, and fixed where it starts.",
        "infra/finops/cost_by_team.sql · infra/terraform/main.tf", "Athena over the CUR · Terraform",
        code("infra/finops/cost_by_team.sql", "SELECT", None, limit=14) + "\n\n" + code("infra/terraform/main.tf", 'resource "aws_athena_workgroup"', 'resource "aws_glue_job" "maintenance"', limit=15),
        side(("Four mechanisms", ["<b>Attribution.</b> Every resource carries an owning team and a workload tag; the Cost and Usage Report, queried in Athena, says who spends what.",
                                  "<b>Guard-rails.</b> One Athena workgroup per audience with a per-query scan cap, so a careless dashboard can't scan the lake on every refresh.",
                                  "<b>Right-sized compute.</b> Glue auto-scaling for the hourly apply; FLEX execution for backfills and maintenance.",
                                  "<b>A budget alert acted on weekly,</b> in the same forum as incidents."]),
             ("At Lifepal", ["The platform ran at about <b>$11k a month against a $19k projection</b>. Most of it came from layout (partition pruning, Z-ordering, compaction) and from refreshing data only as often as people need it."]))))
    s.append(slide(0, "06 · Unit cost · AWS", "Where the bill goes, and the lever for each",
        "On Abound's stack the services are few and the levers are known. Measure first, fix the biggest line, prove nothing broke.",
        table([("Athena", "bytes scanned, $5 per TB", f"Partition and sort; compact; pre-aggregate for dashboards; workgroup scan caps. Here: {less(q1)} less per customer read"),
               ("Glue", "DPU-hours", "Only new files (the file log), auto-scaling, FLEX for non-urgent jobs, no hourly run that nobody reads hourly"),
               ("S3", "storage and requests", "expire_snapshots and remove_orphan_files weekly; landing files expire after 30 days; Intelligent-Tiering for cold data"),
               ("DMS", "replication instance hours", "Right-size the instance; replicate only the tables and columns that are used"),
               ("Aurora", "instance size and replicas", "Move analytics reads to the lake, so the writer never grows for reporting load")],
              ["Service", "What you pay for", "The lever"])
        + '<div class="grid three" style="margin-top:6mm">' + "".join(f'<div class="card"><h3>{a}</h3><p>{b}</p></div>' for a, b in [
            ("1 · Measure", "Spend by team and workload from the CUR; Athena bytes by workgroup from CloudTrail (infra/finops/)."),
            ("2 · Fix the biggest line", "Usually small files and full scans in Athena, or over-provisioned, over-frequent Glue jobs."),
            ("3 · Prove nothing broke", "Every saving ships with the reconciliation and freshness checks green: cheaper, never less correct.")]) + '</div>'))
    s.append(slide(9, "07 · The bridge to Data Science", "Calibrate on the lake, as of the decision", "Calibration needs the score at decision time, what happened next, and every feature as it was known then.",
        '<div class="grid two"><div>' + table([(b["band"], b["loans"], pct(b["predicted"]), pct(b["observed"])) for b in bands],
                                               ["Score band", "Loans", "Predicted", "Observed"])
        + f'<p class="src">gold.ds_calibration_by_band: the scorecard is too optimistic above 10%, and that gap is what recalibration fixes.</p></div>'
        f'<div><div class="card"><h3>Point in time, or a leak</h3><p>Open Banking enrichment relabels salary credits days after the fetch. A feature built from today\'s labels differs for <b>{pit["differ"]} of {pit["applications"]}</b> decisions, adding <b>£{pit["mean_naive"] - pit["mean_pit"]:,.0f}</b> of income on average that the model never saw.</p></div>'
        '<div class="callout" style="margin-top:4mm"><b>At Lifepal:</b> feature pipelines behind the customer-risk and claims-fraud models: point-in-time training sets, training/serving parity, drift monitoring and recalibration.</div></div></div>'))
    s.append(code_slide("07 · Data Science · Spark SQL", "A feature as it was known at decision time",
        "Open Banking enrichment changes rows after the fetch. The change history in silver lets a feature see exactly what the model saw.",
        "src/nexus/gold.py", "Spark SQL · Iceberg",
        code("src/nexus/gold.py", "        pit AS (", "        SELECT a.application_id, a.customer_id"),
        side(("The trap", ["A join on today's rows uses today's categories. Salary credits relabelled after the decision inflate income the model never saw, so offline results look better than production."]),
             ("In the run", [f"The naive feature differs for <b>{pit['differ']} of {pit['applications']}</b> decisions since 1 July, by <b>£{pit['mean_naive'] - pit['mean_pit']:,.0f}</b> of income on average."]),
             ("At Lifepal", ["Feature pipelines behind the customer-risk and claims-fraud models (LightGBM, XGBoost): point-in-time training sets, training/serving parity, drift monitoring and recalibration."]))))
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
    s.append(code_slide("09 · On AWS · Glue 5.0", "The same code, as a Glue job",
        "The only thing the job adds is the catalog: everything else is the Python that runs locally and in CI.",
        "glue/cdc_job.py", "AWS Glue 5.0 · Spark 3.5 · Iceberg 1.7",
        code("glue/cdc_job.py", "spark = (SparkSession", "job.commit()", limit=30),
        side(("DMS settings it expects", ["CSV with a header, <b>TimestampColumnName=cdc_ts</b>, Op on the full load too, and <b>$AR_H_CHANGE_SEQ</b> added as change_seq by a table-mapping rule (infra/dms/)."]),
             ("Around it", ["An hourly Glue trigger. A Lambda posts failing checks to on-call; reloads and backfills run as one-off ECS tasks on Fargate. Terraform for all of it, least-privilege IAM, KMS on the lake."]),
             ("Stated plainly", ["Written to be read and reviewed; not applied, since there is no AWS account behind a portfolio. The same functions ran for every number in this pack."]))))
    s.append(slide(12, "10 · My first 90 days", "Moving workloads into the Data Lake", "The role's first 6–12 months, as I'd start them.",
        '<div class="grid four">' + "".join(f'<div class="card"><h3>{a}</h3><p>{b}</p></div>' for a, b in [
            ("Days 1–30 · Listen and ship", "Learn the lake, the DMS tasks and the models from their owners. A small fix in production early."),
            ("Days 31–60 · Prove it", "Row-by-row reconciliation from Aurora for the tables Data Science uses; a change-volume alert per table."),
            ("Days 61–90 · Move one workload", "Model calibration on the lake, point in time, with Data Science; the RDS query switched off."),
            ("Day 90 · Measured", "Fresher data, a smaller Athena bill per query, and one fewer heavy read on Aurora.")]) + '</div>'
        f'<div class="proof" style="margin-top:8mm"><span class="kicker">Proven at Lifepal</span>{PROOF}</div>'
        f'<p class="src" style="margin-top:6mm">Everything in this pack runs at <b>{SITE}</b>. Synthetic data; an independent application, not affiliated with or endorsed by Abound.</p>'))
    s.append(slide(0, "Summary", "Summary", "Thank you for reading. Every Lifepal figure is from my CV and production work, and every run figure from the code; I'm happy to go as deep as you like on any of them.",
        '<div class="grid" style="grid-template-columns:repeat(5,1fr)">' + "".join(f'<div class="card" style="border-top:4px solid var(--{c})"><h3>{a}</h3><p>{b}</p></div>' for a, b, c in [
            ("CDC you can trust", f"DMS changes keyed on change_seq; a full replay changed {replay_new} rows; tombstones that stay deleted.", "violet"),
            ("Proven against Aurora", f"Row-by-row reconciliation caught {inc['rows_affected']} rows DMS never sent; a reload fixed them.", "pink"),
            ("Built for Data Science", "Calibration by score band and features as of the decision, from the change history.", "violet"),
            ("Cheaper by design", f"Partitioned, sorted, compacted: {less(q1)} less scanned. At Lifepal, $11k against $19k.", "pink"),
            ("Governed end to end", "Contracts, quality rules, erasure across bronze, silver and every snapshot.", "violet")]) + '</div>'
        '<div class="grid two" style="margin-top:5mm"><div class="card"><h3>Production experience and working knowledge</h3><p>In production: Python, SQL, PySpark, AWS (S3, DMS, Glue, Lambda, ECS), Aurora / RDS PostgreSQL and MySQL, Kafka + Debezium CDC, Delta Lake on Databricks, PyTest and GitLab CI, Terraform, Docker. dbt and Snowflake at Deloitte.</p>'
        '<p style="margin-top:5px">Working level: Athena. This repository is my Iceberg work: MERGE, hidden partitioning, sort orders, compaction and snapshot expiry on Spark 3.5, as Glue 5.0 runs it.</p></div>'
        '<div class="card" style="background:var(--ink);color:#fff;border:none"><h3 style="color:#fff">In one line</h3><p style="color:#e3f3ee;font-size:10.5pt">I get every change out of a transactional database reliably, prove the lake matches it, and lay it out so Data Science and the business can read it fast and cheaply. I\'ve done it in regulated financial services, and I\'d like to do it on Abound\'s lake.</p>'
        '<div class="built" style="margin-top:8px"><span class="lp"><img src="lifepal.png" alt="Lifepal"></span><span class="arrow">→</span><span class="codat" style="font-size:15pt;color:#fff">Abound</span></div></div></div>'
        f'<p class="src" style="margin-top:4mm">R. Firman Insan Muhammad (Vim) · Data Engineer · London · firman.insan.m@gmail.com · +44 747 061 4781 · right to work in the UK, no sponsorship required · code and live site: github.com/rfim/nexus-abound</p>'))
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
    lane = lambda cls, steps: f'<div class="lane {cls}">' + "".join(f"<div><b>{a}</b>{b}</div>" for a, b in steps) + "</div>"   # noqa: E731
    chip = lambda xs, dark=False: "".join(f'<span class="chip{" dark" if dark else ""}">{x}</span>' for x in xs)   # noqa: E731
    return head("Nexus for Abound · Companion note", "companion.css") + f'''<div class="page">
<div class="kicker">Companion note · prepared for the Platform team · Data Engineer</div>
<div class="top"><div><h1>Different market, <b>same job.</b></h1>
  <div class="tl">Why Lifepal's regulated insurance marketplace maps onto Abound's consumer lending.</div>
  <p><b>In Lifepal, the other side was the insurers; at Abound, it's the loan itself.</b> Lifepal connected people with the insurers that cover them, and priced and checked risk on the way; Abound connects people's bank data, through Open Banking, to a lending decision it makes and funds. In both, a transactional database is the system of record, data changes after the fact, a customer can ask to be forgotten, and a real financial decision depends on the number being right. The data problems that follow are the ones NEXUS was built to solve.</p></div>
  <div>{TILES}<div class="built"><div><div class="k2">BUILT IN</div><span class="lp"><img src="lifepal.png" alt="Lifepal" style="height:30px"></span></div><span class="arrow">→</span><div><div class="k2">PRESENTED TO</div><span class="codat">Abound</span></div></div></div></div>

<div class="sec"><div class="sh"><b>One flow, two businesses</b><i>Abound side from public material</i></div>
{lane("", [("01 Apply", "Lifepal · a customer asks for a quote<br>Abound · a customer applies for a loan"),
           ("02 Share", "Lifepal · personal and policy data, with consent<br>Abound · Open Banking transactions, with consent"),
           ("03 Decide", "Lifepal · risk and fraud models, then the insurer<br>Abound · affordability and the scorecard"),
           ("04 Change", "Lifepal · claims, renewals, late statuses<br>Abound · repayments, arrears, re-categorised rows")])}
</div>

<div class="sec"><div class="sh"><b>The closest parallel</b><i>risk decisions on data that keeps changing</i></div>
<div class="b2b"><div>
  <div class="who" style="color:var(--violet)">Lifepal · person → insurer</div>
  {lane("", [("Customer", "Needs cover; applies once."), ("Data", "Policy, quote and claim rows in PostgreSQL and MySQL."),
             ("Model", "Customer-risk and claims-fraud models (LightGBM, XGBoost)."), ("Outcome", "Claims and renewals arrive for months.")])}
  <div class="who" style="color:var(--pink);margin-top:8px">Abound · person → loan</div>
  {lane("codat", [("Customer", "Applies for a loan; connects their bank."), ("Data", "Applications, loans, repayments in Aurora; Open Banking rows."),
                  ("Model", "A scorecard on transaction data, calibrated by band."), ("Outcome", "Repayments, misses and cures, every month.")])}
</div><div>''' + table([
        ("<b>Analytics off the production DB</b>", "Moved reporting, ad-hoc analytics and model training onto the S3 lake", "The role's first 6–12 months; gold tables for each workload"),
        ("<b>Changes twice, late, out of order</b>", "Merged on the source log position, idempotent under replay", f"change_seq ordering; a full replay changed {replay_new} rows"),
        ("<b>Features as they were known</b>", "Point-in-time training sets; training/serving parity", f"PIT income differs for {pit['differ']} of {pit['applications']} decisions"),
        ("<b>Risk and selection bias</b>", "Causal inference with data scientists, to separate real drivers from selection", "Calibration by band, on approved loans only, stated as such")],
        ["The same problem", "How I handled it at Lifepal", "Where it shows here"]) + f'''
  <p class="src" style="margin-top:5px">Described at the level I can share publicly; I'm happy to walk through the details in conversation.</p></div></div></div>

<div class="sec"><div class="sh"><b>What Abound runs</b><i>as the role lists it · data here is synthetic</i></div>
<div class="ints">
  <div class="card"><div class="kicker">Source and capture</div><p>{chip(["Aurora PostgreSQL", "Aurora MySQL", "AWS DMS", "CDC"])}</p><p style="margin-top:4px">Here: a simulated lender (customers, applications, loans, repayments, Open Banking rows) and the files DMS would write, with six planted problems.</p></div>
  <div class="card"><div class="kicker">Process and store</div><p>{chip(["Spark", "AWS Glue", "Iceberg", "S3"])}</p><p style="margin-top:4px">Here: PySpark on Iceberg 1.7, as Glue 5.0 runs it; bronze, silver with history, gold.</p></div>
  <div class="card"><div class="kicker">Serve and run</div><p>{chip(["Athena", "Lambda", "ECS", "Fargate", "Python"])}</p><p style="margin-top:4px">Here: layouts planned for Athena cost; Lambda and Fargate around the job in Terraform.</p></div>
  <div class="card"><div class="kicker" style="color:var(--violet2)">Scale</div><div class="big" style="color:#fff;font-size:20pt">£1.6bn</div><p>lending capacity after a £250m Deutsche Bank facility in March 2025, as reported.</p></div>
</div></div>

<div class="sec"><div class="sh"><b>Concept map</b><i>same grain, different names</i></div>''' + table([
        ("<b>Policy and quote rows</b>" + chip(["PostgreSQL", "MySQL"]), "<b>Applications, loans, repayments</b>" + chip(["Aurora"], True), '<span class="g">one row per source record</span>', "Get every change out, exactly once"),
        ("<b>Claim status updates</b>" + chip(["late, for months"]), "<b>Repayment outcomes</b>" + chip(["missed", "paid late"], True), '<span class="g">one row per change</span>', "Newest change wins; history kept"),
        ("<b>Risk at quote time</b>" + chip(["as-of features"]), "<b>Risk at decision time</b>" + chip(["as-of decided_at"], True), '<span class="g">one row per decision</span>', "No feature from after the decision"),
        ("<b>Risk and fraud models</b>" + chip(["LightGBM", "XGBoost"]), "<b>The scorecard</b>" + chip(["calibrated by band"], True), '<span class="g">score band × cohort</span>', "Predicted against observed"),
        ("<b>Reporting and partners</b>" + chip(["insurer packs"]), "<b>Reporting and funders</b>" + chip(["loan book", "facility tape"], True), '<span class="g">month end</span>', "One number for every audience"),
        ("<b>Erasure requests</b>" + chip(["GDPR"]), "<b>Erasure requests</b>" + chip(["GDPR"], True), '<span class="g">one person, every table</span>', "Deleted everywhere, including snapshots")],
        ["Lifepal · insurance marketplace", "Abound · consumer lending (my reading)", "Grain", "Shared data problem"]) + '''
<p class="src">Lifepal examples are from my own work. Abound examples come from the role, public information, and the synthetic data in this project.</p></div>

<div class="sec"><div class="sh"><b>What carries over directly</b><i>proven at Lifepal</i></div><div class="carry">''' + "".join(
        f'<div class="card"><h3>{a}</h3><p><b>Lifepal:</b> {b}</p><p style="margin-top:4px"><b>Here:</b> {c}</p></div>' for a, b, c in [
            ("CDC into the lake", "DMS and Debezium from PostgreSQL and MySQL; reporting latency 24 h → under 5 min.", f"DMS files keyed on change_seq; {replay_new} rows changed by a full replay."),
            ("Off the production DB", "Reporting, analytics and training moved onto the lake; ad-hoc requests −60%.", "Gold tables for the loan book, calibration and features."),
            ("Layout and cost", "Nightly batch 6 h → 90 min; $11k a month against a $19k projection.", f"{less(q1)} less scanned per customer read on Iceberg."),
            ("Data Science partner", "Point-in-time training sets, parity, drift and recalibration.", "Calibration by score band; PIT income feature."),
            ("Governance", "500+ checks in CI, PII masking, RBAC, lineage; escalations −60%.", f"Reconciliation caught {inc['rows_affected']} rows; erasure across snapshots.")]) + f'''</div></div>

<div class="sec cols"><div><div class="sh"><b>Stated plainly</b><i>what's real</i></div>
<div class="plain"><b>Real:</b> the Lifepal outcomes, from my CV and production work, and all the code in the repository, which runs end to end in CI.<br><br>
<b>Synthetic:</b> every customer, loan, repayment and transaction. The Abound side comes from the job description and public information; no Abound systems or data were used.<br><br>
<b>Depth:</b> DMS, Spark, Glue, Aurora/RDS PostgreSQL and MySQL, Lambda and ECS are production experience. Delta is my production table format; this repository is my Iceberg work. Athena is working level.</div></div>
<div><div class="sh"><b>Questions I'd bring</b><i>for the Platform team</i></div><div class="plain"><b>1.</b> Which workloads still read Aurora directly, and which would Data Science move first?<br><b>2.</b> How are DMS gaps and restarts detected today: heartbeat, volume, or reconciliation?<br><b>3.</b> Where do model calibration and its features live now, and how fresh must they be?<br><b>4.</b> Which AWS line items are growing fastest: Glue, Athena, S3 or DMS?<br><b>5.</b> How do erasure requests reach the lake today?</div></div></div>
<p class="src" style="margin-top:6mm">{SITE} · an independent application, not affiliated with or endorsed by Abound.</p>
</div></body></html>'''


for name, fn in (("deck.html", deck), ("one_page.html", one_page), ("companion.html", companion)):
    (HERE / name).write_text(fn())
print("wrote deck.html, one_page.html, companion.html")
