// The run & replayed: the recorded pipeline run as a graph, each step lighting up in order with its real
// duration, then its checks, the incident and the fix. Step by step as on NEXUS for Tripadvisor: each step's log,
// its terminal and the code that decided, read from the repository. Data: assets/data/site.json.
(async () => {
  const $ = (id) => document.getElementById(id);
  const d = await NX.site();
  const T = Object.fromEntries(d.tasks.map((t) => [t.key, t]));
  const secs = (ks) => ks.reduce((a, k) => a + (T[k]?.duration_s || 0), 0);
  const sum = (o, f) => Object.values(o).reduce((a, v) => a + f(v), 0);
  const first = d.checks_first, final = d.checks, inc = d.incident;
  const batches = d.batches.slice(1);
  const NODES = [
    { k: "dms_files", tasks: ["generate"], group: "capture", kinds: ["dms", "python"],
      meta: { files: d.generator.cdc_files + Object.keys(d.generator.full_load).length, changes: NX.int(d.generator.source_changes) } },
    { k: "full_load", tasks: ["batch0"], group: "land", kinds: ["spark", "iceberg"],
      meta: { rows: NX.int(sum(d.batches[0].tables, (t) => t.new_changes)), tables: Object.keys(d.batches[0].tables).length } },
    { k: "cdc_batches", tasks: batches.map((b) => `batch${b.batch}`), group: "land", kinds: ["glue", "spark", "iceberg"],
      meta: { batches: batches.length, new: NX.int(batches.reduce((a, b) => a + sum(b.tables, (t) => t.new_changes), 0)),
              duplicates: batches.reduce((a, b) => a + sum(b.tables, (t) => t.duplicates), 0), "columns added": "apr_bps" } },
    { k: "replay", tasks: ["replay"], group: "prove", kinds: ["spark", "iceberg"],
      meta: { "re-read": NX.int(sum(d.replay, (v) => v.rows_read)), "rows changed": sum(d.replay, (v) => v.new_changes) } },
    { k: "checks", tasks: ["check"], group: "prove", kinds: ["spark", "python"], checks: first },
    { k: "reload", tasks: ["investigate", "reload"], group: "fix", kinds: ["dms", "spark", "iceberg"],
      meta: { "rows fixed": inc?.rows_affected, file: d.reload?.file.split("/").pop() } },
    { k: "erasure", tasks: ["erase"], group: "fix", kinds: ["iceberg"],
      meta: { customer: d.erasure.customer, "tables expired": d.erasure.expired_tables.length } },
    { k: "recheck", tasks: ["recheck"], group: "prove", kinds: ["spark", "python"], checks: final },
    { k: "layout", tasks: ["history", "naive", "layout"], group: "serve", kinds: ["iceberg", "athena"],
      meta: { compaction: `${d.layout.compaction.rewritten_files} → ${d.layout.compaction.added_files} files` } },
    { k: "gold", tasks: ["gold"], group: "serve", kinds: ["spark", "iceberg", "athena"],
      meta: Object.fromEntries(Object.entries(d.gold).map(([k, v]) => [k, v])) },
  ];
  const pass = (cs) => cs.filter((c) => c.status === "pass").length;
  $("o-tiles").innerHTML = [
    [NODES.length, "steps in the run", `${d.tasks.length} tasks · ${d.seconds}s end to end`],
    [`${pass(final)}/${final.length}`, "checks pass after the fix", `${first.length - pass(first)} failed on the first pass`],
    [NX.int(sum(d.replay, (v) => v.new_changes)), "rows changed by a full replay", `${NX.int(sum(d.replay, (v) => v.rows_read))} changes re-read`],
    [inc ? inc.rows_affected : 0, "rows the reconciliation caught", "the DMS gap, fixed by a reload"],
  ].map(([n, l, s], i) => `<div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="big ${i % 2 ? "p" : ""}">${n}</div><h3 style="margin:.4rem 0 .2rem">${l}</h3><p>${NX.esc(s)}</p></div>`).join("");
  $("o-sched").textContent = `on AWS: Glue trigger cron(10 * * * ? *) · recorded run ${d.seconds}s`;

  // ---------- the graph ----------
  const W = 108, gap = 10.2, pos = {};
  NODES.forEach((n, i) => (pos[n.k] = { x: 14 + i * (W + gap), y: i % 2 ? 150 : 110 }));
  const kindColor = { dms: "#f0a23a", glue: "#ffd27a", spark: "#ff9a6a", iceberg: "#7ce0bf", athena: "#8fc2ff", python: "#b9e6d8" };
  const node = (n) => {
    const p = pos[n.k], cs = n.checks || [], ok = pass(cs), bad = cs.length - ok;
    return `<g class="node off" id="a-${n.k}" transform="translate(${p.x},${p.y})"><rect width="${W}" height="78" rx="12"/>
      <text x="${W / 2}" y="22" text-anchor="middle" style="font-size:12.5px">${n.k}</text>
      <text class="sm" x="${W / 2}" y="40" text-anchor="middle">${secs(n.tasks).toFixed(1)}s · ${n.group}</text>
      ${n.kinds.slice(0, 3).map((kd, j) => `<use href="#ti-${kd}" x="${W / 2 - 26 + j * 19}" y="47" width="15" height="15"><title>${kd}</title></use>`).join("")}
      ${cs.length ? `<text x="${W / 2}" y="72" text-anchor="middle" style="font:700 10px var(--mono);fill:${bad ? "#ff9a7a" : "#7ce0bf"}">${ok}/${cs.length} checks</text>` : ""}</g>`;
  };
  const edges = NODES.slice(1).map((n, i) => {
    const p = pos[NODES[i].k], q = pos[n.k];
    return `<path class="wire" id="e-${n.k}" d="M${p.x + W} ${p.y + 39} C ${p.x + W + 6} ${p.y + 39}, ${q.x - 6} ${q.y + 39}, ${q.x} ${q.y + 39}"/>`;
  }).join("");
  const legend = Object.entries(kindColor).map(([k, c], i) => `<g transform="translate(${20 + i * 130},300)"><circle r="5" cx="6" cy="0" fill="${c}"/><text x="18" y="4" style="font:500 11px var(--font);fill:#9cc0b7">${k}</text></g>`).join("");
  $("dag").innerHTML = `<text x="14" y="40" style="font:700 11px var(--mono);fill:#8fb3aa;letter-spacing:.14em">CAPTURE → LAND → PROVE → FIX → PROVE AGAIN → SERVE</text>` + edges + NODES.map(node).join("") + legend;

  const say = (n) => {
    const bits = Object.entries(n.meta || {}).filter(([, v]) => v != null).slice(0, 4).map(([a, b]) => `${a}: <b>${NX.esc(b)}</b>`);
    const failed = (n.checks || []).filter((c) => c.status !== "pass").map((c) => `${c.table} ${c.check}`);
    return `<b>${n.k}</b> · ${secs(n.tasks).toFixed(1)}s${bits.length ? " · " + bits.join(" · ") : ""}` +
      (failed.length ? ` · <span style="color:#ff9a7a">✕ ${NX.esc(failed.join(", "))}</span>` : n.checks ? ` · <span style="color:#7ce0bf">✓ ${n.checks.length} checks</span>` : "");
  };
  // ---------- step by step: each step's log, its terminal, and the code that decided ----------
  const fmtT = (o) => Object.entries(o).map(([t, v]) => `${t.padEnd(16)} ${String(v.files).padStart(4)} files ${String(v.new_changes).padStart(7)} new ${String(v.duplicates).padStart(4)} dup` +
    (v.columns_added.length ? `  + ADD COLUMNS ${v.columns_added.join(", ")}` : ""));
  const fails = first.filter((c) => c.status !== "pass");
  const g = d.generator;
  const STEP = {
    dms_files: { t: "Simulate Aurora and DMS", d: "A year of a small lender, and the files DMS writes to S3.", py: ["src/nexus/generate.py", "def write_dms("],
      log: [`${NX.int(g.source_changes)} changes committed in Aurora`, `${Object.keys(g.full_load).length} full-load files + ${g.cdc_files} CDC files in landing/dms/lending/`,
            `planted: restart re-send (${g.planted.replayed_changes}), late file, erasure, ALTER TABLE, same-txn (${g.planted.same_txn_cures}), gap (${g.planted.gap_changes})`] },
    full_load: { t: "Land the full load", d: "Bronze keeps every change, keyed on (key, change_seq).", py: ["src/nexus/bronze.py", "def ingest("],
      log: [`listing files delivered by ${d.batches[0].until}`, ...fmtT(d.batches[0].tables)] },
    cdc_batches: { t: "Apply five CDC batches", d: "By arrival time; silver keeps the newest change_seq; deletes are tombstones.", py: ["src/nexus/silver.py", "def apply("],
      log: batches.flatMap((b) => [`batch ${b.batch} · delivered by ${b.until.slice(0, 10)}`, ...fmtT(b.tables).map((l) => "  " + l)]) },
    replay: { t: "Replay every file", d: "Re-ingest all DMS output as if the file log were lost. Nothing may change.", py: ["src/nexus/pipeline.py", "def _replay("],
      log: [`re-read ${NX.int(sum(d.replay, (v) => v.rows_read))} changes`, `new rows in bronze: ${sum(d.replay, (v) => v.new_changes)}`, `silver rows changed: ${sum(d.replay, (v) => v.silver.live_delta)}`] },
    checks: { t: "Prove it against Aurora", d: "Reconcile row by row, quality rules, freshness, erasure.", py: ["src/nexus/checks.py", "def reconcile("],
      log: [`✓ ${pass(first)} checks pass`, ...fails.map((c) => `✗ ${c.table}: ${c.check}` + (c.different != null ? ` (${c.missing_in_lake} missing, ${c.different} different)` : c.snapshots_with_pii != null ? ` (${c.snapshots_with_pii}/${c.snapshots} snapshots hold PII)` : ""))] },
    reload: { t: "Investigate, then reload", d: "Find the missing hour, reload the table, apply it as authoritative.", py: ["src/nexus/pipeline.py", "def investigate("],
      log: inc ? [`${inc.rows_affected} keys differ; the source changed them at ${inc.source_changed_from}`, `bronze changes in that hour: ${inc.bronze_changes_in_that_hour_that_day} (median ${inc.bronze_changes_in_that_hour_other_days_median} on other days)`,
                  `→ ${inc.diagnosis}`, `reload ${d.reload.file.split("/").pop()}: ${NX.int(d.reload.rows_read)} rows applied as authoritative`] : [] },
    erasure: { t: "Right to erasure", d: "Purge the customer from bronze, expire old snapshots.", py: ["src/nexus/pipeline.py", "def erase("],
      log: [`customer ${d.erasure.customer}`, ...Object.entries(d.erasure.bronze_rows_deleted).map(([k, v]) => `  bronze.${k}_changes: ${v} rows deleted`), `expire_snapshots on ${d.erasure.expired_tables.length} tables`] },
    recheck: { t: "Prove it again", d: "Every check, after the fixes.", py: ["src/nexus/pipeline.py", "def _check_all("],
      log: [`✓ ${pass(final)}/${final.length} checks pass`, ...final.filter((c) => c.check.startsWith("reconcile")).map((c) => `  ✓ ${c.table.padEnd(16)} ${c.match}/${c.rows_source} rows match Aurora`)] },
    layout: { t: "Lay it out for Athena", d: "Partition by month, sort by customer, compact.", py: ["src/nexus/layout.py", "def compact("],
      log: [`rewrite_data_files: ${d.layout.compaction.rewritten_files} files → ${d.layout.compaction.added_files} sorted files`,
            ...Object.values(d.layout.queries).map((q) => `${q.label}: ${Math.round(q.naive.bytes_read / 1024)} KB → ${Math.round(q.compacted.bytes_read / 1024)} KB read`)] },
    gold: { t: "Build gold for the business", d: "Loan book, calibration by score band, point-in-time features.", py: ["src/nexus/gold.py", "def build_calibration("],
      log: Object.entries(d.gold).map(([k, v]) => `gold.${k}: ${NX.int(v)} rows`) },
  };
  // ---------- each step's own question: how it was chosen (assets/choice.js) ----------
  const ql = d.layout.queries.one_customer_90d, P = g.planted, pitR = d.gold_rows.features_pit;
  const CH = {
    dms_files: { q: "how to get every change out of Aurora?", ask: "Why AWS DMS, and not a nightly export of each table?",
      crit: ["No query load on Aurora", "Every change, in order", "Managed on AWS"],
      o: [["Nightly SELECT * export", [0, 0, 1]], ["Debezium on Kafka Connect", [1, 1, 0]], ["AWS DMS · full load + CDC", [1, 1, 1], true]],
      why: "DMS reads the write-ahead log or binlog, so Aurora runs no extra queries, and AR_H_CHANGE_SEQ orders every change, even two in one transaction. A nightly export misses every change between runs. Debezium does the same job but needs Kafka to operate; at Lifepal I ran both." },
    full_load: { q: "what identifies one change in bronze?", ask: "Why key bronze on change_seq, and not on the file or the commit time?", tag: "Counted on this run",
      unit: "changes bronze would get wrong in this run", o: [["Key = the file it came in", P.replayed_changes, false, "re-sent changes come in a new file"],
        ["Key = primary key + cdc_ts", P.same_txn_cures, false, "two statements, one commit time: one is lost"],
        ["Key = primary key + change_seq", 0, true, "unique and increasing"]],
      why: `DMS re-sent ${P.replayed_changes} changes in a new file after a restart, and ${P.same_txn_cures} payments were two statements with the same commit time. Only AR_H_CHANGE_SEQ tells every change apart.` },
    cdc_batches: { q: "how should silver apply a delete?", ask: "Why keep a tombstone, and not just delete the row?",
      crit: ["Erased data is gone", "A late update can't bring it back", "Simple"],
      o: [["DELETE the row", [1, 0, 1]], ["Soft-delete flag, values kept", [0, 1, 1]], ["Tombstone: keep change_seq, null the values", [1, 1, .5], true]],
      why: `A late update for ${P.erased_customer} arrived after the delete. With a hard DELETE it would have been inserted again; the tombstone's change_seq is newer, so it was ignored: ${d.resurrection.live_rows_after_late_file} live rows.` },
    replay: { q: "is it safe to run a load twice?", ask: "Why MERGE into bronze, and not just append each file?", tag: "Measured on this run",
      unit: "rows added by replaying every DMS file", o: [["Append each file", sum(d.replay, (v) => v.rows_read), false], ["MERGE on (key, change_seq)", sum(d.replay, (v) => v.new_changes), true]],
      why: "Any recovery is a re-run: after a failed job, a lost file log, or a backfill. With MERGE on the change's identity, a re-run changes nothing, so nobody needs to fix a table by hand." },
    checks: { q: "how to find changes that never arrived?", ask: "Why reconcile row by row, when row counts are cheaper?",
      crit: ["Catches a stale row", "Cheap", "No load on the Aurora writer"],
      o: [["Row counts per table", [0, 1, 1]], ["Key list per table", [0, .5, 1]], ["Every column, against a read replica", [1, .5, 1], true]],
      why: inc ? `In this run the counts matched: ${first.find((c) => c.check.startsWith("reconcile") && c.status === "fail").missing_in_lake} rows missing. But ${inc.rows_affected} repayments were stale, because DMS never sent their payment. Only comparing every column found them.` : "" },
    reload: { q: "how to fix the missing hour?", ask: "Why reload the whole table, and not patch the rows or rewind DMS?",
      crit: ["Fixes every wrong row", "No guessing log positions", "Also removes rows the source deleted"],
      o: [["Restart DMS from an earlier position", [.5, 0, 0]], ["Patch the wrong rows by hand", [.5, 1, 0]], ["Reload the table, apply as authoritative", [1, 1, 1], true]],
      why: `The reload is a fresh full load of repayments (${NX.int(d.reload.rows_read)} rows). Applied with WHEN NOT MATCHED BY SOURCE, it fixes the ${inc ? inc.rows_affected : ""} stale rows and anything else DMS missed, with no positions to guess.` },
    erasure: { q: "how to make erasure real?", ask: "Why purge bronze and expire snapshots, when silver already has a tombstone?",
      crit: ["Gone from current tables", "Gone from time travel", "Audit trail kept"],
      o: [["Tombstone in silver only", [1, 0, 1]], ["Drop and rebuild the tables", [1, 1, 0]], ["Tombstone + purge bronze + expire snapshots", [1, 1, 1], true]],
      why: `Iceberg keeps old snapshots, so the email was still readable with time travel: the first check found it in ${first.find((c) => c.check.startsWith("erased")).snapshots_with_pii} snapshots. After the purge and expiry, 0.` },
    recheck: { q: "did the fixes work?", ask: "Why run every check again, and not just the one that failed?", tag: "Measured on this run",
      unit: "checks passing", low: false, o: [["First pass", pass(first), false, `of ${first.length}`], ["After reload and erasure", pass(final), true, `of ${final.length}`]],
      why: "A reload rewrites a whole table and an erasure deletes from several, so either could break a rule that passed before. Running everything again is cheap and closes the incident on evidence." },
    layout: { q: "how to lay out Open Banking transactions for Athena?", ask: "Why compact and sort by customer, when MERGE already partitions by month?",
      unit: `bytes Athena reads for "${ql.label}"`, o: [["Unpartitioned · one file a day", ql.naive.bytes_read, false, `${ql.naive.files_read} of ${ql.naive.files_total} files`],
        ["months(booked_at) · straight from MERGE", ql.merged.bytes_read, false, `${ql.merged.files_read} of ${ql.merged.files_total} files`],
        ["months · sorted by customer · compacted", ql.compacted.bytes_read, true, `${ql.compacted.files_read} of ${ql.compacted.files_total} files`]],
      why: `Same answer on all three. Athena bills bytes scanned, so ${Math.round(ql.naive.bytes_read / ql.compacted.bytes_read)}× less read is ${Math.round(ql.naive.bytes_read / ql.compacted.bytes_read)}× cheaper for Data Science's most common query. Measured on this run's files.` },
    gold: { q: "where should Data Science read calibration data?", ask: "Why build gold tables, and not let Data Science query silver or Aurora?",
      crit: ["No load on Aurora", "As of the decision", "Cheap to query"],
      o: [["Query Aurora directly", [0, 0, .5]], ["Silver tables as they are", [1, 0, .5]], ["Gold: calibration set + point-in-time features", [1, 1, 1], true]],
      why: `Silver holds today's values. Built from today's Open Banking labels, the income feature differs for ${pitR.differ} of ${pitR.applications} decisions; gold joins the history as of each decision, so the model trains on what it actually saw.` },
  };

  const steps = NODES.map((n) => ({ n, ...STEP[n.k] }));
  const cls = (l) => /^✗/.test(l) ? "err" : /^✓|^→/.test(l) ? "res" : "req";
  const SRC = {};
  const src = (f) => (SRC[f] ??= fetch(f).then((r) => r.ok ? r.text() : ""));
  let i = -1, timer = null, pane = "term";

  const paintTerm = async () => {
    const box = $("term-out"), st = steps[i];
    document.querySelectorAll("#term-tabs button").forEach((b) => b.classList.toggle("on", b.dataset.p === pane));
    if (!st) { box.innerHTML = `<span class="t-dim">Step through the run: each step's commands and the code that decided appear here.</span>`; return; }
    if (pane === "term") {
      box.innerHTML = [`<span class="t-cmd">$ PYTHONPATH=src python -m nexus.pipeline</span>`, `<span class="t-dim"># task ${st.n.tasks.join(", ")} · ${secs(st.n.tasks).toFixed(1)}s in the recorded run</span>`,
        ...st.log.map((l) => `<span class="${/^✗/.test(l) ? "t-err" : /^✓/.test(l) ? "t-ok" : ""}">${NX.esc(l)}</span>`)].join("\n");
    } else {
      const [file, mark] = st.py, lines = (await src(file)).split("\n"), at = Math.max(0, lines.findIndex((l) => l.includes(mark)));
      const from = Math.max(0, at - 2), to = Math.min(lines.length, at + 22);
      box.innerHTML = `<span class="t-dim"># ${file}, lines ${from + 1}–${to}</span>\n` + lines.slice(from, to).map((l, k) =>
        `<span class="${from + k === at ? "t-hit" : ""}"><i>${String(from + k + 1).padStart(4)}</i>  ${NX.esc(l)}</span>`).join("\n");
    }
  };
  const render = () => {
    steps.forEach((st, k) => {
      const node = $("a-" + st.n.k), e = $("e-" + st.n.k), bad = (st.n.checks || []).some((c) => c.status !== "pass");
      node.setAttribute("class", k < i ? (bad ? "node hot" : "node on") : k === i ? "node hot" : "node off");
      if (e) e.setAttribute("class", k < i ? "wire on" : k === i ? "wire hot" : "wire");
    });
    $("steps").innerHTML = steps.map((st, k) => `<li data-i="${k}" class="${k < i ? "done" : k === i ? "now" : ""}"><span class="dot">${k < i ? "✓" : k + 1}</span>
      <span><b>${NX.esc(st.t)}</b>${NX.esc(st.d)}</span></li>`).join("");
    const now = $("steps").querySelector(".now"); if (now) $("steps").scrollTop = now.offsetTop - $("steps").offsetTop - 60;
    $("o-log").innerHTML = steps.slice(0, i + 1).map((st) => `<div class="msg ${st.log.some((l) => /^✗/.test(l)) ? "err" : "res"}"><span class="dir">${NX.esc(st.n.k)} · ${secs(st.n.tasks).toFixed(1)}s</span>${NX.esc(st.log.slice(0, 8).join("\n"))}${st.log.length > 8 ? "\n…" : ""}</div>`).join("");
    $("o-log").scrollTop = 1e6;
    $("o-cap").innerHTML = i < 0 ? `Press <b>▶ Play</b>, or <b>Next →</b> to step through the run one step at a time.`
      : i === steps.length - 1 && !timer ? `The run finished in ${d.seconds}s: ${pass(final)}/${final.length} checks pass. The first pass failed on the planted DMS gap and the erasure; <b>reload</b> and <b>erasure</b> fixed both, and <b>recheck</b> proved it.`
      : `<b>${i + 1} · ${NX.esc(steps[i].t)}</b> · ${say(steps[i].n).replace(/^<b>[^<]*<\/b> · /, "")}`;
    $("prev").disabled = i < 0; $("next").disabled = i >= steps.length - 1;
    NXChoice.render($("choice"), CH[steps[Math.max(0, i)].n.k]);
    paintTerm();
  };
  const stop = () => { clearInterval(timer); timer = null; $("o-play").textContent = i >= steps.length - 1 ? "↻ Replay" : "▶ Play"; render(); };
  const go = (k) => { i = Math.max(-1, Math.min(steps.length - 1, k)); render(); };
  $("next").addEventListener("click", () => { if (timer) stop(); go(i + 1); });
  $("prev").addEventListener("click", () => { if (timer) stop(); go(i - 1); });
  $("steps").addEventListener("click", (e) => { const li = e.target.closest("li"); if (!li) return; if (timer) stop(); go(+li.dataset.i); });
  $("term-tabs").addEventListener("click", (e) => { const b = e.target.closest("button"); if (!b) return; pane = b.dataset.p; paintTerm(); });
  $("o-play").addEventListener("click", () => {
    if (timer) return stop();
    if (i >= steps.length - 1) i = -1;
    $("o-play").textContent = "❚❚ Pause";
    const tick = () => { if (i >= steps.length - 1) return stop(); go(i + 1); };
    tick(); timer = setInterval(tick, +$("speed").value);
  });
  $("speed").addEventListener("change", () => { if (timer) { clearInterval(timer); timer = setInterval(() => (i >= steps.length - 1 ? stop() : go(i + 1)), +$("speed").value); } });
  render();

  // ---------- checks ----------
  const firstBy = Object.fromEntries(first.map((c) => [`${c.table}|${c.check}`, c.status]));
  $("o-checks").innerHTML = NX.table(final.map((c) => ({ table: c.table, check: c.check, "first pass": firstBy[`${c.table}|${c.check}`], "after the fix": c.status,
    detail: c.check.startsWith("reconcile") ? `${c.match}/${c.rows_source} rows match${c.sum_of ? ` · sum(${c.sum_of}) ${NX.money2(c.sum_lake)}` : ""}`
      : c.rule ? c.rule : c.lag_hours != null ? `newest change ${c.lag_hours} h old` : c.snapshots != null ? `${c.snapshots_with_pii}/${c.snapshots} snapshots hold PII` : "" })), null,
    { "first pass": (v) => `<span class="${v}">${v}</span>`, "after the fix": (v) => `<span class="${v}">${v}</span>`, detail: (v) => NX.esc(String(v).slice(0, 90)) });

  // ---------- the incident ----------
  const recon = first.find((c) => c.check.startsWith("reconcile") && c.status === "fail");
  $("inc-report").innerHTML = inc ? `<div class="kicker" style="color:var(--pink)">found by the run · fixed by the run</div>
    <h3 style="margin:.3rem 0 .6rem">${inc.rows_affected} ${inc.table} rows don't match Aurora</h3>
    <p><b>Root cause.</b> ${NX.esc(inc.diagnosis)}</p>
    <p style="margin-top:.6rem"><b>Evidence</b></p><ul style="padding-left:1.1rem">
      <li>Reconciliation: ${recon.missing_in_lake} missing and ${recon.different} different rows <code style="color:var(--muted)">checks.reconcile</code></li>
      <li>Source changed them between ${NX.esc(inc.source_changed_from)} and ${NX.esc(inc.source_changed_to.slice(0, 19))} <code style="color:var(--muted)">source_snapshot</code></li>
      <li>Bronze has ${inc.bronze_changes_in_that_hour_that_day} changes in that hour, against a median of ${inc.bronze_changes_in_that_hour_other_days_median} <code style="color:var(--muted)">bronze.repayments_changes</code></li></ul>
    <p style="margin-top:.6rem"><b>Fix.</b> ${NX.esc(inc.remedy)}</p>
    <p style="font-size:.85rem;color:var(--muted)">Reload: <code>${NX.esc(d.reload.file)}</code> · ${NX.int(d.reload.rows_read)} rows applied as authoritative · ${inc.rows_affected} rows fixed</p>` : "";
  $("inc-keys").textContent = inc ? inc.keys.join("\n") : "";
})().catch((e) => console.error(e));
