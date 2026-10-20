// Home page: every figure comes from assets/data/site.json, written by the last run.
const kb = (b) => b >= 1024 * 1024 ? `${(b / 1024 / 1024).toFixed(1)} MB` : `${Math.round(b / 1024)} KB`;
const pctLess = (a, b) => `${Math.round((1 - b / a) * 100)}% less`;

NX.site().then((d) => {
  const g = d.generator, p = g.planted;
  const sumBy = (o, f) => Object.values(o).reduce((a, v) => a + f(v), 0);
  const replayNew = sumBy(d.replay, (v) => v.new_changes);
  const final = d.checks, first = d.checks_first;
  const passed = final.filter((c) => c.status === "pass").length;
  document.querySelectorAll("[data-k=replay_new]").forEach((e) => (e.textContent = NX.int(replayNew)));
  document.querySelectorAll("[data-k=checks]").forEach((e) => (e.textContent = `${passed}/${final.length}`));

  // 02 · six problems
  const restartBatch = d.batches.find((b) => Object.values(b.tables).some((t) => t.duplicates > 0));
  const dups = restartBatch ? sumBy(restartBatch.tables, (t) => t.duplicates) : 0;
  const alter = d.batches.find((b) => b.tables.loans.columns_added.length);
  const recon = first.find((c) => c.check.startsWith("reconcile") && c.status === "fail");
  const fixed = final.find((c) => c.table === recon?.table && c.check.startsWith("reconcile"));
  const problems = [
    ["Re-sent after a task restart", `DMS restarted at ${p.restart_at.slice(0, 16)} and re-sent ${p.replayed_changes} changes in a new file.`,
     `${NX.int(dups)} skipped as duplicates on (key, change_seq). Replaying every file DMS ever wrote added ${NX.int(replayNew)} rows.`],
    ["A file delivered late", `The ${p.late_file.table} file for ${p.late_file.day} landed on ${p.late_file.arrived.slice(0, 10)}, after later files.`,
     "Files are listed by arrival time, not by their path, so the next batch picked it up. change_seq kept the order right."],
    ["A delete, then an older update", `${p.erased_customer} was erased on ${p.erased_at.slice(0, 10)}. Their earlier update arrived afterwards, in that late file.`,
     `A tombstone stopped the old update from bringing the row back: ${d.resurrection.live_rows_after_late_file} live rows after the late file.`],
    ["A column added mid-stream", `ALTER TABLE loans ADD COLUMN apr_bps on ${p.schema_change.at.slice(0, 10)}. Files after that have one more column.`,
     `Bronze added the column in batch ${alter ? alter.batch : "–"} as a metadata-only Iceberg change. Older loans read as null, which a quality rule allows.`],
    ["Two changes, one timestamp", `${p.same_txn_cures} late payments were written as two statements in one transaction, both with the same cdc_ts.`,
     "Ordering uses AR_H_CHANGE_SEQ, which is unique and increasing, so the amount paid is never left to chance."],
    ["Changes DMS never wrote", `${p.gap_changes} repayment changes between ${p.gap_window[0].slice(11, 16)} and ${p.gap_window[1].slice(11, 16)} on ${p.gap_window[0].slice(0, 10)} were never delivered.`,
     recon ? `No key can catch a missing row. Reconciliation to the source found ${recon.rows_lake === recon.rows_source ? "" : NX.int(recon.missing_in_lake) + " missing and "}${NX.int(recon.different)} stale rows. A reload fixed them: ${fixed.match}/${fixed.rows_source} rows match.` : ""],
  ];
  document.getElementById("problem-cards").innerHTML = problems.map(([h, what, how], i) => `
    <div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="kicker" style="color:var(--violet2)">Problem ${i + 1}</div>
      <h3 style="margin:.35rem 0 .5rem;color:#fff">${h}</h3><p>${NX.esc(what)}</p>
      <p class="how"><b>Handled:</b> ${NX.esc(how)}</p></div>`).join("");

  const s = d.story;
  document.getElementById("story").innerHTML = NX.table(s.changes.map((c) => ({
    Op: c.op, cdc_ts: c.cdc_ts, change_seq: `<code>${c.change_seq.slice(0, 14)}<b>${c.change_seq.slice(-6)}</b></code>`,
    status: c.status, amount_paid: c.amount_paid, file: `<code>${NX.esc(c.file)}</code>` })))
    + `<p style="margin-top:.6rem">Silver for <code>${s.repayment_id}</code>: <b>${s.silver.status}</b>, amount paid <b>${NX.money2(s.silver.amount_paid)}</b>. That matches the source.</p>`;

  // 04 · the last run
  const bronzeTotal = sumBy(d.tables, (t) => t.bronze_changes);
  const tiles = [
    [NX.int(g.cdc_files + Object.keys(g.full_load).length), "DMS files landed", `${NX.int(sumBy(g.full_load, (v) => v))} rows in the full load, ${NX.int(sumBy(g.cdc_changes, (v) => v))} CDC changes`],
    [NX.int(replayNew), "rows changed by a full replay", `${NX.int(sumBy(d.replay, (v) => v.duplicates))} changes re-read, all recognised`],
    [NX.int(recon ? recon.rows_source - recon.match : 0), `${recon ? recon.table : ""} rows the reconciliation caught`, "the DMS gap on 20 September, fixed by a reload"],
    [`${passed}/${final.length}`, "checks pass after the fix", `${first.filter((c) => c.status === "fail").length} failed on the first pass, every one explained below`],
  ];
  document.getElementById("run-tiles").innerHTML = tiles.map(([n, l, sub], i) =>
    `<div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="big ${i % 2 ? "p" : ""}">${n}</div><h3 style="margin:.4rem 0 .2rem">${l}</h3><p>${NX.esc(sub)}</p></div>`).join("");

  document.getElementById("batches").innerHTML = NX.table(d.batches.map((b) => ({
    batch: b.batch, delivered: b.label, files: sumBy(b.tables, (t) => t.files),
    "new changes": sumBy(b.tables, (t) => t.new_changes), duplicates: sumBy(b.tables, (t) => t.duplicates) })));

  const key = (c) => `${c.table} · ${c.check}`;
  const firstBy = Object.fromEntries(first.map((c) => [key(c), c.status]));
  document.getElementById("checks").innerHTML = `<div class="checklist">${final.map((c) => {
    const was = firstBy[key(c)];
    return `<div><span><code>${c.table}</code> ${NX.esc(c.check)}</span><span>${was !== c.status ? `<span class="fail">✗</span> → ` : ""}<span class="${c.status}">${c.status === "pass" ? "✓" : "✗"}</span></span></div>`;
  }).join("")}</div>`;

  const inc = d.incident, er = d.erasure;
  const erFirst = first.find((c) => c.check.startsWith("erased"));
  document.getElementById("incident").innerHTML = inc ? `
    <div class="kicker" style="color:var(--pink)">Incident · found by the run, fixed by the run</div>
    <h3 style="margin:.4rem 0">${inc.rows_affected} ${inc.table} rows didn't match the source</h3>
    <p>${NX.esc(inc.diagnosis)}</p>
    <p>Bronze had <b>${inc.bronze_changes_in_that_hour_that_day}</b> repayment changes in that hour, against a median of <b>${inc.bronze_changes_in_that_hour_other_days_median}</b> on other days.</p>
    <p><b>Fix:</b> ${NX.esc(inc.remedy)} The reload (<code>${NX.esc(d.reload.file)}</code>) fixed all ${inc.rows_affected} rows${d.reload.silver.live_delta ? `, ${d.reload.silver.live_delta} of them missing until then` : ""}.</p>
    <p style="margin-top:.8rem"><b>Also on the first pass:</b> the erasure check failed because ${erFirst.snapshots_with_pii} of ${erFirst.snapshots} snapshots of <code>silver.customers</code> could still time-travel to ${er.customer}'s PII. The erasure step deleted their changes from bronze (${Object.entries(er.bronze_rows_deleted).map(([t, n]) => `${n} in ${t}`).join(", ")}) and expired old snapshots on ${er.expired_tables.length} tables.</p>` : "";

  const total = d.tasks.reduce((a, t) => Math.max(a, t.start_s + t.duration_s), 0);
  document.getElementById("tasks").innerHTML = d.tasks.map((t) => `
    <div class="task"><span>${NX.esc(t.label)}</span><span class="bar"><i style="left:${t.start_s / total * 100}%;width:${Math.max(.6, t.duration_s / total * 100)}%"></i></span><span class="num">${t.duration_s}s</span></div>`).join("")
    + `<p style="font-size:.82rem;margin-top:.5rem">${d.seconds}s end to end on a 2-core local Spark.</p>`;

  // 05 · layout
  const names = { naive: "Naive: unpartitioned, one file per delivery", merged: "Partitioned by month, as MERGE wrote it",
                  compacted: "Partitioned, sorted by customer, compacted" };
  document.getElementById("layout-queries").innerHTML = Object.values(d.layout.queries).map((q) => {
    const max = Math.max(...["naive", "merged", "compacted"].map((k) => q[k].bytes_read));
    return `<div class="card"><h3>${NX.esc(q.label)}</h3>
      ${["naive", "merged", "compacted"].map((k) => `<div class="lbar"><div class="lbl">${names[k]}</div>
        <div class="track"><i class="${k}" style="width:${q[k].bytes_read / max * 100}%"></i></div>
        <div class="val">${kb(q[k].bytes_read)} · ${q[k].files_read}/${q[k].files_total} files</div></div>`).join("")}
      <p style="margin-top:.6rem"><b>${pctLess(q.naive.bytes_read, q.compacted.bytes_read)}</b> scanned than the naive layout${q.same_answer ? ", with the same answer." : "."}</p></div>`;
  }).join("");

  // 06 · Data Science
  const bands = d.gold_rows.calibration_by_band;
  const W = 520, H = 220, pad = 34, bw = (W - pad * 2) / bands.length;
  const top = Math.max(...bands.flatMap((b) => [b.predicted, b.observed])) * 1.15;
  const y = (v) => H - pad - v / top * (H - pad * 2);
  document.getElementById("calibration").innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Predicted and observed miss rate by score band">
    ${[0, .25, .5].filter((v) => v < top).map((v) => `<line x1="${pad}" x2="${W - pad}" y1="${y(v)}" y2="${y(v)}" class="grid"/><text x="${pad - 6}" y="${y(v) + 4}" class="ax" text-anchor="end">${v * 100}%</text>`).join("")}
    ${bands.map((b, i) => { const x = pad + i * bw;
      return `<rect x="${x + bw * .14}" y="${y(b.predicted)}" width="${bw * .32}" height="${H - pad - y(b.predicted)}" class="pred"/>
        <rect x="${x + bw * .5}" y="${y(b.observed)}" width="${bw * .32}" height="${H - pad - y(b.observed)}" class="obs"/>
        <text x="${x + bw / 2}" y="${H - pad + 16}" class="ax" text-anchor="middle">${b.band} · ${b.loans}</text>`; }).join("")}
  </svg><p class="legend"><span class="sw pred"></span>predicted (mean score) <span class="sw obs"></span>observed</p>
  <p style="font-size:.86rem">${(() => { const hi = bands[bands.length - 1];
    return `In the top band the scorecard predicts ${NX.pct(hi.predicted)} and ${NX.pct(hi.observed)} happened. That gap is the work recalibration does, and it needs this table refreshed from the lake, not pulled from Aurora.`; })()}</p>`;

  const f = d.gold_rows.features_pit;
  document.getElementById("pit").innerHTML = `<div class="grid g2" style="gap:.6rem">
      <div><div class="big">${NX.int(f.differ)}<small style="font-size:1rem;color:var(--muted)">/${NX.int(f.applications)}</small></div><p>decisions since 1 July where the naive income feature differs</p></div>
      <div><div class="big p">${NX.money(f.mean_naive - f.mean_pit)}</div><p>mean income the naive feature adds, which the model never saw</p></div></div>
    <p style="font-size:.86rem;margin-top:.6rem">Worst case: <code>${f.example.application_id}</code>, decided ${f.example.decided_at.slice(0, 10)}. Point in time ${NX.money(f.example.income_90d_pit)}, naive ${NX.money(f.example.income_90d_naive)}.</p>`;

  const lb = d.gold_rows.loan_book;
  const months = [...new Set(lb.map((r) => r.month_end))], statuses = [...new Set(lb.map((r) => r.status))];
  document.getElementById("loanbook").innerHTML = NX.table(months.map((m) => Object.fromEntries([["month end", m],
    ...statuses.map((st) => [st, (lb.find((r) => r.month_end === m && r.status === st) || {}).loans ?? 0])])));
}).catch((e) => console.error(e));

fetch("infra/dms/s3-target-settings.json").then((r) => r.text()).then((t) => {
  document.getElementById("dms-settings").textContent = "// infra/dms/s3-target-settings.json: the DMS S3 target the pipeline expects\n" + t;
}).catch(() => {});
