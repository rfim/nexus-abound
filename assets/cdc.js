// CDC & step by step: a change leaves Aurora, DMS writes it to S3, a Glue job lands it in bronze and
// applies it to silver. Same stage, packets and log as the MCP page of NEXUS for Codat; the rules are a
// line-for-line port of src/nexus/bronze.py and silver.py.
(async () => {
  const $ = (id) => document.getElementById(id);
  const d = await NX.site(), P = d.generator.planted;

  // ---------- the lake, in the browser ----------
  const Lake = () => {
    const bronze = [], silver = new Map();
    return {
      bronze, silver,
      ingest(changes) {   // bronze: one row per (key, change_seq)
        const fresh = [], dup = [];
        for (const c of changes) (bronze.some((b) => b.key === c.key && b.seq === c.seq) ? dup : fresh).push(c);
        bronze.push(...fresh); return { fresh, dup };
      },
      apply(fresh, authoritative = false) {   // silver: newest change_seq wins; deletes are tombstones
        const latest = new Map(), out = [];
        for (const c of fresh) if (!latest.has(c.key) || c.seq > latest.get(c.key).seq) latest.set(c.key, c);
        for (const [k, c] of latest) {
          const t = silver.get(k);
          if (t && !(c.seq > t.seq)) { out.push(`${k}: ignored, change_seq …${c.seq.slice(-4)} is older than silver's …${t.seq.slice(-4)}`); continue; }
          const del = c.op === "D";
          silver.set(k, { seq: c.seq, deleted: del, vals: del ? null : { ...(t?.vals || {}), ...c.vals } });
          out.push(`${k}: ${del ? "tombstone (values nulled)" : t ? "updated" : "inserted"}`);
        }
        if (authoritative) for (const [k, t] of silver) if (!latest.has(k) && !t.deleted) { t.deleted = true; out.push(`${k}: tombstoned, not in the reload`); }
        return out;
      },
    };
  };
  const seq = (ts, n) => ts.replace(/\D/g, "").slice(0, 14) + String(n).padStart(21, "0");
  const C = (op, ts, n, key, vals = {}) => ({ op, ts, seq: seq(ts, n), key, vals });
  const csv = (rows) => ["Op,cdc_ts,change_seq,key," + Object.keys(rows[0]?.vals || {}).join(","),
    ...rows.map((r) => [r.op, r.ts, r.seq, r.key, ...Object.values(r.vals)].join(","))].join("\n");

  // ---------- six problems, as deliveries ----------
  // each delivery becomes: DMS writes the file → Glue lands it in bronze → silver applies it
  const SCENARIOS = [
    { label: "1 · Re-sent after a task restart", deliveries: [
      { sql: "UPDATE loans SET status = 'in_arrears' WHERE loan_id = 'L0007';\nUPDATE loans SET status = 'active' WHERE loan_id = 'L0007';",
        file: "loans/2026/08/12/20260812-235959000.csv", rows: [C("U", "2026-08-12 08:40:00", 51, "L0007", { status: "in_arrears" }), C("U", "2026-08-12 09:15:00", 52, "L0007", { status: "active" })],
        cap: "Two changes to one loan, written by DMS in the day's file.",
        lp: "At Lifepal DMS and Debezium both re-sent changes after restarts and rebalances; it is normal, not an incident.", cx: "Expect it after every DMS task restart or failover. Make the merge idempotent, not the delivery." },
      { file: "loans/2026/08/12/20260812-100500000.csv", restart: true, rows: [C("U", "2026-08-12 08:40:00", 51, "L0007", { status: "in_arrears" }), C("U", "2026-08-12 09:15:00", 52, "L0007", { status: "active" })],
        cap: `DMS restarts at ${P.restart_at.slice(11, 16)} and re-sends the last two hours, in a <b>new file</b>.`,
        lp: "We keyed bronze on the source's own log position, never on the file name: a re-sent change is the same change.", cx: "Key bronze on (primary key, AR_H_CHANGE_SEQ). A file-name bookmark alone would let these duplicates in." },
    ] },
    { label: "2 · A file delivered late", deliveries: [
      { sql: "UPDATE customers SET postcode = 'N12 8AA' WHERE customer_id = 'C00042';", file: "customers/2026/09/05/20260905-235959000.csv",
        rows: [C("U", "2026-09-05 10:00:00", 90, "C00042", { postcode: "N12 8AA" })], cap: "5 September's file arrives on time with a new postcode.",
        lp: "Late files were routine on our S3 landing: retries, a slow multipart upload, a task catching up.", cx: "List S3 by LastModified, not by the date in the path, so the next run picks up whatever arrived." },
      { file: "customers/2026/09/03/20260903-235959000.csv", rows: [C("U", "2026-09-03 09:00:00", 70, "C00042", { postcode: "E14 2BB" })],
        cap: `The <b>3 September</b> file lands on ${P.late_file.arrived.slice(0, 10)}, carrying an older postcode.`,
        lp: "Ordering came from the source log, so a late file could only add history, never overwrite newer state.", cx: "Silver compares change_seq, so the older postcode is kept in bronze history but never becomes current." },
    ] },
    { label: "3 · A delete, then an older update", deliveries: [
      { sql: "-- the full load on 1 July", file: "customers/LOAD00000001.csv", rows: [C("I", "2026-07-01 00:00:00", 0, P.erased_customer, { email: "amira.khan@example.com" })],
        cap: "The full load brings the customer in.", lp: "Right-to-erasure requests went through the same CDC path as any delete.", cx: "Erasure starts in Aurora; the lake has to follow it, everywhere." },
      { sql: `DELETE FROM customers WHERE customer_id = '${P.erased_customer}';`, file: "customers/2026/09/10/20260910-235959000.csv", rows: [C("D", "2026-09-10 11:00:00", 120, P.erased_customer)],
        cap: `${P.erased_customer} asks to be forgotten. DMS delivers the <b>delete</b>.`,
        lp: "Deletes became tombstones, keyed and sequenced like any change, with personal fields nulled.", cx: "Keep the tombstone and its change_seq; don't hard-delete the silver row, or the next late update brings it back." },
      { file: "customers/2026/09/03/20260903-235959000.csv", rows: [C("U", "2026-09-03 14:05:00", 71, P.erased_customer, { email: "new.amira.khan@example.com" })],
        cap: "The late 3 September file arrives with their <b>earlier</b> email change.",
        lp: "We purged bronze and expired old snapshots afterwards, so time travel couldn't recover the data either.", cx: "Then purge bronze and run expire_snapshots: Iceberg time travel would otherwise still hold the email." },
    ] },
    { label: "4 · A column added mid-stream", deliveries: [
      { sql: "INSERT INTO loans (loan_id, principal, status) VALUES ('L0100', 2000, 'active');", file: "loans/2026/07/31/20260731-235959000.csv",
        rows: [C("I", "2026-07-31 12:00:00", 40, "L0100", { principal: "2000.00", status: "active" })], cap: "A loan before the schema change.",
        lp: "Source teams changed schemas without warning; the pipeline had to absorb additive changes on its own.", cx: "Agree with the Aurora owners: additive changes flow through; renames and type changes need a heads-up." },
      { sql: "ALTER TABLE loans ADD COLUMN apr_bps int;\nINSERT INTO loans (..., apr_bps) VALUES ('L0101', 3000, 'active', 2490);", file: "loans/2026/08/01/20260801-235959000.csv", alter: true,
        rows: [C("I", "2026-08-01 15:00:00", 41, "L0101", { principal: "3000.00", status: "active", apr_bps: "2490" })], cap: "<b>ALTER TABLE</b>: files after 1 August have one more column.",
        lp: "New columns were added to bronze automatically, then promoted to silver through the contract.", cx: "Iceberg's ADD COLUMNS is metadata only: no rewrite. Older rows read as null, and a quality rule says when null is fine." },
    ] },
    { label: "5 · Two changes, one timestamp", deliveries: [
      { sql: "BEGIN;\nUPDATE repayments SET status = 'paid_late', amount_paid = 0 WHERE repayment_id = 'L0257-10';\nUPDATE repayments SET amount_paid = 93.33 WHERE repayment_id = 'L0257-10';\nCOMMIT;",
        file: "repayments/2026/08/16/20260816-235959000.csv", rows: [C("U", "2026-08-16 16:00:00", 938, "L0257-10", { status: "paid_late", amount_paid: "93.33" }), C("U", "2026-08-16 16:00:00", 937, "L0257-10", { status: "paid_late", amount_paid: "0.00" })],
        cap: "Two statements in one transaction share the <b>same commit timestamp</b>, and the file doesn't promise their order.",
        lp: "We ordered by the log position (LSN), never by a timestamp column.", cx: "Order by AR_H_CHANGE_SEQ: unique, increasing, even inside one transaction." },
    ] },
    { label: "6 · Changes DMS never wrote", gap: true, deliveries: [
      { sql: "-- the full load on 1 July", file: "repayments/LOAD00000001.csv", rows: [C("I", "2026-07-01 00:00:00", 0, "L0081-11", { status: "due", amount_paid: "0.00" }), C("I", "2026-07-01 00:00:00", 0, "L0081-12", { status: "due", amount_paid: "0.00" })],
        cap: "The full load: two instalments due.", lp: "Our worst incidents weren't duplicates. They were changes that never arrived.", cx: "Nothing in a CDC stream tells you what's missing. Only a comparison with the source does." },
      { sql: "UPDATE repayments SET status = 'paid', amount_paid = 93.33 WHERE repayment_id = 'L0081-11';  -- direct-debit run, 03:00", lost: true, rows: [],
        cap: `20 September, 03:00: the direct-debit run pays L0081-11, but DMS <b>never writes the change</b> (${P.gap_changes} changes lost in the run).`,
        lp: "A replication task once resumed from the wrong position after maintenance. Keys and counts looked fine.", cx: "Alert on hourly change volume per table, and on the DMS heartbeat, so a gap pages before a report finds it." },
      { reconcile: true, cap: "Reconciliation compares silver with Aurora, <b>row by row</b>.", lp: "A nightly row-by-row reconciliation against a read replica caught it the next morning.", cx: "Run it against an Aurora read replica, never the writer." },
      { sql: "-- DMS: reload table repayments", file: "repayments/LOAD202610010730.csv", reload: true, rows: [C("I", "2026-10-01 07:30:00", 0, "L0081-11", { status: "paid", amount_paid: "93.33" }), C("I", "2026-10-01 07:30:00", 0, "L0081-12", { status: "due", amount_paid: "0.00" })],
        cap: "Reload the table from the source, and apply it as <b>authoritative</b>.", lp: "The fix was a reload; the lesson was the volume alert.", cx: "A reload also tombstones rows the source no longer has (WHEN NOT MATCHED BY SOURCE)." },
      { reconcile: true, cap: "Reconciliation again.", lp: "Same check, same morning: green.", cx: "Close the incident only when the check that found it passes." },
    ] },
  ];
  // ---------- how each problem's rule was chosen (assets/choice.js): measured on this run, or scored on criteria ----------
  const reRead = Object.values(d.replay).reduce((a, v) => a + v.rows_read, 0), reChanged = Object.values(d.replay).reduce((a, v) => a + v.new_changes, 0);
  const inc = d.incident || {};
  const CH = [
    { q: "how does bronze recognise a change it already has?", ask: "Why not trust DMS to send each change once?", unit: "re-sent changes let into bronze",
      o: [["Bookmark by file name", P.replayed_changes], ["Key on (primary key, change_seq)", 0, 1]],
      why: `The re-sent changes come in a new file, so a file bookmark admits all ${P.replayed_changes}. A full replay of every file re-read ${reRead.toLocaleString("en-GB")} changes and changed ${reChanged} rows.` },
    { q: "which files does the next run read?", ask: "Why not read the folder for the date in the path?", crit: ["Late file picked up", "No re-reads", "Older change can't win"],
      o: [["Date in the S3 path", [0, 1, 0]], ["Re-list everything each run", [1, 0, 0]], ["LastModified since the last run · order by change_seq", [1, 1, 1], 1]],
      why: "A file for 3 September landed on 18 September. Listing by arrival picks it up; change_seq keeps its older postcode in history, never current." },
    { q: "what does a delete do in silver?", ask: "Why not just delete the row?", crit: ["A late update can't bring it back", "Personal data gone", "History for audit"],
      o: [["Hard-delete the row", [0, 1, 0]], ["Soft-delete flag, keep the fields", [1, 0, 1]], ["Tombstone with change_seq · purge bronze · expire snapshots", [1, 1, .5], 1]],
      why: `After the late 3 September update, live rows for ${P.erased_customer}: <b>${d.resurrection.live_rows_after_late_file}</b>. The tombstone's change_seq is newer, so the older update is ignored.` },
    { q: "how does a new source column land?", ask: "Why not fail the load until someone maps the new column?", crit: ["No table rewrite", "Old rows still read", "No deploy needed"],
      o: [["Fail the load until mapped", [1, 1, 0]], ["Rewrite the table with the new schema", [0, 1, 1]], ["Iceberg ADD COLUMNS · promote through the contract", [1, 1, 1], 1]],
      why: "Additive changes flow through as metadata only. Renames and type changes still need a heads-up from the Aurora owners." },
    { q: "what orders two changes to one row?", ask: "Why not order by the commit timestamp?", crit: ["Unique per change", "Follows commit order"],
      o: [["cdc_ts", [0, .5]], ["Row order in the file", [0, 0]], ["AR_H_CHANGE_SEQ", [1, 1], 1]],
      why: "Two statements in one transaction share a timestamp. The change sequence is unique and increasing, even inside one transaction." },
    { q: "what finds changes that never arrived?", ask: "Why didn't any key or count catch the missing payments?", unit: "missing rows found", low: false,
      o: [["Primary keys and row counts", 0], ["Row-by-row reconciliation with Aurora", inc.rows_affected || 0, 1]],
      why: `The rows existed, so keys and counts looked fine. Only comparing values found the ${inc.rows_affected} unpaid instalments. That hour brought ${inc.bronze_changes_in_that_hour_that_day} changes against a median of ${inc.bronze_changes_in_that_hour_other_days_median}: a volume alert would have paged.` },
  ];
  $("scenario").innerHTML = SCENARIOS.map((s, i) => `<option value="${i}">${s.label}</option>`).join("");

  // ---------- stage primitives (as on the Codat MCP page) ----------
  const setNode = (id, cls) => { const n = $(id); n.classList.remove("on", "hot", "off"); if (cls) n.classList.add(cls); };
  const setWire = (id, cls) => { const w = $(id); w.classList.remove("on", "hot"); if (cls) w.classList.add(cls); };
  const packet = $("packet");
  let speed = 1, instant = false;
  function fly(wireId, { reverse = false, color = "pk", ms = 850 } = {}) {
    const path = $(wireId), L = path.getTotalLength();
    packet.setAttribute("fill", `url(#${color})`);
    setWire(wireId, color === "pk" ? "hot" : "on");
    if (instant) { packet.setAttribute("cx", -50); return Promise.resolve(); }
    return new Promise((res) => {
      const t0 = performance.now(), dur = ms * speed;
      const tick = (t) => {
        const k = Math.min(1, (t - t0) / dur), e = k < .5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
        const pt = path.getPointAtLength((reverse ? 1 - e : e) * L);
        packet.setAttribute("cx", pt.x); packet.setAttribute("cy", pt.y);
        if (k < 1) requestAnimationFrame(tick); else { packet.setAttribute("cx", -50); res(); }
      };
      requestAnimationFrame(tick);
    });
  }
  const wait = (ms) => instant ? Promise.resolve() : NX.sleep(ms * speed);
  const log = (dir, text, cls = "req") => {
    const div = document.createElement("div");
    div.className = `msg ${cls}`;
    div.innerHTML = `<span class="dir">${dir}</span>${NX.esc(text)}`;
    $("wirelog").appendChild(div); $("wirelog").scrollTop = 1e6;
  };
  const counts = (lake) => {
    $("bronze-n").textContent = `${lake.bronze.length} change${lake.bronze.length === 1 ? "" : "s"}`;
    const live = [...lake.silver.values()].filter((s) => !s.deleted).length, tomb = lake.silver.size - live;
    $("silver-n").textContent = `${live} live${tomb ? ` · ${tomb} tombstone${tomb > 1 ? "s" : ""}` : ""}`;
  };

  // ---------- the script ----------
  let steps = [], idx = -1, playing = false, run = 0, view = "abound";
  const VIEW = { lifepal: ["In Lifepal", "lp"], lesson: ["Lesson for Abound", "cx"] };
  const lastCaption = { html: "" };
  const caption = (html) => { lastCaption.html = html; if (view === "abound") $("caption").innerHTML = html; };
  const showCaption = () => {
    const st = steps[idx];
    if (view === "abound" || !st) { $("caption").innerHTML = lastCaption.html; return; }
    $("caption").innerHTML = `<b>${VIEW[view][0]}:</b> ${NX.esc(st[VIEW[view][1]])}`;
  };

  function build() {
    const sc = SCENARIOS[+$("scenario").value], lake = Lake(), source = new Map(), s = [];
    let landed = null;
    for (const dl of sc.deliveries) {
      const extra = { lp: dl.lp, cx: dl.cx };
      if (dl.reconcile) {
        s.push({ ...extra, t: "Reconcile to source", d: "silver vs Aurora, every column", go: async () => {
          await fly("w-sr", { color: "pv" }); setNode("n-recon", "hot"); await fly("w-ra", { color: "pv" });
          const bad = [...source].filter(([k, v]) => { const x = lake.silver.get(k); return !x || x.deleted || JSON.stringify(x.vals) !== JSON.stringify(v); });
          log("reconcile · silver vs Aurora", bad.length ? `✗ ${bad.length} row differs: ${bad.map(([k, v]) => `${k} lake=${JSON.stringify(lake.silver.get(k)?.vals)} source=${JSON.stringify(v)}`).join("\n")}` : "✓ every row matches the source", bad.length ? "err" : "res");
          setNode("n-recon", bad.length ? "hot" : "on"); $("x-mark").setAttribute("opacity", bad.length ? 1 : 0);
          caption(bad.length ? `<b>✗ ${bad.length} row doesn't match the source.</b> No key could have caught it: the change never arrived.`
            : "<b>✓ Every row matches the source.</b> The incident is closed by the check that found it."); } });
        continue;
      }
      // the change in Aurora (the source's own state, for reconciliation)
      s.push({ ...extra, t: dl.lost ? "Committed in Aurora · never delivered" : dl.reload ? "DMS reload table" : "Committed in Aurora", d: dl.sql || "", go: async () => {
        setNode("n-aurora", "on");
        if (dl.sql) log("aurora · lending", dl.sql, dl.lost ? "err" : "req");
        // a late or re-sent file carries changes the source already had: only a new commit moves the source
        for (const r of dl.rows) if (dl.sql && !dl.reload) { if (r.op === "D") source.delete(r.key); else source.set(r.key, { ...(source.get(r.key) || {}), ...r.vals }); }
        if (dl.lost) { source.set("L0081-11", { status: "paid", amount_paid: "93.33" }); setNode("n-dms", "hot"); await fly("w-ad", { color: "pk" });
          log("dms task", "resumed from the wrong position: no file written for 02:00–04:00", "err"); }
        else { await fly("w-ad", { color: "pk" }); setNode("n-dms", "on"); }
        caption(dl.cap); } });
      if (dl.lost) continue;
      s.push({ ...extra, t: dl.restart ? "DMS re-sends, in a new file" : "DMS writes the file to S3", d: dl.file, go: async () => {
        await fly("w-ds", { color: "pk" }); setNode("n-s3", "hot");
        log(`s3://nexus-lake/dms/lending/${dl.file}`, csv(dl.rows), "res");
        if (dl.alter) $("s3-label").textContent = "header + apr_bps";
        caption(`DMS writes <code>${NX.esc(dl.file.split("/").pop())}</code>: Op, cdc_ts and <b>change_seq</b> (AR_H_CHANGE_SEQ) on every row.`); } });
      s.push({ ...extra, t: "Glue lands it in bronze", d: "MERGE ON (key, change_seq) WHEN NOT MATCHED INSERT", go: async () => {
        setNode("n-s3", "on"); await fly("w-sg", { color: "pk" }); setNode("n-glue", "hot"); await fly("w-gb", { color: "pk" }); setNode("n-bronze", "hot");
        if (dl.alter) log("glue · bronze", "ALTER TABLE lake.bronze.loans_changes ADD COLUMNS (apr_bps STRING)   -- metadata only", "req");
        landed = lake.ingest(dl.rows.map((r) => ({ ...r })));
        log("glue · bronze MERGE", `MERGE INTO lake.bronze.${dl.file.split("/")[0]}_changes t USING batch s\n  ON t.key = s.key AND t.change_seq = s.change_seq\n  WHEN NOT MATCHED THEN INSERT *\n→ ${landed.fresh.length} new · ${landed.dup.length} duplicate${landed.dup.length === 1 ? "" : "s"} skipped`, landed.dup.length ? "err" : "res");
        counts(lake);
        caption(landed.dup.length ? `<b>${landed.dup.length} duplicates skipped.</b> Same key, same change_seq: bronze already has them.` : `Bronze keeps <b>every change</b>, keyed on (key, change_seq). ${landed.fresh.length} new.`); } });
      s.push({ ...extra, t: dl.reload ? "Silver applies the reload as authoritative" : "Silver applies the newest change", d: "WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE", go: async () => {
        setNode("n-glue", "on"); setNode("n-bronze", "on"); await fly("w-bs", { color: "pk" }); setNode("n-silver", "hot");
        const out = lake.apply(landed.fresh, dl.reload);
        log("glue · silver MERGE", (out.length ? out : ["nothing new to apply"]).join("\n"), out.some((o) => o.includes("ignored")) ? "err" : "res");
        log("silver · current state", [...lake.silver].map(([k, v]) => `${k}  ${v.deleted ? "TOMBSTONE" : JSON.stringify(v.vals)}  change_seq …${v.seq.slice(-4)}`).join("\n") || "(empty)", "res");
        counts(lake); setNode("n-silver", "on");
        const ign = out.find((o) => o.includes("ignored"));
        caption(ign ? `<b>Ignored:</b> ${NX.esc(ign)}. Silver stays at the newest change.` : dl.reload ? "The reload lands as authoritative: the missing payment is back." : `Silver now holds the current state: ${NX.esc(out.join("; "))}.`); } });
    }
    s.push({ lp: "Reporting, ad-hoc analytics and model training read the lake, never the production database.", cx: "This is the path for model calibration: Athena and Spark read silver and gold, and Aurora never sees analytics load.", t: "Served from the lake", d: "Athena · Data Science · reports", go: async () => {
      await fly("w-sa", { color: "pv" }); setNode("n-ath", "on");
      ["n-dms", "n-s3", "n-glue", "n-bronze"].forEach((n) => setNode(n, ""));
      caption(`<b>✓ ${NX.esc(sc.label.slice(4))}:</b> handled. Athena and Data Science read silver; Aurora carries no analytics load.`); } });
    return s;
  }
  function renderSteps() {
    $("steps").innerHTML = steps.map((s, i) => `<li data-i="${i}" class="${i < idx ? "done" : i === idx ? "now" : ""}">
      <span class="dot">${i < idx ? "✓" : i + 1}</span><span><b>${NX.esc(s.t)}</b>${NX.esc(view === "abound" ? s.d : s[VIEW[view][1]])}</span></li>`).join("");
    $("steps").querySelectorAll("li").forEach((li) => li.addEventListener("click", () => jump(+li.dataset.i)));
    const now = $("steps").querySelector(".now");
    if (now) $("steps").scrollTop = now.offsetTop - $("steps").offsetTop - 80;
  }
  function resetStage() {
    run++; playing = false; $("play").textContent = "▶ Play";
    ["n-aurora", "n-dms", "n-s3", "n-glue", "n-bronze", "n-silver", "n-recon", "n-ath"].forEach((n) => setNode(n, n === "n-aurora" ? "" : "off"));
    ["w-ad", "w-ds", "w-sg", "w-gb", "w-bs", "w-sa", "w-sr", "w-ra"].forEach((w) => setWire(w, ""));
    $("x-mark").setAttribute("opacity", 0); $("s3-label").textContent = "CSV · Op, cdc_ts, change_seq";
    $("bronze-n").textContent = "every change"; $("silver-n").textContent = "current state";
    $("wirelog").innerHTML = "";
    caption(`Aurora is the lending database. Nothing reads it for analytics: every change reaches the lake through DMS. Press <b>Play</b>.`);
    steps = build(); idx = -1; renderSteps();
    NXChoice.render($("choice"), CH[+$("scenario").value]);
  }
  async function stepForward() {
    if (idx >= steps.length - 1) return false;
    idx++; renderSteps(); const my = run;
    await steps[idx].go();
    showCaption();
    return my === run;
  }
  async function jump(target) {
    resetStage(); instant = true;
    while (idx < target) await stepForward();
    instant = false; renderSteps();
  }
  async function play() {
    if (playing) { playing = false; $("play").textContent = "▶ Play"; return; }
    if (idx >= steps.length - 1) resetStage();
    playing = true; $("play").textContent = "❚❚ Pause"; const my = run;
    while (playing && my === run && idx < steps.length - 1) {
      const ok = await stepForward(); if (!ok) return;
      await wait(1100);
    }
    if (my === run) { playing = false; $("play").textContent = "↻ Replay"; }
  }
  document.querySelectorAll("#views [role=tab]").forEach((b) => b.addEventListener("click", () => {
    view = b.dataset.view;
    document.querySelectorAll("#views [role=tab]").forEach((x) => x.setAttribute("aria-selected", x === b));
    $("stage").dataset.view = view; renderSteps(); showCaption();
  }));
  $("play").addEventListener("click", play);
  $("next").addEventListener("click", () => { playing = false; $("play").textContent = "▶ Play"; stepForward(); });
  $("prev").addEventListener("click", () => jump(Math.max(-1, idx - 1)));
  $("reset").addEventListener("click", resetStage);
  $("speed").addEventListener("change", (e) => (speed = +e.target.value));
  $("scenario").addEventListener("change", resetStage);
  resetStage();
})().catch((e) => console.error(e));
