// CDC simulator: the same rules as src/nexus/bronze.py and silver.py, in the browser.
//   bronze: one row per (key, change_seq); a copy already there is skipped
//   silver: a change lands only if its change_seq is newer; a delete is a tombstone with values nulled;
//           a reload is authoritative (keys it doesn't contain are tombstoned)
const Lake = () => {
  const bronze = [], silver = new Map();
  const ingest = (changes) => {
    const fresh = [];
    for (const c of changes) {
      if (bronze.some((b) => b.key === c.key && b.seq === c.seq)) { c._dup = true; continue; }
      bronze.push(c); fresh.push(c);
    }
    return fresh;
  };
  const apply = (fresh, authoritative = false) => {
    const latest = new Map();
    for (const c of fresh) if (!latest.has(c.key) || c.seq > latest.get(c.key).seq) latest.set(c.key, c);
    const out = [];
    for (const [k, c] of latest) {
      const t = silver.get(k);
      if (t && !(c.seq > t.seq)) { out.push({ key: k, result: "ignored: older than silver", c }); continue; }
      const del = c.op === "D";
      silver.set(k, { key: k, seq: c.seq, ts: c.ts, deleted: del, vals: del ? null : { ...c.vals } });
      out.push({ key: k, result: del ? "tombstoned" : t ? "updated" : "inserted", c });
    }
    if (authoritative) for (const [k, t] of silver) if (!latest.has(k) && !t.deleted) {
      t.deleted = true; out.push({ key: k, result: "tombstoned: not in the reload" });
    }
    return out;
  };
  return { bronze, silver, ingest, apply };
};

const seq = (ts, n) => ts.replace(/\D/g, "").slice(0, 14) + String(n).padStart(21, "0");
const ch = (op, ts, n, key, vals = {}, file = "") => ({ op, ts, seq: seq(ts, n), key, vals, file });

// six scenarios; each step is a delivery (one DMS file) or a check
const SCENARIOS = (p) => [
  { title: "Re-sent after a task restart", lesson: "Bronze keys every change on (key, change_seq), so a copy in a new file is recognised and skipped.",
    steps: [
      { file: "loans/2026/08/12/20260812-235959000.csv", note: "The day's changes arrive.", changes: [
        ch("U", "2026-08-12 08:40:00", 51, "L0007", { status: "in_arrears" }), ch("U", "2026-08-12 09:15:00", 52, "L0007", { status: "active" })] },
      { file: `loans/2026/08/12/20260812-100500000.csv`, note: `DMS restarts at ${p.restart_at.slice(11, 16)} and re-sends the last two hours in a new file.`, changes: [
        ch("U", "2026-08-12 08:40:00", 51, "L0007", { status: "in_arrears" }), ch("U", "2026-08-12 09:15:00", 52, "L0007", { status: "active" })] },
    ] },
  { title: "A file delivered late", lesson: "Files are listed by arrival time, and change_seq orders the changes, so a late file can't overwrite newer data.",
    steps: [
      { file: "customers/2026/09/05/20260905-235959000.csv", note: "5 September arrives on time: a new postcode.", changes: [
        ch("U", "2026-09-05 10:00:00", 90, "C00042", { postcode: "N12 8AA" })] },
      { file: `customers/2026/09/03/20260903-235959000.csv`, note: `The 3 September file lands on ${p.late_file.arrived.slice(0, 10)}, carrying an older postcode.`, changes: [
        ch("U", "2026-09-03 09:00:00", 70, "C00042", { postcode: "E14 2BB" })] },
    ] },
  { title: "A delete, then an older update", lesson: "A delete is a tombstone that keeps its change_seq, so the older update can't bring the row back. Its values are nulled: erasure means erasure.",
    steps: [
      { file: "customers/LOAD00000001.csv", note: "The full load.", changes: [
        ch("I", "2026-07-01 00:00:00", 0, p.erased_customer, { email: "amira.khan@example.com" })] },
      { file: "customers/2026/09/10/20260910-235959000.csv", note: `${p.erased_customer} asks to be forgotten on 10 September.`, changes: [
        ch("D", "2026-09-10 11:00:00", 120, p.erased_customer)] },
      { file: "customers/2026/09/03/20260903-235959000.csv", note: "The late 3 September file arrives with their email change.", changes: [
        ch("U", "2026-09-03 14:05:00", 71, p.erased_customer, { email: "new.amira.khan@example.com" })] },
    ] },
  { title: "A column added mid-stream", lesson: "Bronze adds the new column with ALTER TABLE ADD COLUMNS, a metadata-only change in Iceberg. Older rows read as null.",
    steps: [
      { file: "loans/2026/07/31/20260731-235959000.csv", note: "Before the ALTER: no apr_bps column.", changes: [
        ch("I", "2026-07-31 12:00:00", 40, "L0100", { principal: "2000.00", status: "active" })] },
      { file: "loans/2026/08/01/20260801-235959000.csv", note: "ALTER TABLE loans ADD COLUMN apr_bps. The file has one more column.", changes: [
        ch("I", "2026-08-01 15:00:00", 41, "L0101", { principal: "3000.00", status: "active", apr_bps: "2490" })] },
    ] },
  { title: "Two changes, one timestamp", lesson: "Both statements share a commit timestamp, and only AR_H_CHANGE_SEQ orders them. Sorting by cdc_ts would leave the amount to chance.",
    steps: [
      { file: "repayments/2026/08/16/20260816-235959000.csv", note: "A missed instalment is paid late: two statements in one transaction.", changes: [
        ch("U", "2026-08-16 16:00:00", 938, "L0257-10", { status: "paid_late", amount_paid: "93.33" }),
        ch("U", "2026-08-16 16:00:00", 937, "L0257-10", { status: "paid_late", amount_paid: "0.00" })] },
    ] },
  { title: "Changes DMS never wrote", lesson: "No key can catch a missing row. Reconciliation to the source finds it, and a reload applied as authoritative fixes it.",
    steps: [
      { file: "repayments/LOAD00000001.csv", note: "The full load: two instalments due.", changes: [
        ch("I", "2026-07-01 00:00:00", 0, "L0081-11", { status: "due", amount_paid: "0.00" }),
        ch("I", "2026-07-01 00:00:00", 0, "L0081-12", { status: "due", amount_paid: "0.00" })] },
      { file: "(nothing)", note: `The direct-debit run at 03:00 on 20 September pays L0081-11, but DMS never writes the change (${p.gap_changes} changes lost).`, changes: [],
        source: { "L0081-11": { status: "paid", amount_paid: "93.33" } } },
      { check: true, note: "Reconciliation compares silver with the source, row by row." },
      { file: "repayments/LOAD202610010730.csv", reload: true, note: "Reload the table from the source and apply it as authoritative.", changes: [
        ch("I", "2026-10-01 07:30:00", 0, "L0081-11", { status: "paid", amount_paid: "93.33" }),
        ch("I", "2026-10-01 07:30:00", 0, "L0081-12", { status: "due", amount_paid: "0.00" })] },
      { check: true, note: "Reconciliation again." },
    ] },
];

const short = (s) => `${s.slice(0, 14)}…<b>${s.slice(-4)}</b>`;
const vals = (v) => v ? Object.entries(v).map(([k, x]) => `${k}=${x}`).join(" ") : "<i>null</i>";   // NX.table escapes

function render(sc, upto, el) {
  const lake = Lake(), source = new Map();
  const log = [];
  sc.steps.slice(0, upto).forEach((st, i) => {
    if (st.check) {
      const bad = [...source].filter(([k, v]) => { const s = lake.silver.get(k); return !s || JSON.stringify(s.vals) !== JSON.stringify(v); });
      log.push({ i, st, check: bad.length ? `✗ ${bad.length} row differs from the source: ${bad.map(([k]) => k).join(", ")}` : "✓ every row matches the source" });
      return;
    }
    for (const c of st.changes) if (c.op === "D") source.delete(c.key); else source.set(c.key, { ...(source.get(c.key) || {}), ...c.vals });
    for (const [k, v] of Object.entries(st.source || {})) source.set(k, { ...source.get(k), ...v });
    const cs = st.changes.map((c) => ({ ...c }));
    const fresh = lake.ingest(cs);
    log.push({ i, st, cs, fresh, applied: lake.apply(fresh, st.reload) });
  });
  const last = log[log.length - 1];
  el.querySelector(".sim-steps").innerHTML = sc.steps.map((st, i) =>
    `<li class="${i < upto ? "done" : ""} ${i === upto - 1 ? "now" : ""}"><b>${i + 1}</b> ${NX.esc(st.note)}</li>`).join("");
  el.querySelector(".lane-file").innerHTML = last && !last.check ? `<div class="kicker">${last.st.reload ? "Reload file" : "DMS file"}</div>
    <code class="fname">${NX.esc(last.st.file)}</code>${last.cs.length ? NX.table(last.cs.map((c) => ({ Op: c.op, cdc_ts: c.ts, change_seq: `<code>${short(c.seq)}</code>`, key: c.key, values: vals(c.vals) }))) : "<p class='muted'>No file. The changes never left Aurora.</p>"}` :
    last ? `<div class="kicker">Check</div><p class="${last.check.startsWith("✓") ? "pass" : "fail"}" style="font-size:1rem">${NX.esc(last.check)}</p>` : "<p class='muted'>Press Next to deliver the first file.</p>";
  el.querySelector(".lane-bronze").innerHTML = `<div class="kicker">Bronze · every change</div>` + (lake.bronze.length ? NX.table(lake.bronze.map((b) => ({
    Op: b.op, change_seq: `<code>${short(b.seq)}</code>`, key: b.key }))) : "<p class='muted'>empty</p>")
    + (last && last.cs ? `<p class="note">${last.cs.filter((c) => c._dup).length ? `<b>${last.cs.filter((c) => c._dup).length} duplicate${last.cs.filter((c) => c._dup).length > 1 ? "s" : ""} skipped</b> · ` : ""}${last.fresh.length} new</p>` : "");
  el.querySelector(".lane-silver").innerHTML = `<div class="kicker">Silver · current state</div>` + (lake.silver.size ? NX.table([...lake.silver.values()].map((s) => ({
    key: s.key, values: s.deleted ? "<span class='fail'>tombstone</span>" : vals(s.vals), change_seq: `<code>${short(s.seq)}</code>` }))) : "<p class='muted'>empty</p>")
    + (last && last.applied ? `<ul class="note">${last.applied.map((a) => `<li><code>${a.key}</code> ${NX.esc(a.result)}</li>`).join("")}</ul>` : "");
  el.querySelector(".sim-lesson").innerHTML = upto === sc.steps.length ? `<b>What it shows:</b> ${NX.esc(sc.lesson)}` : "";
  // animate the token along the lanes
  el.querySelectorAll(".lane").forEach((l, k) => { l.classList.remove("pulse"); void l.offsetWidth; setTimeout(() => l.classList.add("pulse"), k * 220); });
}

NX.site().then((d) => {
  const scs = SCENARIOS(d.generator.planted);
  const root = document.getElementById("sim");
  let cur = 0, upto = 0, timer = null;
  const tabs = document.getElementById("sim-tabs");
  tabs.innerHTML = scs.map((s, i) => `<button data-i="${i}"><b>${i + 1}</b>${s.title}</button>`).join("");
  const show = () => { tabs.querySelectorAll("button").forEach((b, i) => b.classList.toggle("on", i === cur));
    root.querySelector("h3.sim-title").textContent = scs[cur].title; render(scs[cur], upto, root);
    document.getElementById("sim-next").disabled = upto >= scs[cur].steps.length; };
  const stop = () => { clearInterval(timer); timer = null; document.getElementById("sim-play").textContent = "▶ Play"; };
  tabs.addEventListener("click", (e) => { const b = e.target.closest("button"); if (!b) return; stop(); cur = +b.dataset.i; upto = 0; show(); });
  document.getElementById("sim-next").onclick = () => { stop(); upto = Math.min(upto + 1, scs[cur].steps.length); show(); };
  document.getElementById("sim-reset").onclick = () => { stop(); upto = 0; show(); };
  document.getElementById("sim-play").onclick = () => {
    if (timer) return stop();
    if (upto >= scs[cur].steps.length) upto = 0;
    document.getElementById("sim-play").textContent = "❚❚ Pause";
    const tick = () => { if (upto >= scs[cur].steps.length) { if (cur < scs.length - 1) { cur++; upto = 0; } else return stop(); } else upto++; show(); };
    tick(); timer = setInterval(tick, 2200);
  };
  show();

  // try it: your own changes, the same rules
  const mine = Lake();
  let n = 0;
  const form = document.getElementById("try");
  const draw = (applied = []) => {
    document.getElementById("try-bronze").innerHTML = mine.bronze.length ? NX.table(mine.bronze.map((b) => ({ Op: b.op, cdc_ts: b.ts, change_seq: `<code>${short(b.seq)}</code>`, key: b.key, status: b.vals.status ?? "" }))) : "<p class='muted'>empty</p>";
    document.getElementById("try-silver").innerHTML = (mine.silver.size ? NX.table([...mine.silver.values()].map((s) => ({ key: s.key, status: s.deleted ? "<span class='fail'>tombstone</span>" : s.vals.status, change_seq: `<code>${short(s.seq)}</code>` }))) : "<p class='muted'>empty</p>")
      + (applied.length ? `<ul class="note">${applied.map((a) => `<li><code>${a.key}</code> ${NX.esc(a.result)}</li>`).join("")}</ul>` : "");
  };
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const f = new FormData(form);
    const ts = f.get("ts").replace("T", " ") + ":00";
    const c = ch(f.get("op"), ts, +f.get("n") || ++n, f.get("key").trim() || "L1", f.get("op") === "D" ? {} : { status: f.get("status") });
    const fresh = mine.ingest([c]);
    draw(fresh.length ? mine.apply(fresh) : [{ key: c.key, result: "duplicate (same key and change_seq): skipped in bronze" }]);
  });
  document.getElementById("try-reset").onclick = () => { mine.bronze.length = 0; mine.silver.clear(); draw(); };
  draw();
}).catch((e) => console.error(e));
