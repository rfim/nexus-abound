// The run, replayed: every task of the last pipeline run in order, scaled from its real duration,
// with the log lines it would print. Everything comes from assets/data/site.json.
const CODE = { generate: "generate.py", replay: "bronze.py", check: "checks.py", investigate: "pipeline.py", reload: "silver.py",
  erase: "pipeline.py", recheck: "checks.py", history: "silver.py", naive: "layout.py", layout: "layout.py", gold: "gold.py" };

NX.site().then((d) => {
  const sum = (o, f) => Object.values(o).reduce((a, v) => a + f(v), 0);
  const fmtT = (o) => Object.entries(o).map(([t, v]) => `${t.padEnd(16)} ${String(v.files).padStart(4)} files  ${String(v.new_changes).padStart(6)} new  ${String(v.duplicates).padStart(5)} dup${v.columns_added.length ? `   ALTER TABLE ADD COLUMNS ${v.columns_added.join(", ")}` : ""}`);
  const g = d.generator, inc = d.incident;
  const fails = d.checks_first.filter((c) => c.status === "fail");
  const lines = (t) => {
    if (t.key === "generate") return [`simulated ${NX.int(g.source_changes)} source changes in Aurora`,
      `wrote ${Object.keys(g.full_load).length} full-load files and ${g.cdc_files} CDC files to landing/dms/lending/`,
      `planted: restart replay, late file, erasure, ALTER TABLE, same-txn changes, a ${g.planted.gap_changes}-change gap`];
    if (t.key.startsWith("batch")) { const b = d.batches[+t.key.slice(5)]; return [`listing files delivered by ${b.until}`, ...fmtT(b.tables)]; }
    if (t.key === "replay") return [`re-read ${NX.int(sum(d.replay, (v) => v.rows_read))} changes from every file DMS ever wrote`,
      `new rows in bronze: ${sum(d.replay, (v) => v.new_changes)} · silver rows changed: ${sum(d.replay, (v) => v.silver.live_delta)}`];
    if (t.key === "check") return [...d.checks_first.filter((c) => c.status === "pass").length ? [`✓ ${d.checks_first.filter((c) => c.status === "pass").length} checks pass`] : [],
      ...fails.map((c) => `✗ ${c.table}: ${c.check}` + (c.different != null ? ` (${c.missing_in_lake} missing, ${c.different} different)` : c.snapshots_with_pii != null ? ` (${c.snapshots_with_pii}/${c.snapshots} snapshots still hold PII)` : ""))];
    if (t.key === "investigate") return inc ? [`${inc.rows_affected} keys differ; source changed them ${inc.source_changed_from} → ${inc.source_changed_to.slice(0, 19)}`,
      `bronze changes in that hour that day: ${inc.bronze_changes_in_that_hour_that_day} (median on other days: ${inc.bronze_changes_in_that_hour_other_days_median})`, `→ ${inc.diagnosis}`] : [];
    if (t.key === "reload") return [`DMS reload-table → ${d.reload.file}`, `${NX.int(d.reload.rows_read)} rows applied as authoritative · ${d.reload.silver.live_delta} rows came back`];
    if (t.key === "erase") return [`bronze rows deleted for ${d.erasure.customer}: ${Object.entries(d.erasure.bronze_rows_deleted).map(([k, v]) => `${k} ${v}`).join(", ")}`,
      `expire_snapshots on ${d.erasure.expired_tables.length} tables`];
    if (t.key === "recheck") return [`✓ ${d.checks.filter((c) => c.status === "pass").length}/${d.checks.length} checks pass`];
    if (t.key === "history") return Object.entries(d.history).map(([k, v]) => `${k}_history: ${NX.int(v)} versions (SCD2)`);
    if (t.key === "naive") return ["lab.ob_transactions_naive: unpartitioned, one file per delivery day"];
    if (t.key === "layout") return [`rewrite_data_files: ${d.layout.compaction.rewritten_files} files → ${d.layout.compaction.added_files} sorted files`,
      ...Object.values(d.layout.queries).map((q) => `${q.label}: ${Math.round(q.naive.bytes_read / 1024)} KB → ${Math.round(q.compacted.bytes_read / 1024)} KB scanned`)];
    if (t.key === "gold") return Object.entries(d.gold).map(([k, v]) => `gold.${k}: ${NX.int(v)} rows`);
    return [];
  };
  const status = (t) => (t.key === "check" && fails.length ? "warn" : "ok");

  const list = document.getElementById("o-tasks"), con = document.getElementById("o-log");
  list.innerHTML = d.tasks.map((t, i) => `<li data-i="${i}"><span class="dot"></span><span class="lbl">${NX.esc(t.label)}</span>
    <span class="prog"><i></i></span><span class="num">${t.duration_s}s</span>
    ${CODE[t.key] || t.key.startsWith("batch") ? `<a href="https://github.com/rfim/nexus-abound/blob/main/src/nexus/${CODE[t.key] || "bronze.py"}" class="code">${CODE[t.key] || "bronze.py · silver.py"}</a>` : ""}</li>`).join("");
  const tiles = [[d.tasks.length, "tasks"], [`${d.seconds}s`, "end to end, real run"], [`${fails.length} → 0`, "failing checks, first pass → after fix"], [NX.int(sum(d.tables, (t) => t.bronze_changes)), "changes in bronze"]];
  document.getElementById("o-tiles").innerHTML = tiles.map(([n, l], i) => `<div class="card"><div class="big ${i % 2 ? "p" : ""}">${n}</div><p>${l}</p></div>`).join("");

  let running = false;
  const play = async () => {
    if (running) return; running = true;
    const btn = document.getElementById("o-play"); btn.disabled = true;
    const speed = +document.getElementById("o-speed").value;
    con.textContent = ""; list.querySelectorAll("li").forEach((li) => { li.className = ""; li.querySelector(".prog i").style.width = 0; });
    const say = (s, cls = "") => { const l = document.createElement("div"); l.className = cls; l.textContent = s; con.appendChild(l); con.scrollTop = con.scrollHeight; };
    say(`$ ./scripts/build.sh   # ${new Date().toISOString().slice(0, 10)} replay of the recorded run`, "cmd");
    for (const [i, t] of d.tasks.entries()) {
      const li = list.children[i]; li.className = "run";
      li.scrollIntoView({ block: "nearest" });
      say(`▶ ${t.label}`, "task");
      const ms = Math.max(350, t.duration_s * 1000 / speed), bar = li.querySelector(".prog i"), t0 = performance.now();
      const ls = lines(t), cls = (x) => (/^✗/.test(x) ? "bad" : /^✓/.test(x) ? "good" : "");
      let shown = 0;
      await new Promise((res) => { const step = (now) => { const f = Math.min(1, (now - t0) / ms); bar.style.width = f * 100 + "%";
        // the log lines come out as the task runs, the last one as it finishes
        for (; shown < Math.floor(f * ls.length); shown++) say("  " + ls[shown], cls(ls[shown]));
        f < 1 ? requestAnimationFrame(step) : res(); }; requestAnimationFrame(step); });
      for (; shown < ls.length; shown++) say("  " + ls[shown], cls(ls[shown]));
      li.className = status(t) === "warn" ? "warn" : "done";
      await NX.sleep(120);
    }
    say(`✓ run complete in ${d.seconds}s (replayed ×${speed})`, "good");
    btn.disabled = false; running = false;
  };
  document.getElementById("o-play").onclick = play;
  // show the finished state before anyone presses play
  list.querySelectorAll("li").forEach((li, i) => { li.className = status(d.tasks[i]) === "warn" ? "warn" : "done"; li.querySelector(".prog i").style.width = "100%"; });
  con.textContent = "Press ▶ Replay the run to stream the log.";
}).catch((e) => console.error(e));
