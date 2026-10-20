// The run & replayed: the recorded pipeline run as a graph, each step lighting up in order with its real
// duration, then its checks, the incident and the fix. Same graph and replay as NEXUS for Codat's
// orchestrator page; here the steps are DMS, Spark on Glue and Iceberg. Data: assets/data/site.json.
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
      ${n.kinds.slice(0, 3).map((kd, j) => `<circle cx="${W / 2 - 18 + j * 18}" cy="55" r="5" fill="${kindColor[kd]}"><title>${kd}</title></circle>`).join("")}
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
  let playing = false;
  $("o-play").addEventListener("click", async () => {
    if (playing) return; playing = true;
    NODES.forEach((n) => { $("a-" + n.k).setAttribute("class", "node off"); const e = $("e-" + n.k); if (e) e.setAttribute("class", "wire"); });
    const total = NODES.reduce((a, n) => a + secs(n.tasks), 0);
    for (const n of NODES) {
      const e = $("e-" + n.k); if (e) e.setAttribute("class", "wire hot");
      $("a-" + n.k).setAttribute("class", "node hot");
      $("o-cap").innerHTML = say(n);
      await NX.sleep(Math.max(700, (secs(n.tasks) / total) * 9000));
      $("a-" + n.k).setAttribute("class", (n.checks || []).some((c) => c.status !== "pass") ? "node hot" : "node on");
      if (e) e.setAttribute("class", "wire on");
    }
    $("o-cap").innerHTML = `The run finished in ${d.seconds}s: ${pass(final)}/${final.length} checks pass. The first pass failed on the planted DMS gap and the erasure; <b>reload</b> and <b>erasure</b> fixed both, and <b>recheck</b> proved it.`;
    playing = false;
  });
  new IntersectionObserver((es, ob) => { if (es[0].isIntersecting) { ob.disconnect(); setTimeout(() => $("o-play").click(), 500); } }, { threshold: .3 }).observe($("dag"));

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
