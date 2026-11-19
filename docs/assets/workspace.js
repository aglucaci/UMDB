(function(){
  // UMDB Workspace: reproducible research cohorts, kept in this browser (no account).
  //
  // A cohort is {id, name, created, definition, balance?} where definition is either
  // {filters: <Explorer filters>} or {op: "union"|"intersection"|"difference", a: <definition>, b: <definition>}.
  // This is the same document `umdb cohort export cohort.json` reads, and the research
  // package written here has the same files as the Python exporter (scripts/umdb_harvester/cohorts.py).
  // Balancing uses the same seeded shuffle (mulberry32) in both, so the samples match.
  const U = window.UMDB, X = window.UMDBUI;
  const { esc, fmt } = X;
  const DEFAULT_SEED = 20261119;

  // ---------------------------------------------------------------- definitions
  function leafFilters(def){ return def.op ? [...leafFilters(def.a), ...leafFilters(def.b)] : [def.filters]; }
  function maskOf(E, def){
    if (!def.op) return E.rowMask(def.filters);
    return E.combineMasks(def.op, maskOf(E, def.a), maskOf(E, def.b));
  }
  function runAssaysOf(def){        // runs to keep: assays named by every branch, or all
    if (!def.op) return def.filters.assay || [];
    const a = runAssaysOf(def.a), b = runAssaysOf(def.b);
    return !a.length || !b.length ? [] : [...new Set([...a, ...b])].sort();
  }
  function member(E, def, s, assays, project, year){
    if (!def.op) return E.sampleMatchesFull(def.filters, s, assays, project, year);
    const a = member(E, def.a, s, assays, project, year), b = member(E, def.b, s, assays, project, year);
    return def.op === "union" ? (a || b) : def.op === "intersection" ? (a && b) : (a && !b);
  }
  function describe(E, def){
    if (!def.op) return X.describeFilters(def.filters, E);
    const sym = { union: "∪", intersection: "∩", difference: "minus" }[def.op];
    return `(${describe(E, def.a)}) ${sym} (${describe(E, def.b)})`;
  }

  // ---------------------------------------------------------------- balancing (same as cohorts.py)
  function mulberry32(a){
    return function(){
      a |= 0; a = a + 0x6D2B79F5 | 0;
      let t = Math.imul(a ^ a >>> 15, 1 | a);
      t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
      return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
  }
  function balance(samples, b){
    if (!b || !(b.max_per_cell || b.max_per_project || b.target)) return samples;
    const order = samples.slice().sort((x, y) => (x.biosample < y.biosample ? -1 : x.biosample > y.biosample ? 1 : 0));
    const rand = mulberry32(b.seed || DEFAULT_SEED);
    for (let i = order.length - 1; i > 0; i -= 1){
      const j = Math.floor(rand() * (i + 1));
      [order[i], order[j]] = [order[j], order[i]];
    }
    const cell = new Map(), proj = new Map(), out = [];
    for (const s of order){
      const key = `${s.place_id || "country:" + (s.country_iso2 || "-")}|${s.habitat || ""}`;
      if (b.max_per_cell && (cell.get(key) || 0) >= b.max_per_cell) continue;
      if (b.max_per_project && (proj.get(s.bioproject) || 0) >= b.max_per_project) continue;
      cell.set(key, (cell.get(key) || 0) + 1);
      proj.set(s.bioproject, (proj.get(s.bioproject) || 0) + 1);
      out.push(s);
      if (b.target && out.length >= b.target) break;
    }
    return out.sort((x, y) => (x.biosample < y.biosample ? -1 : 1));
  }

  // ---------------------------------------------------------------- package
  const META_COLS = ["biosample", "bioproject", "urban_status", "habitat", "country_iso2", "country", "region", "place_id",
                     "city", "lat", "lon", "location_status", "collection_date", "collection_year", "collection_date_raw",
                     "assays", "n_runs"];
  const RUN_COLS = ["run", "biosample", "project", "library_layout", "bases", "spots", "instrument_model", "assay_class",
                    "platform", "library_strategy", "release_date"];
  const tsv = (rows, cols) => [cols.join("\t"), ...rows.map((r) => cols.map((c) => (r[c] ?? "").toString().replace(/[\t\r\n]+/g, " ")).join("\t"))].join("\n") + "\n";

  // Samples and runs of a cohort, by loading the project files it touches.
  async function evaluate(E, cohort, progress){
    const def = cohort.definition;
    const summ = E.summarizeMask(maskOf(E, def));
    const projects = Array.from(summ.projects.keys()).map((i) => E.projectAcc[i]).filter((a) => a);
    const assays = runAssaysOf(def);
    const bySample = new Map(), runs = [];
    const queue = projects.slice();
    let done = 0;
    const failed = [];
    async function worker(){
      while (queue.length){
        const acc = queue.shift();
        try {
          const { samples, runs: prs } = await U.fetchProject(acc);
          const rs = new Map();
          prs.forEach((r) => { const k = r._sample.accession; if (!rs.has(k)) rs.set(k, []); rs.get(k).push(r); });
          for (const s of samples){
            if (!s.accession) continue;
            const sr = rs.get(s.accession) || [];
            const years = sr.map((r) => parseInt((r._run.release_date || "").slice(0, 4), 10)).filter(Boolean);
            if (!member(E, def, s, sr.map((r) => r.assay.assay_class), acc, years.length ? Math.min(...years) : null)) continue;
            const prev = bySample.get(s.accession);
            if (prev){ if (acc < prev.bioproject) prev.bioproject = acc; continue; }
            const raw = s.collection_date || "";
            bySample.set(s.accession, {
              biosample: s.accession, bioproject: acc, urban_status: s.urban_status, habitat: s.urban_context || "",
              country_iso2: s.country_iso2 || "", country: s.country || "", region: s.region || "",
              place_id: s.place_id ?? "", city: s.city || "", lat: s.lat ?? "", lon: s.lon ?? "",
              location_status: s.location_status || "unresolved",
              collection_date: /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : "", collection_year: s.collection_year ?? "",
              collection_date_raw: raw, _runs: sr });
          }
        } catch { failed.push(acc); }
        done += 1;
        if (progress) progress(done, projects.length);
      }
    }
    await Promise.all(Array.from({ length: 6 }, worker));
    let samples = balance(Array.from(bySample.values()), cohort.balance);
    const out = [];
    for (const s of samples){
      const keep = s._runs.filter((r) => !assays.length || assays.includes(r.assay.assay_class));
      if (!keep.length) continue;
      s.n_runs = keep.length;
      s.assays = [...new Set(keep.map((r) => r.assay.assay_class))].sort().join(";");
      keep.forEach((r) => runs.push({ run: r.srr, biosample: s.biosample, project: r.runinfo_row.BioProject,
        library_layout: r._run.library_layout, bases: r._run.bases, spots: r._run.spots, instrument_model: r._run.instrument_model,
        assay_class: r.assay.assay_class, platform: r._run.platform, library_strategy: r._run.library_strategy,
        release_date: r._run.release_date }));
      delete s._runs;
      out.push(s);
    }
    runs.sort((a, b) => (a.biosample + a.run < b.biosample + b.run ? -1 : 1));
    return { samples: out, runs, failed, projects: projects.length };
  }

  function summary(samples, runs){
    const top = (key, n) => Object.fromEntries(Array.from(samples.reduce((m, s) => m.set(s[key] || "(none)", (m.get(s[key] || "(none)") || 0) + 1), new Map()).entries()).sort((a, b) => b[1] - a[1]).slice(0, n || 500));
    const count = (arr, f) => Object.fromEntries(Array.from(arr.reduce((m, x) => m.set(f(x), (m.get(f(x)) || 0) + 1), new Map()).entries()).sort((a, b) => b[1] - a[1]));
    return {
      biosamples: samples.length, runs: runs.length, bioprojects: new Set(samples.map((s) => s.bioproject)).size,
      countries: new Set(samples.map((s) => s.country_iso2).filter(Boolean)).size,
      cities: new Set(samples.map((s) => s.place_id).filter((x) => x !== "")).size,
      gigabases: Math.round(runs.reduce((a, r) => a + (r.bases || 0), 0) / 1e6) / 1e3,
      by_habitat: top("habitat"), by_country: top("country"),
      by_collection_year: Object.fromEntries(Object.entries(count(samples, (s) => String(s.collection_year || "(none)"))).sort()),
      by_assay: count(runs, (r) => r.assay_class), by_layout: count(runs, (r) => r.library_layout || "(none)"),
    };
  }

  async function packageFiles(cohort, samples, runs){
    const m = await U.fetchManifest();
    const release = { database_schema_version: m.database_schema_version, database_built_utc: m.database_built_utc,
                      eligibility_rules_version: m.eligibility_rules_version, geo_rules_version: m.geo_rules_version,
                      query_profiles_version: m.query_profiles_version, site_generated_utc: m.generated_utc };
    const summ = summary(samples, runs);
    const definition = { ...cohort.definition, ...(cohort.balance ? { balance: cohort.balance } : {}) };
    const manifest = {
      umdb_cohort_version: "1.0", name: cohort.name, description: cohort.description || "", created_utc: new Date().toISOString(),
      release, definition,
      counts: { biosamples: summ.biosamples, runs: summ.runs, bioprojects: summ.bioprojects, countries: summ.countries,
                cities: summ.cities, gigabases: summ.gigabases },
      files: ["README.md", "cohort.json", "metadata.tsv", "runs.tsv", "accessions.txt", "download_fastq.sh", "samplesheet.csv",
              "cohort_summary.json", "analysis.py", "analysis.R", "CITATION.bib"],
      built_with: "UMDB website Workspace"
    };
    const sheet = ["sample,run_accession,instrument_platform,fastq_1,fastq_2,fasta"].concat(runs.map((r) => {
      const paired = (r.library_layout || "").toUpperCase() === "PAIRED";
      return `${r.biosample},${r.run},${(r.platform || "ILLUMINA").toUpperCase()},fastq/${r.run}${paired ? "_1" : ""}.fastq.gz,${paired ? `fastq/${r.run}_2.fastq.gz` : ""},`;
    }));
    const T = await templates();
    const habitats = Object.entries(summ.by_habitat).slice(0, 8).map(([k, v]) => `${(k || "unassigned").replace(/_/g, " ")} ${fmt(v)}`).join(", ");
    const readme = T.readme.replace("{NAME}", cohort.name).replace("{SAMPLES}", fmt(summ.biosamples)).replace("{RUNS}", fmt(summ.runs))
      .replace("{GBP}", fmt(summ.gigabases)).replace("{PROJECTS}", fmt(summ.bioprojects)).replace("{COUNTRIES}", summ.countries)
      .replace("{CITIES}", summ.cities).replace("{HABITATS}", habitats).replace("{BUILT}", release.database_built_utc || "?")
      .replace("{RULES}", release.eligibility_rules_version || "?").replace("{BALANCE}", cohort.balance ? JSON.stringify(cohort.balance) : "none");
    const built = (release.database_built_utc || "").slice(0, 10);
    return {
      "README.md": readme,
      "cohort.json": JSON.stringify(manifest, null, 2) + "\n",
      "metadata.tsv": tsv(samples, META_COLS),
      "runs.tsv": tsv(runs, RUN_COLS),
      "accessions.txt": runs.map((r) => r.run).join("\n") + "\n",
      "download_fastq.sh": T.download,
      "samplesheet.csv": sheet.join("\n") + "\n",
      "cohort_summary.json": JSON.stringify(summ, null, 2) + "\n",
      "analysis.py": T.py, "analysis.R": T.r,
      "CITATION.bib": `@misc{umdb,\n  title  = {UMDB: Urban Microbiome Database},\n  author = {Lucaci, Alexander G.},\n  year   = {${built.slice(0, 4) || "2026"}},\n  url    = {https://aglucaci.github.io/UMDB/},\n  note   = {Database built ${built || "unknown"}; eligibility rules ${release.eligibility_rules_version || "?"}}\n}\n`,
    };
  }

  // Package templates live in assets/cohort-templates.json so the Python and browser exports stay in step.
  let tplPromise = null;
  const templates = () => tplPromise || (tplPromise = fetch("./assets/cohort-templates.json").then((r) => r.json()));

  let jszipPromise = null;
  function loadJSZip(){
    if (window.JSZip) return Promise.resolve(window.JSZip);
    return jszipPromise || (jszipPromise = new Promise((resolve, reject) => {
      const s = Object.assign(document.createElement("script"), { src: "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js",
        onload: () => resolve(window.JSZip), onerror: () => { jszipPromise = null; reject(new Error("could not load JSZip")); } });
      document.head.appendChild(s);
    }));
  }

  // Build and download <slug>.zip; `ui` receives progress text.
  async function exportPackage(E, cohort, ui){
    ui("Loading the studies in this cohort…");
    const { samples, runs, failed, projects } = await evaluate(E, cohort, (d, n) => ui(`Loading studies: ${fmt(d)} of ${fmt(n)}`));
    ui("Writing the package…");
    const files = await packageFiles(cohort, samples, runs);
    const JSZip = await loadJSZip();
    const zip = new JSZip();
    const slug = (cohort.name || "cohort").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "cohort";
    const dir = zip.folder(`umdb-${slug}`);
    Object.entries(files).forEach(([n, text]) => dir.file(n, text, n.endsWith(".sh") ? { unixPermissions: "755" } : {}));
    const blob = await zip.generateAsync({ type: "blob", compression: "DEFLATE", platform: "UNIX" });
    X.download(`umdb-${slug}.zip`, blob);
    return { samples: samples.length, runs: runs.length, projects, failed };
  }

  // ---------------------------------------------------------------- comparison
  // Standardized difference of proportions; |d| > 0.1 is a common imbalance threshold.
  function smd(pa, pb){
    const v = (pa * (1 - pa) + pb * (1 - pb)) / 2;
    if (v > 0) return (pa - pb) / Math.sqrt(v);
    return pa === pb ? 0 : (pa > pb ? Infinity : -Infinity);   // 100% vs 0%: complete separation
  }
  function compareTable(title, ma, mb, na, nb, label){
    const keys = [...new Set([...ma.keys(), ...mb.keys()])];
    const rows = keys.map((k) => {
      const a = ma.get(k) || 0, b = mb.get(k) || 0;
      const pa = na ? a / na : 0, pb = nb ? b / nb : 0;
      return { k, a, b, pa, pb, d: smd(pa, pb) };
    }).sort((x, y) => (y.a + y.b) - (x.a + x.b)).slice(0, 12);
    const flagged = rows.filter((r) => Math.abs(r.d) > 0.1).length;
    return { flagged, csv: rows.map((r) => ({ level: label(r.k), a: r.a, b: r.b, share_a: r.pa.toFixed(4), share_b: r.pb.toFixed(4), smd: Number.isFinite(r.d) ? r.d.toFixed(3) : String(r.d) })),
      html: `<div class="panel"><div class="chart-head"><h3>${esc(title)}</h3>${flagged ? `<span class="badge warn">${flagged} imbalanced</span>` : '<span class="badge verified">balanced</span>'}</div>
      <table class="data-table compact"><thead><tr><th></th><th class="num">A</th><th class="num">B</th><th class="num">Std. diff.</th></tr></thead><tbody>
      ${rows.map((r) => `<tr><td>${esc(label(r.k))}</td><td class="num">${(100 * r.pa).toFixed(1)}%</td><td class="num">${(100 * r.pb).toFixed(1)}%</td>
        <td class="num">${Math.abs(r.d) > 0.1 ? `<span class="badge warn">${Number.isFinite(r.d) ? r.d.toFixed(2) : (r.d > 0 ? "+∞" : "−∞")}</span>` : r.d.toFixed(2)}</td></tr>`).join("")}</tbody></table></div>` };
  }

  // ---------------------------------------------------------------- page
  async function page(){
    const root = document.getElementById("workspace");
    if (!root) return;
    const E = await U.openIndex();
    let ws = X.loadWorkspace();
    const selected = new Set();
    const save = () => { if (!X.saveWorkspace(ws)) alert("This browser blocks local storage; cohorts will not persist."); };
    const stats = (c) => {
      const mask = maskOf(E, c.definition);
      const summ = E.summarizeMask(mask);
      const q = c.definition.op ? null : E.query(c.definition.filters);
      return { mask, summ, runs: q ? q.runs : null, gbases: q ? q.gbases : null };
    };

    function examples(){
      const f1 = { ...U.emptyFilters(), context: ["municipal_wastewater"], assay: ["WGS"], cmin: 2020, cmax: 2025, loc: "place" };
      const f2 = { ...U.emptyFilters(), context: ["air_dust"], assay: ["WGS"], cmin: 2020, cmax: 2025, loc: "place" };
      X.addCohort("Wastewater shotgun metagenomes, 2020–2025", { filters: f1 }, { balance: { max_per_cell: 25, max_per_project: 200, target: 0, seed: DEFAULT_SEED } });
      X.addCohort("Air and dust shotgun metagenomes, 2020–2025", { filters: f2 });
      ws = X.loadWorkspace(); render();
    }

    function render(){
      const cards = ws.cohorts.map((c) => {
        const st = stats(c);
        return `<article class="panel" id="${esc(c.id)}" style="margin-bottom:12px">
          <div class="chart-head"><label class="f-check" style="gap:10px;display:flex;align-items:center"><input type="checkbox" data-select="${esc(c.id)}"${selected.has(c.id) ? " checked" : ""}>
            <strong style="font-size:16px">${esc(c.name)}</strong></label>
            <div class="btn-row">${c.definition.op ? "" : `<a class="btn small" href="${X.href.explorer(c.definition.filters)}">Explorer</a><a class="btn small" href="${X.href.atlas(c.definition.filters)}">Atlas</a>`}
              <button type="button" class="btn small" data-act="rename" data-id="${esc(c.id)}">Rename</button>
              <button type="button" class="btn small" data-act="json" data-id="${esc(c.id)}">cohort.json</button>
              <button type="button" class="btn small" data-act="delete" data-id="${esc(c.id)}">Delete</button></div></div>
          <div class="muted" style="font-size:13px">${esc(describe(E, c.definition))}</div>
          <div class="kv"><div><span class="k">BioSamples</span>${fmt(st.summ.samples)}</div>
            <div><span class="k">Studies</span>${fmt(st.summ.projects.size)}</div>
            <div><span class="k">Runs</span>${st.runs == null ? '<span class="muted">at export</span>' : fmt(st.runs)}</div>
            <div><span class="k">Countries</span>${fmt([...st.summ.country.keys()].filter(Boolean).length)}</div>
            <div><span class="k">Cities</span>${fmt(st.summ.place.size)}</div></div>
          <details style="margin-top:10px"><summary class="muted" style="cursor:pointer;font-size:13px">Balancing ${c.balance && (c.balance.max_per_cell || c.balance.max_per_project || c.balance.target) ? `<span class="badge verified">on</span>` : ""}</summary>
            <div class="btn-row" style="margin-top:8px">
              <label class="field">Max per city × habitat<input type="number" min="0" data-bal="max_per_cell" data-id="${esc(c.id)}" value="${c.balance?.max_per_cell || ""}" placeholder="no cap"></label>
              <label class="field">Max per study<input type="number" min="0" data-bal="max_per_project" data-id="${esc(c.id)}" value="${c.balance?.max_per_project || ""}" placeholder="no cap"></label>
              <label class="field">Target size<input type="number" min="0" data-bal="target" data-id="${esc(c.id)}" value="${c.balance?.target || ""}" placeholder="all"></label>
              <label class="field">Random seed<input type="number" data-bal="seed" data-id="${esc(c.id)}" value="${c.balance?.seed || DEFAULT_SEED}"></label>
            </div><div class="sub">A seeded shuffle, then samples are kept while the caps allow. The seed is saved in cohort.json; the Python exporter gives the same samples.</div></details>
          <div class="btn-row" style="margin-top:12px"><button type="button" class="btn primary" data-act="export" data-id="${esc(c.id)}">Download research package (.zip)</button>
            <span class="muted" style="font-size:12px" data-status="${esc(c.id)}">${st.summ.projects.size > 400 ? `Large cohort (${fmt(st.summ.projects.size)} studies): the browser export loads each study; <code>umdb cohort export cohort.json</code> is faster.` : ""}</span></div>
        </article>`;
      }).join("");
      const ids = ws.cohorts.map((c) => `<option value="${esc(c.id)}">${esc(c.name)}</option>`).join("");
      root.innerHTML = `
        <div class="grid-2">
          <div class="panel"><h2>Combine cohorts</h2>
            <div class="btn-row"><label class="field">A<select id="op-a">${ids}</select></label>
              <label class="field">Operation<select id="op"><option value="union">A ∪ B (union)</option><option value="intersection">A ∩ B (intersection)</option><option value="difference">A minus B</option></select></label>
              <label class="field">B<select id="op-b">${ids}</select></label>
              <button type="button" class="btn" id="op-go" style="align-self:flex-end"${ws.cohorts.length < 2 ? " disabled" : ""}>Create</button></div>
            <div class="sub">Exact at the BioSample level: each sample is in one row of the catalog index.</div></div>
          <div class="panel"><h2>Add a cohort</h2>
            <div class="btn-row"><a class="btn primary" href="./explorer.html">Build one in the Explorer</a><a class="btn" href="./collections.html">Start from a collection</a>
              <label class="btn" style="cursor:pointer">Import cohort.json<input type="file" id="import" accept=".json,application/json" hidden></label>
              ${ws.cohorts.length ? "" : '<button type="button" class="btn" id="examples">Add two example cohorts</button>'}</div>
            <div class="sub">Cohorts are saved in this browser only. Download cohort.json to keep or share one.</div></div>
        </div>
        <div class="btn-row" style="margin:18px 0 10px"><h2 style="margin:0;font-size:18px">Your cohorts (${ws.cohorts.length})</h2>
          <button type="button" class="btn" id="compare"${selected.size === 2 ? "" : " disabled"}>Compare the two selected</button></div>
        ${cards || '<div class="panel empty">No cohorts yet. In the Explorer, choose filters and press <strong>Save to Workspace</strong>.</div>'}
        <div id="compare-out"></div>`;
    }

    function compare(){
      const [a, b] = ws.cohorts.filter((c) => selected.has(c.id));
      const sa = stats(a), sb = stats(b);
      const overlap = E.summarizeMask(E.combineMasks("intersection", sa.mask, sb.mask)).samples;
      const na = sa.summ.samples, nb = sb.summ.samples;
      const t = [
        compareTable("Habitat", sa.summ.habitat, sb.summ.habitat, na, nb, (k) => X.CTX_LABEL[k] || k),
        compareTable("Continent", sa.summ.continent, sb.summ.continent, na, nb, (k) => X.CONTINENT[k] || k),
        compareTable("Assay (samples with any run of it)", sa.summ.assay, sb.summ.assay, na, nb, (k) => k),
        compareTable("Urban eligibility", sa.summ.tier, sb.summ.tier, na, nb, (k) => X.TIER_LABEL[k] || k),
        compareTable("Location quality", sa.summ.loc, sb.summ.loc, na, nb, (k) => X.LOC_LABEL[k] || k),
        compareTable("Collection year", sa.summ.cyear, sb.summ.cyear, na, nb, (k) => k || "no year"),
      ];
      const flagged = t.reduce((n, x) => n + x.flagged, 0);
      document.getElementById("compare-out").innerHTML = `<div class="panel" style="margin-top:18px">
        <div class="chart-head"><h2>Compare: A = ${esc(a.name)} · B = ${esc(b.name)}</h2><button type="button" class="btn small" id="cmp-csv">Download comparison (CSV)</button></div>
        <div class="kv"><div class="compare-col"><span class="k">A BioSamples</span>${fmt(na)}</div><div class="compare-col b"><span class="k">B BioSamples</span>${fmt(nb)}</div>
          <div><span class="k">In both</span>${fmt(overlap)}</div><div><span class="k">Studies A / B</span>${fmt(sa.summ.projects.size)} / ${fmt(sb.summ.projects.size)}</div>
          <div><span class="k">Shared studies</span>${fmt([...sa.summ.projects.keys()].filter((p) => sb.summ.projects.has(p)).length)}</div></div>
        <p class="notice" style="margin-top:12px">${flagged ? `${flagged} levels differ by a standardized difference above 0.1. A difference between A and B may reflect these, not the variable you are studying: balance, stratify or adjust for them.` : "No level differs by a standardized difference above 0.1 in these variables."} Study (BioProject) is the strongest batch variable in public data; ${overlap ? `${fmt(overlap)} samples are in both cohorts.` : "the cohorts share no samples."}</p></div>
        <div class="grid-3" style="margin-top:12px">${t.map((x) => x.html).join("")}</div>`;
      document.getElementById("cmp-csv").onclick = () => {
        const names = ["habitat", "continent", "assay", "tier", "location_quality", "collection_year"];
        const rows = t.flatMap((x, i) => x.csv.map((r) => ({ variable: names[i], ...r })));
        X.download("umdb_cohort_comparison.csv", X.toCSV(rows, ["variable", "level", "a", "b", "share_a", "share_b", "smd"]), "text/csv");
      };
      document.getElementById("compare-out").scrollIntoView({ behavior: "smooth" });
    }

    root.addEventListener("change", (e) => {
      const t = e.target;
      if (t.dataset.select){ t.checked ? selected.add(t.dataset.select) : selected.delete(t.dataset.select);
        if (selected.size > 2){ selected.delete([...selected][0]); } render(); return; }
      if (t.dataset.bal){
        const c = ws.cohorts.find((x) => x.id === t.dataset.id);
        c.balance = { max_per_cell: 0, max_per_project: 0, target: 0, seed: DEFAULT_SEED, ...(c.balance || {}) };
        c.balance[t.dataset.bal] = t.value ? parseInt(t.value, 10) : (t.dataset.bal === "seed" ? DEFAULT_SEED : 0);
        save(); return;
      }
      if (t.id === "import" && t.files[0]){
        t.files[0].text().then((txt) => {
          const doc = JSON.parse(txt);
          const d = doc.definition || doc;
          const def = d.op ? { op: d.op, a: d.a, b: d.b } : { filters: { ...U.emptyFilters(), ...(d.filters || {}) } };
          X.addCohort(doc.name || "Imported cohort", def, d.balance ? { balance: d.balance } : {});
          ws = X.loadWorkspace(); render();
        }).catch((err) => alert(`Not a cohort file: ${err.message}`));
      }
    });
    root.addEventListener("click", async (e) => {
      const b = e.target.closest("button");
      if (!b) return;
      if (b.id === "examples") return examples();
      if (b.id === "compare") return compare();
      if (b.id === "op-go"){
        const a = ws.cohorts.find((c) => c.id === document.getElementById("op-a").value);
        const bb = ws.cohorts.find((c) => c.id === document.getElementById("op-b").value);
        const op = document.getElementById("op").value;
        if (!a || !bb || a === bb) return alert("Choose two different cohorts.");
        X.addCohort(`${a.name} ${{ union: "∪", intersection: "∩", difference: "minus" }[op]} ${bb.name}`, { op, a: a.definition, b: bb.definition });
        ws = X.loadWorkspace(); return render();
      }
      const c = ws.cohorts.find((x) => x.id === b.dataset.id);
      if (!c) return;
      if (b.dataset.act === "delete"){ if (confirm(`Delete “${c.name}” from this browser?`)){ ws.cohorts = ws.cohorts.filter((x) => x !== c); selected.delete(c.id); save(); render(); } }
      if (b.dataset.act === "rename"){ const n = prompt("Cohort name", c.name); if (n){ c.name = n.trim(); save(); render(); } }
      if (b.dataset.act === "json"){
        const doc = { name: c.name, definition: { ...c.definition, ...(c.balance ? { balance: c.balance } : {}) } };
        X.download(`${c.name.toLowerCase().replace(/[^a-z0-9]+/g, "-")}.cohort.json`, JSON.stringify(doc, null, 2), "application/json");
      }
      if (b.dataset.act === "export"){
        const status = root.querySelector(`[data-status="${c.id}"]`);
        b.disabled = true;
        try {
          const r = await exportPackage(E, c, (msg) => { status.textContent = msg; });
          status.textContent = `Downloaded: ${fmt(r.samples)} BioSamples, ${fmt(r.runs)} runs${r.failed.length ? `; ${r.failed.length} studies could not be loaded` : ""}.`;
        } catch (err) { status.textContent = `Export failed: ${err.message}`; }
        b.disabled = false;
      }
    });
    render();
    if (location.hash) document.getElementById(location.hash.slice(1))?.scrollIntoView();
  }

  window.UMDBWorkspace = { exportPackage, evaluate, balance, mulberry32, maskOf, describe };
  page().catch((err) => {
    const root = document.getElementById("workspace");
    if (root) root.innerHTML = `<div class="panel empty"><strong>The workspace could not load.</strong> ${esc(err.message)}</div>`;
  });
})();
