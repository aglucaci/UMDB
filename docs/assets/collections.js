(function(){
  // Curated research collections: collections.html lists them, collections.html?slug=… shows one.
  // Definitions, statistics and run lists are built by scripts/umdb_harvester/cohorts.py.
  const U = window.UMDB, X = window.UMDBUI;
  const { esc, fmt, href } = X;
  const root = document.getElementById("collections");

  const top = (obj, label) => Object.entries(obj || {}).map(([k, v]) => ({ label: label ? label(k) : k, value: v, key: k }));

  async function list(doc){
    root.innerHTML = `<div class="product-grid">${doc.collections.map((c) => `
      <a class="product" href="${href.collection(c.slug)}"><strong>${esc(c.title)}</strong><span>${esc(c.subtitle)}</span>
        <div class="kv" style="margin-top:10px"><div><span class="k">BioSamples</span>${fmt(c.summary.biosamples)}</div>
        <div><span class="k">Countries</span>${fmt(c.summary.countries)}</div><div><span class="k">Studies</span>${fmt(c.summary.bioprojects)}</div></div>
        <span class="tag">Open collection →</span></a>`).join("")}</div>
      <p class="sub" style="margin-top:16px">Collections are recomputed from each catalog build (database built ${esc(doc.release.database_built_utc || "?")}, eligibility rules ${esc(doc.release.eligibility_rules_version || "?")}). Cite the release you used.</p>`;
  }

  async function detail(doc, slug){
    const c = doc.collections.find((x) => x.slug === slug);
    if (!c){ root.innerHTML = `<div class="panel empty">No collection called “${esc(slug)}”. <a href="./collections.html">All collections</a></div>`; return; }
    document.title = `UMDB — ${c.title}`;
    document.getElementById("page-title").textContent = c.title;
    document.getElementById("page-lead").textContent = c.subtitle;
    const s = c.summary;
    root.innerHTML = `
      <div class="crumbs"><a href="./collections.html">Collections</a> › ${esc(c.title)}</div>
      <div class="kv"><div><span class="k">BioSamples</span>${fmt(s.biosamples)}</div><div><span class="k">Runs</span>${fmt(s.runs)}</div>
        <div><span class="k">Studies</span>${fmt(s.bioprojects)}</div><div><span class="k">Countries</span>${fmt(s.countries)}</div>
        <div><span class="k">Cities</span>${fmt(s.cities)}</div><div><span class="k">Sequence</span>${fmt(Math.round(s.gigabases))} Gbp</div></div>
      <div class="btn-row" style="margin:14px 0">
        <a class="btn primary" href="./db/${esc(c.runs_file)}" download>Run list (TSV.gz, ${(c.runs_file_bytes / 1e6).toFixed(1)} MB)</a>
        <button type="button" class="btn" id="pkg">Research package (.zip)</button>
        <a class="btn" href="${href.explorer(c.filters)}">Open in Explorer</a>
        <a class="btn" href="${href.atlas(c.filters)}">Show on Atlas</a>
        <button type="button" class="btn" id="save">Save to Workspace</button>
        <span class="muted" id="pkg-status" style="font-size:12px"></span>
      </div>
      <div class="grid-2">
        <div class="panel"><h2>Inclusion criteria</h2><ul style="margin:0;padding-left:18px;font-size:14px">${c.inclusion.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>
          <h3>Suggested use</h3><p style="font-size:14px;margin:0">${esc(c.use)}</p>
          <h3>Recommended workflow</h3><p style="font-size:14px;margin:0">${esc(c.workflow)}</p></div>
        <div class="panel"><h2>Reproduce it</h2>
          <pre class="mono" style="white-space:pre-wrap;font-size:12.5px;background:rgba(0,0,0,.25);padding:12px;border-radius:12px;margin:0">umdb cohort collection ${esc(c.slug)} -o ${esc(c.slug)}/

# balanced: at most 25 samples per city and habitat, 200 per study
umdb cohort collection ${esc(c.slug)} --max-per-cell 25 --max-per-project 200 --seed 20261119

# reprocess the shotgun runs with the UMDB-SCIENCE pipeline
nextflow run pipeline --cohort ${esc(c.slug)}/runs.tsv --run_reads …</pre>
          <p class="sub">Definition (Explorer filters): <code>${esc(X.describeFilters(c.filters))}</code></p></div>
      </div>
      <div class="grid-2" style="margin-top:16px"><div class="panel" id="c-hab"></div><div class="panel" id="c-ctry"></div></div>
      <div class="grid-2" style="margin-top:16px"><div class="panel" id="c-year"></div><div class="panel" id="c-assay"></div></div>
      <p class="sub" style="margin-top:16px">Collection built from the catalog of ${esc(doc.release.database_built_utc || "?")}. Cite UMDB and name the collection (<span class="mono">${esc(c.slug)}</span>) and the database build date.</p>`;
    X.bars(document.getElementById("c-hab"), "Habitats", top(s.by_habitat, (k) => X.CTX_LABEL[k] || k).map((d) => ({ ...d, href: d.key && d.key !== "(none)" ? href.habitat(d.key) : null })), { unit: "samples", file: `${c.slug}_habitats` });
    X.bars(document.getElementById("c-ctry"), "Countries", top(s.by_country), { unit: "samples", limit: 15, file: `${c.slug}_countries` });
    X.columns(document.getElementById("c-year"), "BioSamples by collection year", X.yearItems(new Map(Object.entries(s.by_collection_year))), { file: `${c.slug}_collection_years`,
      note: s.by_collection_year["(none)"] ? `${fmt(s.by_collection_year["(none)"])} samples have no collection year.` : "" });
    X.bars(document.getElementById("c-assay"), "Assays", top(s.by_assay), { unit: "runs", file: `${c.slug}_assays` });
    document.getElementById("save").onclick = (e) => {
      const id = X.addCohort(`Collection: ${c.title}`, { filters: c.filters });
      e.target.textContent = id ? "Saved to Workspace" : "Browser storage blocked";
      e.target.disabled = true;
    };
    document.getElementById("pkg").onclick = async (e) => {
      const st = document.getElementById("pkg-status");
      if (c.summary.bioprojects > 400 && !confirm(`This collection spans ${fmt(c.summary.bioprojects)} studies; building the package in the browser loads each of them. The command line (umdb cohort collection ${c.slug}) is faster. Continue?`)) return;
      e.target.disabled = true;
      try {
        const E = await U.openIndex();
        const r = await window.UMDBWorkspace.exportPackage(E, { name: `UMDB collection: ${c.title}`, description: c.subtitle, definition: { filters: c.filters } }, (m) => { st.textContent = m; });
        st.textContent = `Downloaded: ${fmt(r.samples)} BioSamples, ${fmt(r.runs)} runs.`;
      } catch (err) { st.textContent = `Failed: ${err.message}`; }
      e.target.disabled = false;
    };
  }

  U.fetchCollections().then((doc) => {
    const slug = X.param("slug");
    return slug ? detail(doc, slug) : list(doc);
  }).catch((err) => { root.innerHTML = `<div class="panel empty"><strong>Collections could not be loaded.</strong> ${esc(err.message)}</div>`; });
})();
