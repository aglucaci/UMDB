(function(){
  // Persistent, shareable record pages. <body data-entity="study|sample|run|city|country|habitat">
  //   study.html?acc=PRJNA…   sample.html?acc=SAMN…   run.html?acc=SRR…
  //   city.html?id=<GeoNames id>   country.html?iso=US   habitat.html?id=municipal-wastewater
  // Each shows UMDB's normalized annotation next to the submitted metadata, the curation
  // evidence and quality flags, links to the source archives, and downloads.
  const U = window.UMDB, X = window.UMDBUI;
  const { esc, fmt, href, TIER_LABEL, CTX_LABEL, CONTINENT, LOC_LABEL } = X;
  const $ = (id) => document.getElementById(id);
  const root = $("entity");
  const kind = document.body.dataset.entity;

  const fail = (msg) => { root.innerHTML = `<div class="panel empty"><strong>${esc(msg)}</strong><p><a href="./explorer.html">Open the Explorer</a> to search the catalog.</p></div>`; };
  const tierBadge = (t) => `<span class="badge ${esc(t || "unscreened")}">${esc(TIER_LABEL[t] || t || "Unscreened")}</span>`;
  const locBadge = (st) => `<span class="badge ${esc(st || "unresolved")}">${esc(LOC_LABEL[st] || st || "unresolved")}</span>`;
  const ctxLink = (c) => c ? `<a href="${href.habitat(c)}">${esc(CTX_LABEL[c] || c)}</a>` : `<span class="muted">No urban context</span>`;
  const kv = (pairs) => `<div class="kv">${pairs.filter(Boolean).map(([k, v]) => `<div><span class="k">${esc(k)}</span>${v}</div>`).join("")}</div>`;
  const gbp = (b) => b ? `${(b / 1e9).toFixed(b < 1e10 ? 2 : 1)} Gbp` : "—";
  const setTitle = (t) => { document.title = `UMDB — ${t}`; };
  const count = (arr, fn) => { const m = new Map(); arr.forEach((x) => { const k = fn(x); if (k != null) m.set(k, (m.get(k) || 0) + 1); }); return m; };

  async function provenance(){
    const m = await U.fetchManifest();
    return `<div class="sub">UMDB database built ${esc(m.database_built_utc || "?")} · eligibility rules ${esc(m.eligibility_rules_version || "?")} · location rules ${esc(m.geo_rules_version || "?")} · query profiles ${esc(m.query_profiles_version || "?")}</div>`;
  }

  function saveCohortButton(name, filters){
    return `<button type="button" class="btn" data-save-cohort='${esc(JSON.stringify({ name, filters }))}'>Save as cohort</button>`;
  }
  root.addEventListener("click", (e) => {
    const b = e.target.closest("[data-save-cohort]");
    if (!b) return;
    const { name, filters } = JSON.parse(b.dataset.saveCohort);
    const id = X.addCohort(name, { filters });
    b.textContent = id ? "Saved to Workspace" : "Could not save (browser storage blocked)";
    if (id) b.insertAdjacentHTML("afterend", ` <a class="btn small" href="./workspace.html#${id}">Open Workspace</a>`);
    b.disabled = true;
  });

  // ------------------------------------------------------------------ flags
  function qualityFlags(s, runs, buildDate){
    const flags = [];
    const raw = s.collection_date || "";
    const iso = /^\d{4}-\d{2}-\d{2}/.test(raw) ? raw.slice(0, 10) : "";
    const firstRelease = runs.map((r) => (r._run.release_date || "").slice(0, 10)).filter(Boolean).sort()[0] || "";
    if ((iso && buildDate && iso > buildDate) || (s.collection_year && buildDate && s.collection_year > +buildDate.slice(0, 4)))
      flags.push(["flag", "Collection date is in the future", "Excluded from temporal analyses"]);
    if (iso && firstRelease && iso > firstRelease)
      flags.push(["flag", "Collected after the data were released", `First run released ${firstRelease}`]);
    if (s.collection_year && s.collection_year < 1950) flags.push(["flag", "Implausible collection year", String(s.collection_year)]);
    if (s.location_status === "conflicting") flags.push(["warn", "Coordinates contradict the stated place", s.location_note || ""]);
    if (!s.country_iso2) flags.push(["warn", "No validated country", "Location text could not be matched to ISO 3166"]);
    else if (!s.place_id) flags.push(["warn", "No validated city or town", "Mapped at country level only"]);
    if (!raw) flags.push(["warn", "No collection date", ""]);
    if (s.lat != null && s.lon != null && Number.isInteger(+s.lat) && Number.isInteger(+s.lon))
      flags.push(["warn", "Coordinates given to whole degrees", "Too coarse to confirm or contradict a place"]);
    return flags;
  }

  // ------------------------------------------------------------------ study
  async function study(){
    const acc = X.param("acc").toUpperCase();
    if (!acc) return fail("No BioProject accession given.");
    let data;
    try { data = await U.fetchProject(acc); } catch { return fail(`${acc} is not in UMDB.`); }
    const { project, samples, runs } = data;
    setTitle(`${acc}: ${project.title || "study"}`);
    const real = samples.filter((s) => s.accession);
    const runAssays = count(runs, (r) => r.assay.assay_class);
    const bases = runs.reduce((a, r) => a + (r._run.bases || 0), 0);
    const years = runs.map((r) => parseInt((r._run.release_date || "").slice(0, 4), 10)).filter(Boolean);
    const countries = new Map(), cities = new Map();
    real.forEach((s) => {
      if (s.country_iso2) countries.set(s.country_iso2, (countries.get(s.country_iso2) || 0) + 1);
      if (s.place_id != null) cities.set(s.place_id, { n: (cities.get(s.place_id)?.n || 0) + 1, name: s.city });
    });
    const f = { ...U.emptyFilters(), project: acc, tier: [] };
    root.innerHTML = `
      <div class="crumbs"><a href="./explorer.html">Catalog</a> › Study</div>
      <div class="entity-head"><div class="acc">${esc(acc)}</div><h1>${esc(project.title || "Untitled study")}</h1>
        <div class="muted">${esc(project.center_name || "")}${project.data_type ? " · " + esc(project.data_type) : ""}</div></div>
      ${kv([["BioSamples", fmt(real.length)], ["Runs", fmt(runs.length)], ["Sequence", gbp(bases)],
            ["Released", years.length ? (Math.min(...years) === Math.max(...years) ? String(years[0]) : `${Math.min(...years)}–${Math.max(...years)}`) : "—"],
            ["Countries", [...countries.keys()].map((c) => `<a href="${href.country(c)}">${esc(c)}</a>`).join(", ") || "—"],
            ["Submitted", esc((project.submission_date || "").slice(0, 10) || "—")]])}
      <div class="btn-row" style="margin:14px 0">
        <a class="btn primary" href="${href.explorer(f)}">Open in Explorer</a>
        <a class="btn" href="${href.atlas(f)}">Show on Atlas</a>
        ${saveCohortButton(`Study ${acc}`, f)}
        <button type="button" class="btn" id="dl">Download runs (TSV)</button>
        <a class="btn" href="https://www.ncbi.nlm.nih.gov/bioproject/${encodeURIComponent(acc)}" target="_blank" rel="noopener">NCBI BioProject ↗</a>
        <a class="btn" href="https://www.ebi.ac.uk/ena/browser/view/${encodeURIComponent(acc)}" target="_blank" rel="noopener">ENA ↗</a>
      </div>
      ${project.description ? `<div class="panel"><h2>Study overview</h2><div style="white-space:pre-wrap;font-size:14px;color:#cfd9ea;max-height:16em;overflow:auto">${esc(project.description)}</div></div>` : ""}
      <div class="grid-3" style="margin-top:16px">
        <div class="panel" id="c-habitat"></div><div class="panel" id="c-assay"></div><div class="panel" id="c-tier"></div>
      </div>
      <div class="grid-2" style="margin-top:16px"><div class="panel" id="c-cities"></div><div class="panel" id="c-cyear"></div></div>
      <div class="panel" style="margin-top:16px"><h2>BioSamples (${fmt(real.length)})</h2>
        <div class="table-shell"><table class="data-table"><thead><tr><th>BioSample</th><th>Accepted location</th><th>Collected</th><th>Urban eligibility</th><th>Habitat</th></tr></thead>
        <tbody>${real.slice(0, 300).map((s) => `<tr><td class="mono"><a href="${href.sample(s.accession)}">${esc(s.accession)}</a><span class="sub">${esc(s.title || "")}</span></td>
          <td>${s.place_id != null ? `<a href="${href.city(s.place_id)}">${esc(s.city)}</a>, ` : ""}${s.country_iso2 ? `<a href="${href.country(s.country_iso2)}">${esc(s.country || s.country_iso2)}</a>` : '<span class="muted">—</span>'} ${locBadge(s.location_status)}</td>
          <td>${esc(s.collection_date || "")}</td><td>${tierBadge(s.urban_status)}</td><td>${ctxLink(s.urban_context)}</td></tr>`).join("")}</tbody></table></div>
        ${real.length > 300 ? `<div class="sub">Showing 300 of ${fmt(real.length)}; the download has every run and sample.</div>` : ""}</div>
      <div class="panel" style="margin-top:16px"><h2>Sequencing runs (${fmt(runs.length)})</h2>
        <div class="table-shell"><table class="data-table"><thead><tr><th>Run</th><th>BioSample</th><th>Assay</th><th>Library</th><th>Instrument</th><th class="num">Gbp</th></tr></thead>
        <tbody>${runs.slice(0, 300).map((r) => `<tr><td class="mono"><a href="${href.run(r.srr)}">${esc(r.srr)}</a></td><td class="mono">${r._sample.accession ? `<a href="${href.sample(r._sample.accession)}">${esc(r._sample.accession)}</a>` : ""}</td>
          <td>${esc(r.assay.assay_class)}</td><td>${esc([r._run.library_strategy, r._run.library_layout].filter(Boolean).join(" · "))}</td>
          <td>${esc(r._run.instrument_model || r._run.platform || "")}</td><td class="num">${r._run.bases ? (r._run.bases / 1e9).toFixed(2) : "—"}</td></tr>`).join("")}</tbody></table></div>
        ${runs.length > 300 ? `<div class="sub">Showing 300 of ${fmt(runs.length)}.</div>` : ""}</div>
      <div class="panel" style="margin-top:16px"><h2>Provenance</h2>
        <p style="font-size:14px">Title, description and centre come from NCBI BioProject; sample and run fields from BioSample and SRA RunInfo. Urban eligibility, habitat and validated locations are UMDB annotations (<a href="./methodology.html">methods</a>, <a href="https://github.com/aglucaci/UMDB/blob/main/INCLUSION_CRITERIA.md">inclusion criteria</a>).</p>
        ${await provenance()}</div>`;
    X.bars($("c-habitat"), "Habitat", X.mapItems(count(real, (s) => s.urban_context || ""), (k) => CTX_LABEL[k] || k, (k) => k ? href.habitat(k) : null), { unit: "samples", file: `${acc}_habitats` });
    X.bars($("c-assay"), "Assay", X.mapItems(runAssays), { unit: "runs", file: `${acc}_assays` });
    X.bars($("c-tier"), "Urban eligibility", X.mapItems(count(real, (s) => s.urban_status), (k) => TIER_LABEL[k] || k), { unit: "samples", file: `${acc}_tiers` });
    X.bars($("c-cities"), "Cities and towns", Array.from(cities.entries()).map(([id, v]) => ({ label: v.name, value: v.n, href: href.city(id) })).sort((a, b) => b.value - a.value), { unit: "samples", file: `${acc}_cities` });
    X.columns($("c-cyear"), "BioSamples by collection year", X.yearItems(count(real, (s) => s.collection_year == null ? null : String(s.collection_year))), { file: `${acc}_collection_years` });
    $("dl").onclick = () => X.download(`${acc}_runs.tsv`, runsTSV(runs, acc), "text/tab-separated-values");
  }

  function runsTSV(runs, acc){
    const cols = ["run", "biosample", "bioproject", "assay_class", "library_strategy", "library_layout", "platform", "instrument_model",
                  "release_date", "bases", "country_iso2", "city", "geonames_id", "location_status", "collection_date", "urban_status", "habitat"];
    const clean = (v) => (v ?? "").toString().replace(/[\t\r\n]+/g, " ");
    return [cols.join("\t"), ...runs.map((r) => { const s = r._sample;
      return [r.srr, s.accession, acc, r.assay.assay_class, r._run.library_strategy, r._run.library_layout, r._run.platform, r._run.instrument_model,
              r._run.release_date, r._run.bases, s.country_iso2, s.city, s.place_id, s.location_status, s.collection_date, s.urban_status, s.urban_context].map(clean).join("\t"); })].join("\n") + "\n";
  }

  // ------------------------------------------------------------------ sample / run
  async function loadByAccession(acc){
    const hit = await U.lookupAccession(acc);
    if (!hit) return null;
    const data = await U.fetchProject(hit.project);
    return { ...hit, data };
  }

  async function sample(){
    const acc = X.param("acc").toUpperCase();
    if (!acc) return fail("No BioSample accession given.");
    const hit = await loadByAccession(acc).catch(() => null);
    if (!hit) return fail(`${acc} is not in UMDB.`);
    const { project, samples, runs } = hit.data;
    const s = samples.find((x) => x.accession === acc);
    if (!s) return fail(`${acc} is not in UMDB.`);
    const mine = runs.filter((r) => r._sample.accession === acc);
    const m = await U.fetchManifest();
    const flags = qualityFlags(s, mine, (m.database_built_utc || "").slice(0, 10));
    setTitle(`${acc}${s.title ? ": " + s.title : ""}`);
    const row = (label, submitted, normalized, note) => `<tr><th>${esc(label)}</th><td>${submitted || '<span class="muted">—</span>'}</td><td>${normalized || '<span class="muted">—</span>'}</td><td class="muted">${note || ""}</td></tr>`;
    root.innerHTML = `
      <div class="crumbs"><a href="./explorer.html">Catalog</a> › <a href="${href.study(project.accession)}">${esc(project.accession)}</a> › BioSample</div>
      <div class="entity-head"><div class="acc">${esc(acc)}</div><h1>${esc(s.title || "BioSample")}</h1>
        <div>${tierBadge(s.urban_status)} ${s.urban_context ? `<span class="badge">${esc(CTX_LABEL[s.urban_context] || s.urban_context)}</span>` : ""} ${locBadge(s.location_status)}</div></div>
      ${kv([["Study", `<a href="${href.study(project.accession)}">${esc(project.accession)}</a><span class="sub">${esc(project.title || "")}</span>`],
            ["Runs", fmt(mine.length)], ["Sequence", gbp(mine.reduce((a, r) => a + (r._run.bases || 0), 0))],
            ["Location", s.place_id != null ? `<a href="${href.city(s.place_id)}">${esc(s.city)}</a>, ${esc(s.country)}` : s.country_iso2 ? `<a href="${href.country(s.country_iso2)}">${esc(s.country)}</a>` : "—"],
            ["Collected", esc(s.collection_date || "—")]])}
      <div class="btn-row" style="margin:14px 0">
        <a class="btn" href="https://www.ncbi.nlm.nih.gov/biosample/${encodeURIComponent(acc)}" target="_blank" rel="noopener">NCBI BioSample ↗</a>
        <a class="btn" href="https://www.ebi.ac.uk/ena/browser/view/${encodeURIComponent(acc)}" target="_blank" rel="noopener">ENA ↗</a>
        <a class="btn" href="https://github.com/aglucaci/UMDB/issues/new?template=metadata_correction.yml&title=${encodeURIComponent("Correction: " + acc)}" target="_blank" rel="noopener">Report a correction ↗</a>
      </div>
      ${flags.length ? `<div class="panel"><h2>Quality flags</h2>${flags.map(([k, t, d]) => `<div style="margin:4px 0"><span class="badge ${k}">${esc(t)}</span> <span class="muted" style="font-size:13px">${esc(d)}</span></div>`).join("")}</div>` : ""}
      <div class="panel" style="margin-top:16px"><h2>Submitted and normalized metadata</h2>
        <div class="table-shell"><table class="data-table"><thead><tr><th>Field</th><th>Submitted</th><th>UMDB</th><th>Evidence</th></tr></thead><tbody>
        ${row("Location", esc(s.geo_loc_raw), [s.city, s.region, s.country].filter(Boolean).map(esc).join(", ") + (s.country_iso2 ? ` <span class="mono muted">${esc(s.country_iso2)}${s.place_id != null ? " · GeoNames " + esc(s.place_id) : ""}</span>` : ""),
              `${esc(LOC_LABEL[s.location_status] || s.location_status)}${s.location_method ? " by " + esc(s.location_method) : ""}${s.location_note ? ". " + esc(s.location_note) : ""}`)}
        ${row("Coordinates", s.lat != null ? `${esc(s.lat)}, ${esc(s.lon)}` : "", s.lat != null ? "Reported point (shown on the Atlas)" : "City centroid only, if a place is accepted", "Reported coordinates are never moved; a place is a GeoNames settlement")}
        ${row("Collection date", esc(s.collection_date), s.collection_year != null ? esc(s.collection_year) : "", "Full ISO dates only become collection_date; partial dates keep the year")}
        ${row("Urban eligibility", "", tierBadge(s.urban_status), esc(s.eligibility_reason || ""))}
        ${row("Habitat (urban context)", "", ctxLink(s.urban_context), "Primary context assigned by the eligibility screen")}
        ${row("Isolation source", esc(s.isolation_source), "", "")}
        ${row("Broad-scale environment", esc(s.env_broad_scale), "", "MIxS env_broad_scale")}
        ${row("Local environment", esc(s.env_local_scale), "", "MIxS env_local_scale")}
        ${row("Environmental medium", esc(s.env_medium), "", "MIxS env_medium")}
        ${row("Host", esc(s.host), "", "")}
        </tbody></table></div></div>
      <div class="panel" style="margin-top:16px"><h2>Sequencing runs</h2>
        <div class="table-shell"><table class="data-table"><thead><tr><th>Run</th><th>Assay</th><th>Library</th><th>Instrument</th><th>Released</th><th class="num">Gbp</th></tr></thead>
        <tbody>${mine.map((r) => `<tr><td class="mono"><a href="${href.run(r.srr)}">${esc(r.srr)}</a></td><td>${esc(r.assay.assay_class)}</td>
          <td>${esc([r._run.library_strategy, r._run.library_source, r._run.library_layout].filter(Boolean).join(" · "))}</td><td>${esc(r._run.instrument_model || "")}</td>
          <td>${esc((r._run.release_date || "").slice(0, 10))}</td><td class="num">${r._run.bases ? (r._run.bases / 1e9).toFixed(2) : "—"}</td></tr>`).join("")}</tbody></table></div></div>
      <div class="panel" style="margin-top:16px"><h2>Provenance</h2><p style="font-size:14px">Submitted values are copied from NCBI BioSample. UMDB values are rule-based annotations; the evidence column says which rule decided them.</p>${await provenance()}</div>`;
  }

  async function run(){
    const acc = X.param("acc").toUpperCase();
    if (!acc) return fail("No run accession given.");
    const hit = await loadByAccession(acc).catch(() => null);
    const r = hit && hit.data.runs.find((x) => x.srr === acc);
    if (!r) return fail(`${acc} is not in UMDB.`);
    const { project } = hit.data;
    const s = r._sample, x = r._run;
    setTitle(acc);
    root.innerHTML = `
      <div class="crumbs"><a href="./explorer.html">Catalog</a> › <a href="${href.study(project.accession)}">${esc(project.accession)}</a> › ${s.accession ? `<a href="${href.sample(s.accession)}">${esc(s.accession)}</a> › ` : ""}Run</div>
      <div class="entity-head"><div class="acc">${esc(acc)}</div><h1>${esc(x.experiment_title || r.title || "Sequencing run")}</h1>
        <div>${tierBadge(x.urban_status)} <span class="badge">${esc(r.assay.assay_class)}</span></div></div>
      ${kv([["Study", `<a href="${href.study(project.accession)}">${esc(project.accession)}</a>`], ["BioSample", s.accession ? `<a href="${href.sample(s.accession)}">${esc(s.accession)}</a>` : "—"],
            ["Experiment", esc(x.experiment || "—")], ["Library", esc([x.library_strategy, x.library_source, x.library_selection, x.library_layout].filter(Boolean).join(" · ") || "—")],
            ["Instrument", esc([x.platform, x.instrument_model].filter(Boolean).join(" · ") || "—")], ["Spots", x.spots ? fmt(x.spots) : "—"],
            ["Bases", x.bases ? `${fmt(x.bases)} (${gbp(x.bases)})` : "—"], ["Read length", x.avg_length ? `${fmt(x.avg_length)} bp` : "—"],
            ["Size", x.size_mb ? `${fmt(Math.round(x.size_mb))} MB` : "—"], ["Released", esc((x.release_date || "").slice(0, 10) || "—")],
            ["Organism label", esc(x.scientific_name || "—")], ["Location", s.place_id != null ? `<a href="${href.city(s.place_id)}">${esc(s.city)}</a>, ${esc(s.country)}` : esc(s.country || "—")]])}
      <div class="btn-row" style="margin:14px 0">
        <a class="btn" href="https://www.ncbi.nlm.nih.gov/sra/${encodeURIComponent(acc)}" target="_blank" rel="noopener">NCBI SRA ↗</a>
        <a class="btn" href="https://www.ebi.ac.uk/ena/browser/view/${encodeURIComponent(acc)}" target="_blank" rel="noopener">ENA ↗</a>
      </div>
      <div class="panel"><h2>Download the reads</h2>
        <pre class="mono" style="white-space:pre-wrap;font-size:13px;background:rgba(0,0,0,.25);padding:12px;border-radius:12px;margin:0"># SRA Toolkit
prefetch ${esc(acc)} && fasterq-dump --split-3 ${esc(acc)}

# nf-core/fetchngs
echo ${esc(acc)} > ids.csv && nextflow run nf-core/fetchngs --input ids.csv --outdir fastq</pre></div>
      <div class="panel" style="margin-top:16px"><h2>Provenance</h2><p style="font-size:14px">Run fields come from SRA RunInfo. The assay class is UMDB's standardization of library strategy, source and selection.</p>${await provenance()}</div>`;
  }

  // ------------------------------------------------------------------ city / country / habitat
  async function place(){
    const E = await U.openIndex();
    const allTiers = X.param("tier") === "all";
    const f = U.emptyFilters();
    if (allTiers) f.tier = [];
    let title, crumb, extraKV = [], subtitle = "", fileBase;
    if (kind === "city"){
      const p = E.geo.placeById.get(X.param("id"));
      if (!p) return fail("That place is not in UMDB.");
      const c = E.geo.countries[p.country];
      f.city = String(p.geonames_id); f.country = c.iso2;
      title = p.name; fileBase = `city_${p.geonames_id}`;
      crumb = `<a href="${href.country(c.iso2)}">${esc(c.name)}</a> › City`;
      subtitle = `${p.admin1 != null ? esc(E.geo.admin1[p.admin1].name) + ", " : ""}<a href="${href.country(c.iso2)}">${esc(c.name)}</a>`;
      extraKV = [["Population (GeoNames)", p.population ? fmt(p.population) : "—"], ["Centre", `${p.lat.toFixed(3)}, ${p.lon.toFixed(3)}`],
                 ["GeoNames", `<a href="https://www.geonames.org/${p.geonames_id}" target="_blank" rel="noopener">${p.geonames_id} ↗</a>`]];
    } else if (kind === "country"){
      const c = E.geo.countryByIso.get(X.param("iso").toUpperCase());
      if (!c) return fail("That country is not in UMDB.");
      f.country = c.iso2; title = c.name; fileBase = `country_${c.iso2}`;
      crumb = "Country"; subtitle = esc(CONTINENT[c.continent] || "");
      extraKV = [["Population (GeoNames)", c.population ? fmt(c.population) : "—"], ["ISO 3166", esc(c.iso2)]];
    } else {
      const ctx = X.habitatFromSlug(X.param("id"));
      if (!E.contexts.includes(ctx)) return fail("That habitat is not in UMDB.");
      f.context = [ctx]; title = CTX_LABEL[ctx] || ctx; fileBase = `habitat_${X.param("id")}`;
      crumb = "Habitat"; subtitle = "Urban context assigned by the eligibility screen";
    }
    const res = E.query(f);
    setTitle(title);
    const ctry = kind === "country" ? E.geo.countryByIso.get(f.country) : null;
    const perM = ctry && ctry.population ? (res.samples / ctry.population * 1e6) : null;
    const toggle = new URLSearchParams(location.search);
    allTiers ? toggle.delete("tier") : toggle.set("tier", "all");
    const facetItems = (map, label, link) => Array.from(map.entries()).map(([k, v]) => ({ label: label(k), value: v.samples || 0, href: link ? link(k) : null }))
      .filter((d) => d.value > 0).sort((a, b) => b.value - a.value);
    root.innerHTML = `
      <div class="crumbs"><a href="./explorer.html">Catalog</a> › ${crumb}</div>
      <div class="entity-head"><h1>${esc(title)}</h1><div class="muted">${subtitle}</div></div>
      ${kv([["BioSamples", fmt(res.samples)], ["Runs", fmt(res.runs)], ["Studies", fmt(res.projects.length)], ["Sequence", `${fmt(Math.round(res.gbases))} Gbp`],
            ...extraKV, perM != null ? ["Samples per million people", perM.toFixed(perM < 10 ? 2 : 0)] : null])}
      <div class="btn-row" style="margin:14px 0">
        <span class="muted" style="font-size:13px">${allTiers ? "All eligibility tiers" : "Confirmed and probable urban samples"}</span>
        <a class="btn small" href="?${toggle.toString()}">${allTiers ? "Urban only" : "Include all tiers"}</a>
        <a class="btn primary" href="${href.explorer(f)}">Open in Explorer</a>
        <a class="btn" href="${href.atlas(f)}">Show on Atlas</a>
        ${saveCohortButton(title, f)}
      </div>
      <div class="grid-3">
        <div class="panel" id="c1"></div><div class="panel" id="c2"></div><div class="panel" id="c3"></div>
      </div>
      <div class="grid-2" style="margin-top:16px"><div class="panel" id="c4"></div><div class="panel" id="c5"></div></div>
      <div class="panel" style="margin-top:16px"><h2>Studies (${fmt(res.projects.length)})</h2>
        <div class="table-shell"><table class="data-table"><thead><tr><th>BioProject</th><th>Title</th><th class="num">BioSamples here</th><th class="num">Runs here</th><th>Released</th></tr></thead>
        <tbody>${res.projects.slice(0, 100).map((p) => `<tr><td class="mono"><a href="${href.study(p.accession)}">${esc(p.accession)}</a></td><td>${esc(p.title)}<span class="sub">${esc(p.center_name)}</span></td>
          <td class="num">${fmt(p.samples)}</td><td class="num">${fmt(p.runs)}</td><td>${p.first_year ? `${p.first_year}${p.last_year !== p.first_year ? "–" + p.last_year : ""}` : ""}</td></tr>`).join("")}</tbody></table></div>
        ${res.projects.length > 100 ? `<div class="sub">Top 100 of ${fmt(res.projects.length)} by BioSamples; the Explorer lists all.</div>` : ""}</div>
      <div class="panel" style="margin-top:16px"><h2>Provenance</h2><p style="font-size:14px">Counts are exact: each BioSample is counted once. Places and countries come from UMDB's gazetteer validation (${esc(E.geo.attribution)}).</p>${await provenance()}</div>`;
    const hab = facetItems(res.facets.context, (k) => CTX_LABEL[k] || k, (k) => k ? href.habitat(k) : null);
    const assay = facetItems(res.facets.assay, (k) => k);
    const tier = facetItems(res.facets.tier, (k) => TIER_LABEL[k] || k);
    const cy = new Map(Array.from(res.facets.cyear.entries()).map(([k, v]) => [String(k), v.samples]));
    if (kind === "habitat") X.bars($("c1"), "Countries", facetItems(res.facets.country, (i) => E.geo.countries[i].name, (i) => href.country(E.geo.countries[i].iso2)), { unit: "samples", file: `${fileBase}_countries` });
    else X.bars($("c1"), "Habitats", hab, { unit: "samples", file: `${fileBase}_habitats` });
    X.bars($("c2"), "Assays", assay, { unit: "samples", file: `${fileBase}_assays` });
    X.bars($("c3"), "Urban eligibility (all tiers)", tier, { unit: "samples", file: `${fileBase}_tiers` });
    if (kind === "city") X.bars($("c4"), "Location quality", facetItems(res.facets.loc, (k) => LOC_LABEL[k] || k), { unit: "samples", file: `${fileBase}_location_quality` });
    else X.bars($("c4"), "Cities and towns", facetItems(res.facets.city, (i) => E.geo.places[i].name, (i) => href.city(E.geo.places[i].geonames_id)), { unit: "samples", limit: 15, file: `${fileBase}_cities` });
    X.columns($("c5"), "BioSamples by collection year", X.yearItems(cy), { file: `${fileBase}_collection_years`, note: res.samplesNoCyear ? `${fmt(res.samplesNoCyear)} samples have no collection year.` : "" });
  }

  const pages = { study, sample, run, city: place, country: place, habitat: place };
  (pages[kind] || (() => fail("Unknown page.")))().catch((err) => fail(`This page could not be loaded (${err.message}).`));
})();
