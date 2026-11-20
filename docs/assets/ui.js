(function(){
  // Shared helpers for the v1.0 pages (entity pages, Collections, Workspace, Home search).
  // Charts are plain HTML/SVG so they render without a library; every chart can be
  // downloaded as CSV (its data) and, for SVG charts, as SVG or PNG (the figure).
  const U = window.UMDB;
  const esc = (v) => U.escapeHtml(v);
  const fmt = (n) => U.formatNumber(n);

  const TIER_LABEL = { confirmed_urban: "Confirmed urban", probable_urban: "Probable urban", uncertain: "Uncertain",
                       non_urban: "Not urban", non_environmental: "Not environmental", unscreened: "Unscreened" };
  const TIER_ORDER = ["confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental", "unscreened"];
  const CTX_LABEL = { municipal_wastewater: "Municipal wastewater", air_dust: "Air and dust", built_environment: "Built environment",
                      water_drainage: "Urban water and drainage", soil_greenspace: "Soil and green space", transit: "Transit",
                      surfaces_infrastructure: "Surfaces and infrastructure", "": "No urban context" };
  const CONTINENT = { AF: "Africa", AS: "Asia", EU: "Europe", NA: "North America", OC: "Oceania", SA: "South America",
                      AN: "Antarctica", "": "Unresolved" };
  const LOC_LABEL = { verified: "Verified place", probable: "Probable place", ambiguous: "Ambiguous", conflicting: "Conflicting coordinates",
                      unresolved: "Country or less" };
  const habitatSlug = (ctx) => (ctx || "").replace(/_/g, "-");
  const habitatFromSlug = (slug) => (slug || "").replace(/-/g, "_");

  const param = (k) => new URLSearchParams(location.search).get(k) || "";

  function download(name, text, type){
    const blob = text instanceof Blob ? text : new Blob([text], { type: type || "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = Object.assign(document.createElement("a"), { href: url, download: name });
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 3000);
  }
  function toCSV(rows, cols){
    const q = (v) => { const s = (v ?? "").toString(); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
    return [cols.join(","), ...rows.map((r) => cols.map((c) => q(r[c])).join(","))].join("\n") + "\n";
  }

  // ---------------------------------------------------------------- charts
  let chartSeq = 0;
  function chartShell(el, title, opts){
    const id = `chart-${++chartSeq}`;
    el.innerHTML = `<div class="chart-head"><h3>${esc(title)}</h3><div class="chart-tools" role="group" aria-label="Download ${esc(title)}">
        <button type="button" data-dl="csv">CSV</button>${opts.svg ? '<button type="button" data-dl="svg">SVG</button><button type="button" data-dl="png">PNG</button>' : ""}</div></div>
      <div id="${id}"></div>`;
    return el.querySelector(`#${id}`);
  }
  function wireDownloads(el, base, rows, cols){
    el.querySelector(".chart-tools").addEventListener("click", (e) => {
      const kind = e.target.dataset.dl;
      if (kind === "csv") download(`${base}.csv`, toCSV(rows, cols), "text/csv");
      const svg = el.querySelector("svg");
      if (kind === "svg" && svg) download(`${base}.svg`, svgText(svg), "image/svg+xml");
      if (kind === "png" && svg) svgToPng(svg, `${base}.png`);
    });
  }
  function svgText(svg){
    const clone = svg.cloneNode(true);
    clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    clone.insertAdjacentHTML("afterbegin", `<rect width="100%" height="100%" fill="#0e1724"/>`);
    return new XMLSerializer().serializeToString(clone);
  }
  function svgToPng(svg, name){
    const { width, height } = svg.viewBox.baseVal;
    const img = new Image();
    img.onload = () => {
      const c = Object.assign(document.createElement("canvas"), { width: width * 2, height: height * 2 });
      const ctx = c.getContext("2d");
      ctx.scale(2, 2); ctx.drawImage(img, 0, 0, width, height);
      c.toBlob((b) => download(name, b), "image/png");
    };
    img.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(svgText(svg));
  }

  // Horizontal bars: items [{label, value, href?}] -> HTML bars with a CSV download.
  function bars(el, title, items, opts){
    opts = opts || {};
    const body = chartShell(el, title, {});
    const max = Math.max(1, ...items.map((d) => d.value));
    const shown = items.slice(0, opts.limit || 12);
    body.innerHTML = shown.length ? shown.map((d) => `<div class="hbar"><span class="lab" title="${esc(d.label)}">${d.href ? `<a href="${esc(d.href)}">${esc(d.label)}</a>` : esc(d.label)}</span>
        <span class="track"><span class="fill" style="width:${(100 * d.value / max).toFixed(1)}%"></span></span><span class="val">${fmt(d.value)}${opts.unit ? " " + esc(d.value === 1 ? opts.unit.replace(/s$/, "") : opts.unit) : ""}</span></div>`).join("")
      + (items.length > shown.length ? `<div class="sub">Top ${shown.length} of ${fmt(items.length)}; the CSV has all of them.</div>` : "")
      : `<div class="sub">None.</div>`;
    wireDownloads(el, opts.file || slugify(title), items.map((d) => ({ name: d.label, value: d.value })), ["name", "value"]);
  }

  // Column chart (e.g. by year) as SVG, downloadable as CSV, SVG and PNG.
  function columns(el, title, items, opts){
    opts = opts || {};
    const body = chartShell(el, title, { svg: true });
    const W = 640, H = 220, L = 46, B = 26, T = 10;
    const max = Math.max(1, ...items.map((d) => d.value));
    const bw = items.length ? (W - L - 8) / items.length : 0;
    const ticks = [0, max / 2, max];
    const label = (i) => items.length <= 14 || i % Math.ceil(items.length / 12) === 0;
    body.innerHTML = items.length ? `<svg viewBox="0 0 ${W} ${H}" class="chart-svg" role="img" aria-label="${esc(title)}" style="width:100%;height:auto">
      ${ticks.map((v) => { const y = T + (H - T - B) * (1 - v / max); return `<line x1="${L}" x2="${W}" y1="${y}" y2="${y}" stroke="rgba(148,174,214,.16)"/><text x="${L - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#98a8c0">${fmt(Math.round(v))}</text>`; }).join("")}
      ${items.map((d, i) => { const h = (H - T - B) * d.value / max; const x = L + i * bw;
        return `<rect x="${x + 1}" y="${H - B - h}" width="${Math.max(1, bw - 2)}" height="${h}" fill="${opts.color || "#4ec5a8"}"><title>${esc(d.label)}: ${fmt(d.value)}</title></rect>`
          + (label(i) ? `<text x="${x + bw / 2}" y="${H - 8}" text-anchor="middle" font-size="10" fill="#98a8c0">${esc(d.label)}</text>` : ""); }).join("")}
      </svg>${opts.note ? `<div class="sub">${esc(opts.note)}</div>` : ""}` : `<div class="sub">None.</div>`;
    wireDownloads(el, opts.file || slugify(title), items.map((d) => ({ name: d.label, value: d.value })), ["name", "value"]);
  }
  const slugify = (s) => (s || "chart").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");

  // Map entries -> sorted bar items.
  function mapItems(map, labelFn, hrefFn){
    return Array.from(map.entries()).filter(([, v]) => v > 0)
      .map(([k, v]) => ({ key: k, label: labelFn ? labelFn(k) : String(k), value: v, href: hrefFn ? hrefFn(k) : null }))
      .sort((a, b) => b.value - a.value);
  }
  function yearItems(map){
    const ys = Array.from(map.keys()).filter((k) => k !== "" && k != null).map(Number).filter(Number.isFinite);
    if (!ys.length) return [];
    const lo = Math.max(Math.min(...ys), 1990), hi = Math.max(...ys);
    const out = [];
    for (let y = lo; y <= hi; y += 1) out.push({ label: String(y), value: map.get(String(y)) || map.get(y) || 0 });
    return out;
  }

  // ---------------------------------------------------------------- workspace storage
  // Browser-local; see workspace.js for the cohort format.
  const WS_KEY = "umdb.workspace.v1";
  function loadWorkspace(){
    try { return JSON.parse(localStorage.getItem(WS_KEY)) || { cohorts: [] }; } catch { return { cohorts: [] }; }
  }
  function saveWorkspace(ws){
    try { localStorage.setItem(WS_KEY, JSON.stringify(ws)); return true; } catch { return false; }
  }
  function addCohort(name, definition, extra){
    const ws = loadWorkspace();
    const id = `c${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
    ws.cohorts.push({ id, name, created: new Date().toISOString(), definition, ...(extra || {}) });
    return saveWorkspace(ws) ? id : null;
  }
  function describeFilters(f, E){
    const parts = [];
    if ((f.tier || []).length) parts.push(f.tier.map((t) => TIER_LABEL[t] || t).join(" or "));
    else parts.push("All tiers");
    if ((f.context || []).length) parts.push(f.context.map((c) => CTX_LABEL[c] || c).join(", "));
    if ((f.assay || []).length) parts.push(f.assay.join(", "));
    if (f.country) parts.push(E ? E.countryName(f.country) : f.country);
    if (f.city) parts.push(E ? E.placeName(f.city) : `place ${f.city}`);
    if (f.region) parts.push(`region ${f.region}`);
    if (f.cmin != null || f.cmax != null) parts.push(`collected ${f.cmin ?? "…"}–${f.cmax ?? "…"}`);
    if (f.ymin != null || f.ymax != null) parts.push(`released ${f.ymin ?? "…"}–${f.ymax ?? "…"}`);
    if (f.loc && f.loc !== "any") parts.push({ country: "validated country", place: "accepted place", verified: "verified place" }[f.loc]);
    if (f.project) parts.push(f.project);
    if (f.q) parts.push(`“${f.q}”`);
    return parts.join(" · ");
  }

  // Routes: entity page URLs (static pages with query parameters; 404.html maps /study/X to them).
  const href = {
    study: (acc) => `./study.html?acc=${encodeURIComponent(acc)}`,
    sample: (acc) => `./sample.html?acc=${encodeURIComponent(acc)}`,
    run: (acc) => `./run.html?acc=${encodeURIComponent(acc)}`,
    city: (id) => `./city.html?id=${encodeURIComponent(id)}`,
    country: (iso) => `./country.html?iso=${encodeURIComponent(iso)}`,
    habitat: (ctx) => `./habitat.html?id=${encodeURIComponent(habitatSlug(ctx))}`,
    collection: (slug) => `./collections.html?slug=${encodeURIComponent(slug)}`,
    explorer: (f) => U.filtersToURL(f, "./explorer.html"),
    atlas: (f) => U.filtersToURL(f, "./global-map.html"),
  };

  // Accession or free text -> where to go.
  async function resolveSearch(text){
    const t = (text || "").trim();
    const kind = U.accessionKind(t);
    if (kind === "study") return href.study(t.toUpperCase());
    if (kind){
      const hit = await U.lookupAccession(t).catch(() => null);
      if (hit) return kind === "sample" ? href.sample(t.toUpperCase()) : href.run(t.toUpperCase());
    }
    const f = U.emptyFilters();
    f.q = t;
    return href.explorer(f);
  }

  window.UMDBUI = { TIER_LABEL, TIER_ORDER, CTX_LABEL, CONTINENT, LOC_LABEL, habitatSlug, habitatFromSlug, param, download, toCSV,
                    bars, columns, mapItems, yearItems, loadWorkspace, saveWorkspace, addCohort, describeFilters, href,
                    resolveSearch, esc, fmt, svgText };
})();
