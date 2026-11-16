(function(){
  // UMDB website data (site schema 3). Built from SQLite by scripts/umdb_harvester/site.py.
  //
  //   summary.json   exact headline counts, labelled by unit
  //   geo.json       validated location hierarchy (ISO countries, GeoNames regions and places)
  //   samples.json   BioSamples aggregated so each sample is counted once (exact sample totals)
  //   cube.json      runs aggregated by the same dimensions (exact run totals)
  //   projects.json  one row per BioProject
  //   p/<ACC>.json   one project's samples and runs, fetched when opened
  //
  // UMDB.openIndex() returns a query engine shared by the Explorer and the Map:
  // query(filters) gives sample, run and project totals plus facet counts for
  // every filter, each computed with all *other* filters applied, so cascading
  // country -> region -> city lists only offer combinations that exist.
  const URLS = {
    summary: "./db/summary.json", geo: "./db/geo.json", samples: "./db/samples.json",
    cube: "./db/cube.json", projects: "./db/projects.json", manifest: "./db/manifest.json"
  };
  const cache = new Map();
  const shardPromises = new Map();

  // Every data URL carries the build timestamp from manifest.json (always
  // revalidated), so a new release is never mixed with cached files from the
  // previous one, while unchanged releases still come from the browser cache.
  let versionPromise = null;
  function buildVersion(){
    if (!versionPromise){
      versionPromise = fetch(URLS.manifest, { cache: "no-cache" })
        .then((res) => (res.ok ? res.json() : {}))
        .then((m) => m.generated_utc || m.database_built_utc || "")
        .catch(() => "");
    }
    return versionPromise;
  }

  function fetchJson(url){
    if (!cache.has(url)){
      const versioned = url === URLS.manifest ? Promise.resolve(url)
        : buildVersion().then((v) => (v ? `${url}${url.includes("?") ? "&" : "?"}v=${encodeURIComponent(v)}` : url));
      cache.set(url, versioned.then((u) => fetch(u, { cache: url === URLS.manifest ? "no-cache" : "default" })).then(async (res) => {
        if (!res.ok) throw new Error(`Unable to load ${url} (${res.status})`);
        return await res.json();
      }).catch((err) => { cache.delete(url); throw err; }));
    }
    return cache.get(url);
  }

  function rowsToObjects(cols, rows){
    return (rows || []).map((row) => {
      const o = {};
      cols.forEach((k, i) => { o[k] = row[i]; });
      return o;
    });
  }

  const norm = (v) => (v ?? "").toString().trim();
  const fetchSummary = () => fetchJson(URLS.summary);
  const fetchCube = () => fetchJson(URLS.cube);
  const fetchSampleCube = () => fetchJson(URLS.samples);
  const fetchManifest = () => fetchJson(URLS.manifest).catch(() => ({}));

  // ---------------------------------------------------------------- geography
  let geoPromise = null;
  function fetchGeo(){
    if (!geoPromise){
      geoPromise = fetchJson(URLS.geo).then((doc) => {
        const countries = rowsToObjects(doc.country_cols, doc.countries).map((c, i) => ({ ...c, i }));
        const admin1 = rowsToObjects(doc.admin1_cols, doc.admin1).map((a, i) => ({ ...a, i }));
        const places = rowsToObjects(doc.place_cols, doc.places).map((p, i) => ({ ...p, i }));
        return {
          attribution: doc.attribution, statuses: doc.statuses, rulesVersion: doc.geo_rules_version,
          countries, admin1, places,
          countryByIso: new Map(countries.map((c) => [c.iso2, c])),
          countryByNumeric: new Map(countries.map((c) => [String(parseInt(c.numeric, 10)), c])),
          admin1ByCode: new Map(admin1.map((a) => [a.code, a])),
          placeById: new Map(places.map((p) => [String(p.geonames_id), p]))
        };
      }).catch((err) => { geoPromise = null; throw err; });
    }
    return geoPromise;
  }

  // ---------------------------------------------------------------- projects
  let projectsPromise = null;
  function fetchProjectsIndex(){
    if (!projectsPromise){
      projectsPromise = fetchJson(URLS.projects).then((doc) => {
        const map = new Map();
        for (const o of rowsToObjects(doc.cols, doc.rows)) map.set(o.accession, o);
        return map;
      }).catch((err) => { projectsPromise = null; throw err; });
    }
    return projectsPromise;
  }

  function shardName(acc){
    const a = norm(acc).toUpperCase();
    return (/^[A-Z0-9_.-]+$/.test(a) ? a : "_none") + ".json";
  }

  // One project's samples and runs. Each run is returned as a record object
  // (the shape older pages use) and samples keep their accepted location.
  function fetchProject(acc){
    const key = norm(acc);
    if (!shardPromises.has(key)){
      const promise = fetchJson(`./db/p/${shardName(key)}`).then((shard) => {
        const project = shard.project || {};
        const samples = rowsToObjects(shard.sample_cols, shard.samples);
        const runs = rowsToObjects(shard.run_cols, shard.runs).map((r) => {
          const s = samples[r.sample] || {};
          const bs = s.accession || "";
          return {
            srr: r.accession,
            sra_uid: r.experiment || "",
            title: r.experiment_title || s.title || "",
            _sample: s, _run: r,
            runinfo_row: {
              Run: r.accession, Experiment: r.experiment || "", BioProject: project.accession || key, BioSample: bs,
              SampleName: s.title || "", ReleaseDate: r.release_date || "", CenterName: project.center_name || "",
              LibraryStrategy: r.library_strategy || "", LibrarySource: r.library_source || "",
              LibrarySelection: r.library_selection || "", LibraryLayout: r.library_layout || "",
              Platform: r.platform || "", Model: r.instrument_model || "", ScientificName: r.scientific_name || "",
              spots: r.spots, bases: r.bases, avgLength: r.avg_length, size_MB: r.size_mb
            },
            bioproject: { accession: project.accession || key, title: project.title || "",
                          center_name: project.center_name || "", description: project.description || "" },
            geo: { country: s.country || "", city: s.city || "", region: s.region || "",
                   country_iso2: s.country_iso2 || "", place_id: s.place_id ?? "",
                   location_status: s.location_status || "unresolved", raw: s.geo_loc_raw || "",
                   lat: s.lat == null ? "" : String(s.lat), lon: s.lon == null ? "" : String(s.lon),
                   biosample_accession: bs },
            biosample: { accession: bs, title: s.title || "", collection_date: s.collection_date || "",
                         collection_year: s.collection_year ?? null,
                         isolation_source: s.isolation_source || "", env_broad_scale: s.env_broad_scale || "",
                         env_local_scale: s.env_local_scale || "", env_medium: s.env_medium || "", host: s.host || "" },
            assay: { assay_class: r.assay_class || "Unknown" },
            eligibility: { status: r.urban_status || "unscreened",
                           sample_status: s.urban_status || "unscreened",
                           urban_context: s.urban_context ? [s.urban_context] : [],
                           reasons: s.eligibility_reason ? [s.eligibility_reason] : [] },
            ai_curation: (shard.ai || {})[r.accession] || {},
            ncbi: { srr_url: `https://www.ncbi.nlm.nih.gov/sra/${encodeURIComponent(r.accession)}`,
                    sra_uid_url: r.experiment ? `https://www.ncbi.nlm.nih.gov/sra/${encodeURIComponent(r.experiment)}` : "",
                    bioproject_url: `https://www.ncbi.nlm.nih.gov/bioproject/${encodeURIComponent(project.accession || key)}` },
            debug: { biosample_url: bs ? `https://www.ncbi.nlm.nih.gov/biosample/${encodeURIComponent(bs)}` : "" }
          };
        });
        return { project, samples, runs };
      }).catch((err) => { shardPromises.delete(key); throw err; });
      shardPromises.set(key, promise);
    }
    return shardPromises.get(key);
  }

  // ---------------------------------------------------------------- filters
  const DEFAULT_TIERS = ["confirmed_urban", "probable_urban"];
  const FILTER_KEYS = ["q", "country", "region", "city", "tier", "context", "assay", "loc", "cyear", "year", "project"];

  function emptyFilters(){
    return { q: "", country: "", region: "", city: "", tier: DEFAULT_TIERS.slice(), context: [], assay: [],
             loc: "any", cmin: null, cmax: null, ymin: null, ymax: null, project: "" };
  }

  function parseRange(v){
    const m = norm(v).match(/^(\d{4})?\s*-\s*(\d{4})?$|^(\d{4})$/);
    if (!m) return [null, null];
    if (m[3]) return [parseInt(m[3], 10), parseInt(m[3], 10)];
    return [m[1] ? parseInt(m[1], 10) : null, m[2] ? parseInt(m[2], 10) : null];
  }

  // URL query string <-> filters. Shared by the Explorer and the Map so a
  // selection on one page opens identically on the other.
  function filtersFromURL(search){
    const p = new URLSearchParams(search ?? window.location.search);
    const f = emptyFilters();
    const list = (k) => norm(p.get(k)).split(",").map(norm).filter(Boolean);
    f.q = norm(p.get("q"));
    f.country = norm(p.get("country")).toUpperCase();
    f.region = norm(p.get("region"));
    f.city = norm(p.get("city_id"));
    if (p.has("tier")) f.tier = list("tier").includes("all") ? [] : list("tier");
    f.context = list("context");
    f.assay = list("assay");
    f.loc = ["any", "place", "verified", "country"].includes(p.get("loc")) ? p.get("loc") : "any";
    [f.cmin, f.cmax] = parseRange(p.get("cyear"));
    [f.ymin, f.ymax] = parseRange(p.get("year"));
    f.project = norm(p.get("project"));
    return f;
  }

  function filtersToParams(f){
    const p = new URLSearchParams();
    if (f.q) p.set("q", f.q);
    if (f.country) p.set("country", f.country);
    if (f.region) p.set("region", f.region);
    if (f.city) p.set("city_id", f.city);
    const tiers = f.tier || [];
    const isDefault = tiers.length === DEFAULT_TIERS.length && DEFAULT_TIERS.every((t) => tiers.includes(t));
    if (!isDefault) p.set("tier", tiers.length ? tiers.join(",") : "all");
    if ((f.context || []).length) p.set("context", f.context.join(","));
    if ((f.assay || []).length) p.set("assay", f.assay.join(","));
    if (f.loc && f.loc !== "any") p.set("loc", f.loc);
    const range = (a, b) => (a == null && b == null) ? "" : (a === b ? String(a) : `${a ?? ""}-${b ?? ""}`);
    if (range(f.cmin, f.cmax)) p.set("cyear", range(f.cmin, f.cmax));
    if (range(f.ymin, f.ymax)) p.set("year", range(f.ymin, f.ymax));
    if (f.project) p.set("project", f.project);
    return p;
  }

  function filtersToURL(f, page){
    const qs = filtersToParams(f).toString();
    return `${page || window.location.pathname}${qs ? "?" + qs : ""}`;
  }

  // ---------------------------------------------------------------- engine
  // Facet ids double as bit positions in a row's "failed filters" mask.
  const F = { q: 0, country: 1, region: 2, city: 3, tier: 4, context: 5, assay: 6, loc: 7, cyear: 8, year: 9, project: 10 };
  const LOC = { verified: 0, probable: 1 };

  let indexPromise = null;
  function openIndex(){
    if (!indexPromise){
      indexPromise = Promise.all([fetchGeo(), fetchSampleCube(), fetchCube(), fetchProjectsIndex()])
        .then(([geo, scube, rcube, projects]) => buildEngine(geo, scube, rcube, projects))
        .catch((err) => { indexPromise = null; throw err; });
    }
    return indexPromise;
  }

  function columns(cube){
    const ix = Object.fromEntries(cube.cols.map((k, i) => [k, i]));
    return (name) => ix[name];
  }

  function buildEngine(geo, scube, rcube, projects){
    const SD = scube.dims, RD = rcube.dims;
    const projectAcc = SD.project;                 // both cubes share the project/assay/tier/context dims
    const assays = SD.assay, tiers = SD.tier, contexts = SD.context;
    const projectInfo = projectAcc.map((acc) => projects.get(acc) || { accession: acc, title: "", runs: 0, samples: 0 });
    const assayBits = SD.assaysets.map((set) => set.reduce((m, a) => m | (1 << a), 0));
    const sc = columns(scube), rc = columns(rcube);
    const S = scube.rows, R = rcube.rows;
    const s = { proj: sc("projects"), year: sc("year"), cyear: sc("cyear"), country: sc("country"), admin1: sc("admin1"),
                place: sc("place"), assays: sc("assays"), tier: sc("tier"), ctx: sc("context"), loc: sc("loc"),
                lat: sc("lat"), lon: sc("lon"), n: sc("samples") };
    const r = { proj: rc("project"), year: rc("year"), cyear: rc("cyear"), country: rc("country"), admin1: rc("admin1"),
                place: rc("place"), assay: rc("assay"), tier: rc("tier"), ctx: rc("context"), loc: rc("loc"),
                n: rc("runs"), gb: rc("gbases") };
    const searchText = projectInfo.map((p) => `${p.accession} ${p.title || ""} ${p.center_name || ""}`.toLowerCase());

    function compile(f){
      const c = {};
      const q = norm(f.q).toLowerCase();
      if (q){
        const words = q.split(/\s+/).filter(Boolean);
        c.projMatch = new Uint8Array(projectAcc.length);
        searchText.forEach((t, i) => { if (words.every((w) => t.includes(w))) c.projMatch[i] = 1; });
        c.projsetMatch = SD.projsets.map((set) => set.some((p) => c.projMatch[p]) ? 1 : 0);
      }
      if (f.project){
        const pi = projectAcc.indexOf(f.project);
        c.project = pi;
        c.projsetHas = SD.projsets.map((set) => set.includes(pi) ? 1 : 0);
      }
      const ctry = f.country ? geo.countryByIso.get(f.country) : null;
      c.country = f.country ? (ctry ? ctry.i : -2) : null;
      const reg = f.region ? geo.admin1ByCode.get(f.region) : null;
      c.region = f.region ? (reg ? reg.i : -2) : null;
      const city = f.city ? geo.placeById.get(String(f.city)) : null;
      c.city = f.city ? (city ? city.i : -2) : null;
      c.tier = (f.tier || []).length ? new Set(f.tier.map((t) => tiers.indexOf(t)).filter((i) => i >= 0)) : null;
      if (c.tier && !c.tier.size) c.tier = new Set([-2]);
      c.context = (f.context || []).length ? new Set(f.context.map((t) => contexts.indexOf(t))) : null;
      c.assayMask = (f.assay || []).length ? f.assay.reduce((m, a) => { const i = assays.indexOf(a); return i >= 0 ? m | (1 << i) : m; }, 0) : null;
      c.assaySet = (f.assay || []).length ? new Set(f.assay.map((a) => assays.indexOf(a))) : null;
      c.loc = f.loc || "any";
      c.cmin = f.cmin; c.cmax = f.cmax; c.ymin = f.ymin; c.ymax = f.ymax;
      return c;
    }

    function locFails(c, loc, country){
      if (c.loc === "verified") return loc !== LOC.verified;
      if (c.loc === "place") return loc !== LOC.verified && loc !== LOC.probable;
      if (c.loc === "country") return country == null;
      return false;
    }
    const inRange = (v, a, b) => (a == null || (v != null && v >= a)) && (b == null || (v != null && v <= b));

    function sampleMask(c, row){
      let m = 0;
      if (c.projsetMatch && !c.projsetMatch[row[s.proj]]) m |= 1 << F.q;
      if (c.projsetHas && !c.projsetHas[row[s.proj]]) m |= 1 << F.project;
      if (c.country != null && row[s.country] !== c.country) m |= 1 << F.country;
      if (c.region != null && row[s.admin1] !== c.region) m |= 1 << F.region;
      if (c.city != null && row[s.place] !== c.city) m |= 1 << F.city;
      if (c.tier && !c.tier.has(row[s.tier])) m |= 1 << F.tier;
      if (c.context && !c.context.has(row[s.ctx])) m |= 1 << F.context;
      if (c.assayMask != null && !(assayBits[row[s.assays]] & c.assayMask)) m |= 1 << F.assay;
      if (locFails(c, row[s.loc], row[s.country])) m |= 1 << F.loc;
      if (!inRange(row[s.cyear], c.cmin, c.cmax)) m |= 1 << F.cyear;
      if (!inRange(row[s.year], c.ymin, c.ymax)) m |= 1 << F.year;
      return m;
    }

    function runMask(c, row){
      let m = 0;
      if (c.projMatch && !c.projMatch[row[r.proj]]) m |= 1 << F.q;
      if (c.project != null && row[r.proj] !== c.project) m |= 1 << F.project;
      if (c.country != null && row[r.country] !== c.country) m |= 1 << F.country;
      if (c.region != null && row[r.admin1] !== c.region) m |= 1 << F.region;
      if (c.city != null && row[r.place] !== c.city) m |= 1 << F.city;
      if (c.tier && !c.tier.has(row[r.tier])) m |= 1 << F.tier;
      if (c.context && !c.context.has(row[r.ctx])) m |= 1 << F.context;
      if (c.assaySet && !c.assaySet.has(row[r.assay])) m |= 1 << F.assay;
      if (locFails(c, row[r.loc], row[r.country])) m |= 1 << F.loc;
      if (!inRange(row[r.cyear], c.cmin, c.cmax)) m |= 1 << F.cyear;
      if (!inRange(row[r.year], c.ymin, c.ymax)) m |= 1 << F.year;
      return m;
    }

    const bump = (map, key, field, v) => {
      let o = map.get(key);
      if (!o){ o = { samples: 0, runs: 0 }; map.set(key, o); }
      o[field] += v;
    };

    // query(filters, {points}) -> totals, projects, facets (and map points).
    function query(f, opts){
      opts = opts || {};
      const c = compile(f);
      const facets = { country: new Map(), region: new Map(), city: new Map(), tier: new Map(), context: new Map(),
                       assay: new Map(), loc: new Map(), cyear: new Map(), year: new Map() };
      const out = { samples: 0, runs: 0, gbases: 0, samplesNoCyear: 0, samplesNoCountry: 0, samplesNoPlace: 0,
                    projSamples: new Map(), projRuns: new Map(), points: opts.points ? new Map() : null };
      const one = (m, bit) => m === 0 || m === (1 << bit);

      for (const row of S){
        const m = sampleMask(c, row);
        if (m !== 0 && (m & (m - 1)) !== 0) continue;      // fails two or more filters: in no facet
        const n = row[s.n];
        if (m === 0){
          out.samples += n;
          if (row[s.cyear] == null) out.samplesNoCyear += n;
          if (row[s.country] == null) out.samplesNoCountry += n;
          if (row[s.place] == null) out.samplesNoPlace += n;
          for (const p of SD.projsets[row[s.proj]]) out.projSamples.set(p, (out.projSamples.get(p) || 0) + n);
          if (out.points && row[s.lat] != null){
            const key = `${row[s.lat]},${row[s.lon]},${row[s.ctx]}`;
            const pt = out.points.get(key);
            if (pt) pt.n += n; else out.points.set(key, { lat: row[s.lat], lon: row[s.lon], ctx: contexts[row[s.ctx]] || "", n,
                                                          projects: new Set(), places: new Map(), countries: new Set(), tiers: new Map() });
            const target = pt || out.points.get(key);
            for (const p of SD.projsets[row[s.proj]]) target.projects.add(projectAcc[p]);
            if (row[s.place] != null) target.places.set(row[s.place], (target.places.get(row[s.place]) || 0) + n);
            if (row[s.country] != null) target.countries.add(row[s.country]);
            target.tiers.set(tiers[row[s.tier]], (target.tiers.get(tiers[row[s.tier]]) || 0) + n);
          }
        }
        if (one(m, F.country) && row[s.country] != null) bump(facets.country, row[s.country], "samples", n);
        if (one(m, F.region) && row[s.admin1] != null) bump(facets.region, row[s.admin1], "samples", n);
        if (one(m, F.city) && row[s.place] != null) bump(facets.city, row[s.place], "samples", n);
        if (one(m, F.tier)) bump(facets.tier, tiers[row[s.tier]], "samples", n);
        if (one(m, F.context)) bump(facets.context, contexts[row[s.ctx]] || "", "samples", n);
        if (one(m, F.assay)) for (const a of SD.assaysets[row[s.assays]]) bump(facets.assay, assays[a], "samples", n);
        if (one(m, F.loc)) bump(facets.loc, geo.statuses[row[s.loc]], "samples", n);
        if (one(m, F.cyear) && row[s.cyear] != null) bump(facets.cyear, row[s.cyear], "samples", n);
        if (one(m, F.year) && row[s.year] != null) bump(facets.year, row[s.year], "samples", n);
      }
      for (const row of R){
        const m = runMask(c, row);
        if (m !== 0 && (m & (m - 1)) !== 0) continue;
        const n = row[r.n];
        if (m === 0){
          out.runs += n;
          out.gbases += row[r.gb] || 0;
          out.projRuns.set(row[r.proj], (out.projRuns.get(row[r.proj]) || 0) + n);
        }
        if (one(m, F.country) && row[r.country] != null) bump(facets.country, row[r.country], "runs", n);
        if (one(m, F.region) && row[r.admin1] != null) bump(facets.region, row[r.admin1], "runs", n);
        if (one(m, F.city) && row[r.place] != null) bump(facets.city, row[r.place], "runs", n);
        if (one(m, F.tier)) bump(facets.tier, tiers[row[r.tier]], "runs", n);
        if (one(m, F.context)) bump(facets.context, contexts[row[r.ctx]] || "", "runs", n);
        if (one(m, F.assay)) bump(facets.assay, assays[row[r.assay]], "runs", n);
        if (one(m, F.loc)) bump(facets.loc, geo.statuses[row[r.loc]], "runs", n);
        if (one(m, F.cyear) && row[r.cyear] != null) bump(facets.cyear, row[r.cyear], "runs", n);
        if (one(m, F.year) && row[r.year] != null) bump(facets.year, row[r.year], "runs", n);
      }

      const projectsOut = [];
      const ids = new Set([...out.projSamples.keys(), ...out.projRuns.keys()]);
      for (const i of ids){
        const info = projectInfo[i];
        projectsOut.push({ i, accession: info.accession, title: info.title || "", center_name: info.center_name || "",
                           first_year: info.first_year, last_year: info.last_year,
                           totalRuns: info.runs, totalSamples: info.samples,
                           samples: out.projSamples.get(i) || 0, runs: out.projRuns.get(i) || 0 });
      }
      projectsOut.sort((a, b) => b.samples - a.samples || b.runs - a.runs || a.accession.localeCompare(b.accession));
      return {
        samples: out.samples, runs: out.runs, gbases: out.gbases, projects: projectsOut,
        samplesNoCyear: out.samplesNoCyear, samplesNoCountry: out.samplesNoCountry, samplesNoPlace: out.samplesNoPlace,
        facets, points: out.points ? Array.from(out.points.values()) : null
      };
    }

    // Does one sample (from a project file) pass the filters? Used to list
    // matching samples and runs once a project's details are loaded.
    function sampleMatches(f, sample, runAssays){
      const tiersOk = !(f.tier || []).length || f.tier.includes(sample.urban_status || "unscreened");
      const ctxOk = !(f.context || []).length || f.context.includes(sample.urban_context || "");
      const accepted = ["verified", "probable"];
      const st = sample.location_status || "unresolved";
      const countryOk = !f.country || sample.country_iso2 === f.country;
      const placeOk = !f.city || String(sample.place_id ?? "") === String(f.city);
      const region = f.region ? geo.admin1ByCode.get(f.region) : null;
      const placeRec = sample.place_id != null ? geo.placeById.get(String(sample.place_id)) : null;
      const regionOk = !f.region || (placeRec ? (region && placeRec.admin1 === region.i) : sample.region === region?.name);
      const locOk = f.loc === "verified" ? st === "verified" : f.loc === "place" ? accepted.includes(st)
                  : f.loc === "country" ? !!sample.country_iso2 : true;
      const cy = sample.collection_year;
      const cyOk = inRange(cy == null ? null : cy, f.cmin, f.cmax);
      const assayOk = !(f.assay || []).length || (runAssays || []).some((a) => f.assay.includes(a));
      return tiersOk && ctxOk && countryOk && placeOk && regionOk && locOk && cyOk && assayOk;
    }

    return {
      geo, projects, projectAcc, projectInfo, assays, tiers, contexts,
      query, sampleMatches, emptyFilters,
      countryName: (iso) => geo.countryByIso.get(iso)?.name || iso,
      placeName: (id) => geo.placeById.get(String(id))?.name || String(id)
    };
  }

  // ---------------------------------------------------------------- legacy API
  // The Analytics page still works on record-shaped groups (one per run-cube
  // row, weighted by _w runs) and on per-run objects expanded from them.
  let groupsPromise = null;
  function fetchGroups(){
    if (!groupsPromise){
      groupsPromise = Promise.all([fetchCube(), fetchProjectsIndex(), fetchGeo()]).then(([cube, projects, geo]) => {
        const D = cube.dims;
        const ix = Object.fromEntries(cube.cols.map((k, i) => [k, i]));
        return cube.rows.map((row) => {
          const acc = D.project[row[ix.project]] || "";
          const p = projects.get(acc) || {};
          const year = row[ix.year];
          const ctx = D.context[row[ix.context]] || "";
          const c = row[ix.country] == null ? null : geo.countries[row[ix.country]];
          const pl = row[ix.place] == null ? null : geo.places[row[ix.place]];
          return {
            srr: "",
            _synthetic: true,
            _w: row[ix.runs],
            _gb: row[ix.gbases],
            runinfo_row: { BioProject: acc, ReleaseDate: year ? `${year}-01-01` : "", CenterName: p.center_name || "" },
            bioproject: { accession: acc, title: p.title || "", center_name: p.center_name || "" },
            geo: { country: c ? c.name : "", country_iso2: c ? c.iso2 : "", city: pl ? pl.name : "",
                   place_id: pl ? pl.geonames_id : "", lat: pl ? String(pl.lat) : "", lon: pl ? String(pl.lon) : "" },
            assay: { assay_class: D.assay[row[ix.assay]] || "Unknown" },
            eligibility: { status: D.tier[row[ix.tier]] || "unscreened", urban_context: ctx ? [ctx] : [] },
            ai_curation: row[ix.ai] == null ? {} : (D.ai[row[ix.ai]] || {}),
            ncbi: { bioproject_url: acc ? `https://www.ncbi.nlm.nih.gov/bioproject/${encodeURIComponent(acc)}` : "" }
          };
        });
      }).catch((err) => { groupsPromise = null; throw err; });
    }
    return groupsPromise;
  }

  let allRunsPromise = null;
  async function fetchAllRuns(progressCb){
    if (!allRunsPromise){
      allRunsPromise = (async () => {
        const [manifest, groups] = await Promise.all([fetchManifest(), fetchGroups()]);
        const allRuns = [];
        groups.forEach((g, gi) => {
          for (let k = 0; k < g._w; k += 1){
            const o = Object.create(g);
            o._r = `${gi}.${k}`;
            allRuns.push(o);
          }
        });
        if (progressCb) progressCb({ done: 1, total: 1, manifest, allRuns });
        return { manifest, allRuns };
      })().catch((err) => { allRunsPromise = null; throw err; });
    }
    return await allRunsPromise;
  }

  function yearFromRecord(rec){
    const raw = rec?.runinfo_row?.ReleaseDate || rec?.runinfo_row?.LoadDate || "";
    const match = raw.match(/^(\d{4})-/);
    return match ? parseInt(match[1], 10) : null;
  }
  const getRun = (rec) => rec?.srr || rec?.runinfo_row?.Run || rec?._r || "";
  const getBioProject = (rec) => rec?.runinfo_row?.BioProject || rec?.bioproject?.accession || "";
  const getBioSample = (rec) => rec?.runinfo_row?.BioSample || rec?.geo?.biosample_accession || "";
  const getAiCuration = (rec) => rec?.ai_curation || {};
  // Accepted (gazetteer-validated) geography only; AI proposals never change it.
  const getCountry = (rec) => rec?.geo?.country || "";
  const getCity = (rec) => rec?.geo?.city || "";
  const getAssay = (rec) => rec?.assay?.assay_class || rec?.runinfo_row?.LibraryStrategy || "Unknown";
  const getCenter = (rec) => rec?.runinfo_row?.CenterName || rec?.bioproject?.center_name || "";
  const getTitle = (rec) => rec?.bioproject?.title || rec?.title || "";

  function groupProjects(allRuns){
    const map = new Map();
    for (const rec of allRuns){
      const bp = getBioProject(rec) || "(unassigned)";
      if (!map.has(bp)){
        map.set(bp, { accession: bp, title: getTitle(rec), records: [], runs: new Set(), biosamples: new Set(),
                      countries: new Set(), cities: new Set(), assays: new Set(), centers: new Set(), years: new Set() });
      }
      const row = map.get(bp);
      row.records.push(rec);
      if (getRun(rec)) row.runs.add(getRun(rec));
      if (getBioSample(rec)) row.biosamples.add(getBioSample(rec));
      if (getCountry(rec)) row.countries.add(getCountry(rec));
      if (getCity(rec)) row.cities.add(getCity(rec));
      if (getAssay(rec)) row.assays.add(getAssay(rec));
      if (getCenter(rec)) row.centers.add(getCenter(rec));
      const year = yearFromRecord(rec);
      if (year) row.years.add(year);
      if (!row.title && getTitle(rec)) row.title = getTitle(rec);
    }
    return Array.from(map.values());
  }

  function tally(values){
    const counts = new Map();
    for (const value of values){
      const key = norm(value) || "(unknown)";
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    return Array.from(counts.entries()).map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  }

  // Run-level summary of record objects (Analytics). Exact totals come from fetchSummary().
  function summarize(allRuns){
    const projects = groupProjects(allRuns);
    const known = (d) => d.name !== "(unknown)";
    const countries = tally(allRuns.map(getCountry));
    const cities = tally(allRuns.map(getCity));
    return {
      totalRuns: allRuns.length, totalProjects: projects.length,
      totalBioSamples: new Set(allRuns.map(getBioSample).filter(Boolean)).size,
      totalCountries: countries.filter(known).length, totalCities: cities.filter(known).length,
      geoResolvedRuns: allRuns.filter((r) => getCountry(r) && getCity(r)).length,
      downloadableRuns: allRuns.length,
      years: tally(allRuns.map(yearFromRecord).filter(Boolean)), assays: tally(allRuns.map(getAssay)),
      countries, cities, centers: tally(allRuns.map(getCenter)), projects
    };
  }

  function escapeHtml(value){
    return (value ?? "").toString().replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }
  const formatNumber = (value) => new Intl.NumberFormat("en-US").format(value || 0);
  const topItems = (items, limit) => items.slice(0, limit);

  const api = {
    fetchManifest, fetchSummary, fetchGeo, fetchSampleCube, fetchCube, fetchProjectsIndex, fetchProject, shardName,
    openIndex, emptyFilters, filtersFromURL, filtersToParams, filtersToURL, DEFAULT_TIERS, FILTER_KEYS,
    fetchGroups, fetchAllRuns, groupProjects, summarize, topItems, formatNumber, escapeHtml, yearFromRecord,
    getCountry, getCity, getAssay, getCenter, getBioProject, getBioSample, getAiCuration
  };
  window.UMDB = api;
  window.UMDBDB = api;
})();
