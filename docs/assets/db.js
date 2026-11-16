(function(){
  // Compact site data (site schema 2): the site loads summary.json, cube.json
  // (runs aggregated by project, year, country, city, assay, tier, context and
  // rounded coordinates) and projects.json, and fetches one project's samples
  // and runs from db/p/<ACC>.json only when it is opened.
  const MANIFEST_URL = "./db/srr_records_manifest.json";
  const SUMMARY_URL = "./db/summary.json";
  const CUBE_URL = "./db/cube.json";
  const PROJECTS_URL = "./db/projects.json";
  let manifestPromise = null;
  let summaryPromise = null;
  let allRunsPromise = null;
  let cubePromise = null;
  let projectsPromise = null;
  let groupsPromise = null;
  const shardPromises = new Map();

  function fetchJson(url){
    return fetch(url, { cache: "default" }).then(async (res) => {
      if (!res.ok) throw new Error(`Unable to load ${url} (${res.status})`);
      return await res.json();
    });
  }

  function rowsToObjects(cols, rows){
    return (rows || []).map((row) => {
      const o = {};
      cols.forEach((k, i) => { o[k] = row[i]; });
      return o;
    });
  }

  function fetchCube(){
    if (!cubePromise) cubePromise = fetchJson(CUBE_URL).catch((err) => { cubePromise = null; throw err; });
    return cubePromise;
  }

  // Map of BioProject accession -> {title, center_name, data_type, submission_date, runs, samples, first_year, last_year}
  function fetchProjectsIndex(){
    if (!projectsPromise){
      projectsPromise = fetchJson(PROJECTS_URL).then((doc) => {
        const map = new Map();
        for (const o of rowsToObjects(doc.cols, doc.rows)) map.set(o.accession, o);
        return map;
      }).catch((err) => { projectsPromise = null; throw err; });
    }
    return projectsPromise;
  }

  // One record-shaped object per cube row, weighted: _w runs, _ns BioSamples.
  // They answer the same getters as run records (country, city, assay, year,
  // tier, project, centre), so filters and charts work without loading runs.
  function fetchGroups(){
    if (!groupsPromise){
      groupsPromise = Promise.all([fetchCube(), fetchProjectsIndex()]).then(([cube, projects]) => {
        const D = cube.dims;
        const ix = Object.fromEntries(cube.cols.map((k, i) => [k, i]));
        return cube.rows.map((row) => {
          const acc = D.project[row[ix.project]] || "";
          const p = projects.get(acc) || {};
          const year = row[ix.year];
          const ctx = D.context[row[ix.context]] || "";
          const lat = row[ix.lat], lon = row[ix.lon];
          return {
            srr: "",
            _synthetic: true,
            _w: row[ix.runs],
            _ns: row[ix.samples],
            _gb: row[ix.gbases],
            runinfo_row: { BioProject: acc, ReleaseDate: year ? `${year}-01-01` : "", CenterName: p.center_name || "" },
            bioproject: { accession: acc, title: p.title || "", center_name: p.center_name || "" },
            geo: { country: D.country[row[ix.country]] || "", city: D.city[row[ix.city]] || "",
                   lat: lat == null ? "" : String(lat), lon: lon == null ? "" : String(lon) },
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

  function shardName(acc){
    const a = norm(acc).toUpperCase();
    return (/^[A-Z0-9_.-]+$/.test(a) ? a : "_none") + ".json";
  }

  // Full samples and runs of one BioProject, as run records.
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
                   lat: s.lat == null ? "" : String(s.lat), lon: s.lon == null ? "" : String(s.lon),
                   biosample_accession: bs, resolution: s.location_resolution || "" },
            biosample: { accession: bs, title: s.title || "", collection_date: s.collection_date || "",
                         isolation_source: s.isolation_source || "", env_broad_scale: s.env_broad_scale || "",
                         env_local_scale: s.env_local_scale || "", env_medium: s.env_medium || "", host: s.host || "" },
            assay: { assay_class: r.assay_class || "Unknown" },
            eligibility: { status: r.urban_status || "unscreened",
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

  function norm(value){
    return (value ?? "").toString().trim();
  }

  function safeInt(value){
    const n = parseInt((value ?? "").toString(), 10);
    return Number.isFinite(n) ? n : null;
  }

  function normalizePartPath(path){
    let s = (path ?? "").toString().replaceAll("\\", "/");
    if (s.startsWith("docs/")) s = s.slice(5);
    if (!s.includes("/")) s = "db/" + s;
    if (s.startsWith("db/")) s = "./" + s;
    if (!s.startsWith("./")) s = "./" + s.replace(/^\/+/, "");
    return s;
  }

  function yearFromRecord(rec){
    const raw = rec?.runinfo_row?.ReleaseDate || rec?.runinfo_row?.LoadDate || "";
    const match = raw.match(/^(\d{4})-/);
    return match ? safeInt(match[1]) : null;
  }

  function getRun(rec){
    return rec?.srr || rec?.runinfo_row?.Run || rec?._r || "";
  }

  function getBioProject(rec){
    return rec?.runinfo_row?.BioProject || rec?.bioproject?.accession || "";
  }

  function getBioSample(rec){
    return rec?._bs || rec?.runinfo_row?.BioSample || rec?.geo?.biosample_accession || "";
  }

  function getAiCuration(rec){
    return rec?.ai_curation || {};
  }

  function getCountry(rec){
    return getAiCuration(rec)?.final_country || rec?.geo?.country || "";
  }

  function getCity(rec){
    return getAiCuration(rec)?.final_city || rec?.geo?.city || "";
  }

  function getAssay(rec){
    return getAiCuration(rec)?.final_assay_class || rec?.assay?.assay_class || rec?.runinfo_row?.LibraryStrategy || "Unknown";
  }

  function getCenter(rec){
    return rec?.runinfo_row?.CenterName || rec?.bioproject?.center_name || "";
  }

  function getTitle(rec){
    return rec?.bioproject?.title || rec?.title || "";
  }

  function isKnownGeo(value){
    const s = norm(value).toLowerCase();
    return !!s && s !== "(unknown)";
  }

  async function fetchManifest(){
    if (!manifestPromise){
      manifestPromise = fetch(MANIFEST_URL, { cache: "default" }).then(async (res) => {
        if (!res.ok) throw new Error("Unable to load manifest");
        return await res.json();
      }).catch((err) => {
        manifestPromise = null;
        throw err;
      });
    }
    return await manifestPromise;
  }

  async function fetchSummary(){
    if (!summaryPromise){
      summaryPromise = fetch(SUMMARY_URL, { cache: "default" }).then(async (res) => {
        if (!res.ok) throw new Error("Unable to load summary");
        return await res.json();
      }).catch(async () => {
        const { manifest, allRuns } = await fetchAllRuns();
        const summary = summarize(allRuns);
        const largestProject = summary.projects.slice().sort((a, b) => b.runs.size - a.runs.size)[0];
        return {
          generated_utc: manifest.generated_utc || manifest.generated || "",
          totalRuns: summary.totalRuns,
          totalProjects: summary.totalProjects,
          totalBioSamples: summary.totalBioSamples,
          totalCountries: summary.totalCountries,
          totalCities: summary.totalCities,
          geoResolvedRuns: summary.geoResolvedRuns,
          downloadableRuns: summary.downloadableRuns,
          years: summary.years,
          assays: summary.assays,
          countries: summary.countries,
          cities: summary.cities,
          centers: summary.centers,
          largestProject: largestProject ? {
            accession: largestProject.accession,
            title: largestProject.title,
            run_count: largestProject.runs.size,
            biosample_count: largestProject.biosamples.size,
            years: Array.from(largestProject.years).sort((a, b) => a - b)
          } : null,
          topProjects: []
        };
      }).catch((err) => {
        summaryPromise = null;
        throw err;
      });
    }
    return await summaryPromise;
  }

  // One lightweight object per run, expanded from the weighted cube rows, for
  // pages that count and filter runs (analytics, map). Each inherits its
  // group's fields; _r and _bs are stand-in run and BioSample keys so distinct
  // counts stay right. Real accessions come from fetchProject().
  async function fetchAllRuns(progressCb){
    if (!allRunsPromise){
      allRunsPromise = (async () => {
        const [manifest, groups] = await Promise.all([fetchManifest().catch(() => ({})), fetchGroups()]);
        const allRuns = [];
        groups.forEach((g, gi) => {
          for (let k = 0; k < g._w; k += 1){
            const o = Object.create(g);
            o._r = `${gi}.${k}`;
            o._bs = `${gi}.${g._ns ? k % g._ns : k}`;
            allRuns.push(o);
          }
        });
        if (progressCb) progressCb({ done: 1, total: 1, manifest, allRuns });
        return { manifest, allRuns };
      })().catch((err) => {
        allRunsPromise = null;
        throw err;
      });
    }
    return await allRunsPromise;
  }

  function groupProjects(allRuns){
    const map = new Map();
    for (const rec of allRuns){
      const bp = getBioProject(rec) || "(unassigned)";
      if (!map.has(bp)){
        map.set(bp, {
          accession: bp,
          title: getTitle(rec),
          records: [],
          runs: new Set(),
          biosamples: new Set(),
          countries: new Set(),
          cities: new Set(),
          assays: new Set(),
          centers: new Set(),
          years: new Set()
        });
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
    return Array.from(counts.entries())
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  }

  function summarize(allRuns){
    const projects = groupProjects(allRuns);
    const years = tally(allRuns.map(yearFromRecord).filter(Boolean));
    const assays = tally(allRuns.map(getAssay));
    const countries = tally(allRuns.map(getCountry));
    const cities = tally(allRuns.map(getCity));
    const centers = tally(allRuns.map(getCenter));

    const geoResolvedRuns = allRuns.filter((rec) => isKnownGeo(getCountry(rec)) && isKnownGeo(getCity(rec))).length;
    const downloadableRuns = allRuns.filter((rec) => norm(rec?.runinfo_row?.download_path)).length;

    return {
      totalRuns: allRuns.length,
      totalProjects: projects.length,
      totalBioSamples: new Set(allRuns.map(getBioSample).filter(Boolean)).size,
      totalCountries: countries.filter((d) => isKnownGeo(d.name)).length,
      totalCities: cities.filter((d) => isKnownGeo(d.name)).length,
      geoResolvedRuns,
      downloadableRuns,
      years,
      assays,
      countries,
      cities,
      centers,
      projects
    };
  }

  function topItems(items, limit){
    return items.slice(0, limit);
  }

  function formatNumber(value){
    return new Intl.NumberFormat("en-US").format(value || 0);
  }

  const api = {
    fetchManifest,
    fetchSummary,
    fetchAllRuns,
    fetchCube,
    fetchGroups,
    fetchProjectsIndex,
    fetchProject,
    shardName,
    summarize,
    topItems,
    formatNumber,
    yearFromRecord,
    getCountry,
    getCity,
    getAssay,
    getCenter,
    getBioProject,
    getBioSample,
    getAiCuration,
    groupProjects
  };

  // Keep both globals available so older pages and newer pages can share
  // the same data helpers without runtime errors.
  window.UMDB = api;
  window.UMDBDB = api;
})();
