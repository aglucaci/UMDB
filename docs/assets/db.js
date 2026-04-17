(function(){
  const MANIFEST_URL = "./db/srr_records_manifest.json";
  const SUMMARY_URL = "./db/summary.json";
  let manifestPromise = null;
  let summaryPromise = null;
  let allRunsPromise = null;

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
    return rec?.srr || rec?.runinfo_row?.Run || "";
  }

  function getBioProject(rec){
    return rec?.runinfo_row?.BioProject || rec?.bioproject?.accession || "";
  }

  function getBioSample(rec){
    return rec?.runinfo_row?.BioSample || rec?.geo?.biosample_accession || "";
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

  async function fetchAllRuns(progressCb){
    if (!allRunsPromise){
      allRunsPromise = (async () => {
        const manifest = await fetchManifest();
        const parts = manifest.parts || [];
        const allRuns = [];

        for (let i = 0; i < parts.length; i += 1){
          const url = normalizePartPath(parts[i].path);
          const res = await fetch(url, { cache: "default" });
          if (!res.ok) throw new Error("Unable to load " + url);
          const chunk = await res.json();
          if (Array.isArray(chunk)) allRuns.push(...chunk);
          if (progressCb) progressCb({ done: i + 1, total: parts.length, manifest, allRuns });
        }

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
