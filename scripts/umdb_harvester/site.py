"""Website data for GitHub Pages, built from the SQLite database.

The site never ships every run record. It loads a few small files and fetches
one project's details only when someone opens it:

  docs/db/summary.json     exact headline counts, each labelled with its unit
                           (runs, BioSamples, BioProjects), computed in SQLite
  docs/db/geo.json         the validated location hierarchy: ISO countries,
                           GeoNames admin1 regions and places that samples use
  docs/db/samples.json     BioSamples aggregated by project set, collection
                           year, country, region, place, assay set, eligibility
                           tier, context, location status and rounded
                           coordinates. One sample is counted in exactly one
                           row, so any filter's sample total is exact.
  docs/db/cube.json        runs aggregated by project, release year, country,
                           region, place, assay, tier, context and AI review
  docs/db/projects.json    one row per BioProject (title, centre, counts)
  docs/db/p/<ACC>.json     one project's samples and runs, loaded on demand
  docs/db/ix/<NN>.json     accession lookup: BioSample and run accession -> row
                           of projects.json, split by the accession's last two
                           digits, so a /sample or /run page finds its project file
  docs/db/collections.json curated collections with statistics, and
  docs/db/collections/<slug>.tsv.gz  their run lists (see cohorts.py)
  docs/db/manifest.json    build metadata and file list

All geography comes from `sample_locations` (gazetteer-validated, see geo.py):
a country is published when its status is verified or probable, a place only
when the place status is verified or probable. Raw location text appears only
in the per-project files, next to the accepted annotation. AI proposals are
shown as proposals and never change counts or filters.

Files whose content has not changed are left untouched, so a harvest only
rewrites the projects it changed.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .config import DOCS_DIR
from .database import DB_PATH, SCHEMA_VERSION
from .utils import log, paint, num, fmt_duration, utc_now, scrub_secrets

SITE_SCHEMA_VERSION = "3.0.0"
SITE_DB_DIR = os.path.join(DOCS_DIR, "db")
SHARD_DIR = os.path.join(SITE_DB_DIR, "p")
NO_PROJECT = "_none"
# Files of the old run-record export, removed when the compact site is built.
LEGACY_FILES = re.compile(r"^(srr_records(_part\d{3})?\.json|biosamples\.json|srr_index\.json|bioprojects\.json)$")

ACCEPTED = ("verified", "probable")
LOCATION_STATUSES = ["verified", "probable", "ambiguous", "conflicting", "unresolved"]

SAMPLE_COLS = ["accession", "title", "collection_date", "country", "region", "city", "lat", "lon",
               "urban_status", "urban_context", "eligibility_reason", "isolation_source",
               "env_broad_scale", "env_local_scale", "env_medium", "host", "location_resolution",
               "country_iso2", "place_id", "location_status", "location_method", "location_note",
               "geo_loc_raw", "collection_year"]
RUN_COLS = ["accession", "sample", "experiment", "assay_class", "library_strategy", "library_source",
            "library_selection", "library_layout", "platform", "instrument_model", "release_date",
            "spots", "bases", "avg_length", "size_mb", "scientific_name", "urban_status", "experiment_title"]
# Sample cube: `projects` and `assays` are indexes into dims.projsets / dims.assaysets
# (a sample can belong to several projects and have runs of several assays);
# country / admin1 / place index geo.json lists; loc indexes LOCATION_STATUSES.
# `year` is the SRA release year (a sample's earliest run), `cyear` the collection year.
SAMPLE_CUBE_COLS = ["projects", "year", "cyear", "country", "admin1", "place", "assays", "tier", "context", "loc",
                    "lat", "lon", "samples"]
RUN_CUBE_COLS = ["project", "year", "cyear", "country", "admin1", "place", "assay", "tier", "context", "loc", "ai",
                 "runs", "gbases"]


def _dump(obj: Any) -> bytes:
    return scrub_secrets(json.dumps(obj, ensure_ascii=False, separators=(",", ":"))).encode("utf-8")


def _write_if_changed(path: str, data: bytes) -> bool:
    try:
        with open(path, "rb") as fh:
            if fh.read() == data:
                return False
    except FileNotFoundError:
        pass
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)
    return True


def shard_name(accession: str) -> str:
    acc = (accession or "").strip().upper()
    return (acc if re.fullmatch(r"[A-Z0-9_.-]+", acc or "") else NO_PROJECT) + ".json"


class _Dict:
    """Value -> index dictionary for dictionary-encoded columns."""

    def __init__(self):
        self.values: List[Any] = []
        self.index: Dict[Any, int] = {}

    def __call__(self, value: Any) -> int:
        key = json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
        i = self.index.get(key)
        if i is None:
            i = self.index[key] = len(self.values)
            self.values.append(value)
        return i


def _round(x: Any, nd: int) -> Any:
    return None if x is None else round(float(x), nd)


def _sort_key(row: List[Any]) -> tuple:
    return tuple((v is None, v if v is not None else 0) for v in row)


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE name = ?", (name,)).fetchone() is not None


def build_site(db_path: str = DB_PATH, out_dir: str = SITE_DB_DIR) -> Dict[str, Any]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} not found; run `umdb db build` first")
    t0 = time.time()
    shard_dir = os.path.join(out_dir, "p")
    os.makedirs(shard_dir, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
    log(f"Building website data in {out_dir}/ from {db_path}")
    if not _has_table(con, "sample_locations"):
        raise RuntimeError(f"{db_path} predates gazetteer validation (schema {meta.get('schema_version')}); "
                           f"run `umdb db build`")
    if not con.execute("SELECT 1 FROM sample_locations LIMIT 1").fetchone():
        log(paint("  no validated locations in the database (GeoNames unavailable at build time): "
                  "every sample is published without country or place", "warn"))

    ai: Dict[str, Dict[str, Any]] = {}
    for r in con.execute("SELECT * FROM ai_annotations"):
        ai[r["run_accession"]] = {k: r[k] for k in ("urban_origin", "metadata_sufficiency", "final_country",
                                                   "final_city", "final_assay_class") if r[k]}
        ai[r["run_accession"]]["reviewed"] = True
        ai[r["run_accession"]]["status"] = "proposed"

    projects = {r["accession"]: dict(r) for r in con.execute(
        "SELECT accession, title, description, organism, data_type, center_name, submission_date, last_update FROM projects")}

    # --- accepted geography -------------------------------------------------
    loc: Dict[str, Dict[str, Any]] = {}
    for r in con.execute("""
            SELECT l.sample_accession AS s, l.country_iso2, l.country_status, l.admin1_code, l.place_id,
                   l.status, l.method, l.note, l.distance_km
            FROM sample_locations l"""):
        iso = r["country_iso2"] if r["country_status"] in ACCEPTED else None
        loc[r["s"]] = {"country": iso, "admin1": r["admin1_code"] if iso else None,
                       "place": r["place_id"] if (iso and r["status"] in ACCEPTED) else None,
                       "status": r["status"], "method": r["method"], "note": r["note"]}
    countries = {r["iso2"]: dict(r) for r in con.execute("SELECT iso2, iso_numeric, name, continent FROM countries")}
    population = _country_population()
    admin1 = {r["code"]: dict(r) for r in con.execute("SELECT code, country_iso2, name FROM admin1")}
    places = {r["geonames_id"]: dict(r) for r in con.execute(
        "SELECT geonames_id, name, country_iso2, admin1_code, lat, lon, population, feature_code FROM places")}

    used_c = sorted({v["country"] for v in loc.values() if v["country"]}, key=lambda c: countries[c]["name"])
    used_a = sorted({v["admin1"] for v in loc.values() if v["admin1"] and v["admin1"] in admin1},
                    key=lambda a: (admin1[a]["country_iso2"], admin1[a]["name"]))
    used_p = sorted({v["place"] for v in loc.values() if v["place"]},
                    key=lambda p: (places[p]["country_iso2"], places[p]["name"]))
    ci = {c: i for i, c in enumerate(used_c)}
    ai1 = {a: i for i, a in enumerate(used_a)}
    pi = {p: i for i, p in enumerate(used_p)}
    geo_doc = {
        "v": SITE_SCHEMA_VERSION,
        "attribution": "Place names and identifiers: GeoNames (https://www.geonames.org/), CC BY 4.0",
        "geo_rules_version": meta.get("geo_rules_version"),
        "statuses": LOCATION_STATUSES,
        "country_cols": ["iso2", "numeric", "name", "continent", "population"],
        "countries": [[c, countries[c]["iso_numeric"], countries[c]["name"], countries[c]["continent"],
                       population.get(c)] for c in used_c],
        "admin1_cols": ["code", "country", "name"],
        "admin1": [[a, ci.get(admin1[a]["country_iso2"]), admin1[a]["name"]] for a in used_a],
        "place_cols": ["geonames_id", "name", "country", "admin1", "lat", "lon", "population"],
        "places": [[p, places[p]["name"], ci.get(places[p]["country_iso2"]),
                    ai1.get(places[p]["admin1_code"]), round(places[p]["lat"], 4), round(places[p]["lon"], 4),
                    places[p]["population"]] for p in used_p],
    }

    def geo_idx(sample: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[int], int]:
        g = loc.get(sample or "")
        if not g:
            return None, None, None, LOCATION_STATUSES.index("unresolved")
        return (ci.get(g["country"]), ai1.get(g["admin1"]), pi.get(g["place"]),
                LOCATION_STATUSES.index(g["status"]) if g["status"] in LOCATION_STATUSES else 4)

    # --- runs ------------------------------------------------------------------
    runs = con.execute("""
        SELECT r.accession, r.project_accession AS project, r.sample_accession AS sample, r.experiment_accession AS experiment,
               r.release_date, r.release_year, r.spots, r.bases, r.avg_length, r.size_mb, r.scientific_name,
               r.center_name, r.urban_status, e.assay_class, e.library_strategy, e.library_source,
               e.library_selection, e.library_layout, e.platform, e.instrument_model, e.title AS experiment_title,
               s.title AS s_title, s.collection_date_raw, s.collection_year, s.geo_loc_raw, s.lat, s.lon,
               s.urban_status AS s_status, s.urban_context, s.eligibility_reason, s.isolation_source,
               s.env_broad_scale, s.env_local_scale, s.env_medium, s.host, s.location_resolution
        FROM runs r
        LEFT JOIN experiments e ON e.accession = r.experiment_accession
        LEFT JOIN samples s ON s.accession = r.sample_accession
        ORDER BY r.project_accession, r.sample_accession, r.accession
    """).fetchall()
    log(f"  {num(len(runs))} runs, {num(len(projects))} projects, {num(len(ai))} AI reviews")

    d_project, d_assay, d_tier, d_ctx, d_ai = (_Dict() for _ in range(5))
    run_cube: Dict[tuple, List[float]] = {}
    by_project: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    sample_info: Dict[str, Dict[str, Any]] = {}
    for r in runs:
        a = ai.get(r["accession"], {})
        c, a1, pl, ls = geo_idx(r["sample"])
        key = (d_project(r["project"] or ""), r["release_year"], r["collection_year"], c, a1, pl,
               d_assay(r["assay_class"] or "Unknown"), d_tier(r["urban_status"] or "unscreened"),
               d_ctx(r["urban_context"] or ""), ls, d_ai(a) if a else -1)
        cell = run_cube.get(key)
        if cell is None:
            cell = run_cube[key] = [0, 0]
        cell[0] += 1
        cell[1] += r["bases"] or 0
        by_project[r["project"] or ""].append(r)
        if r["sample"]:
            si = sample_info.get(r["sample"])
            if si is None:
                si = sample_info[r["sample"]] = {"row": r, "projects": set(), "assays": set(), "year": None}
            if r["release_year"] and (si["year"] is None or r["release_year"] < si["year"]):
                si["year"] = r["release_year"]
            si["projects"].add(r["project"] or "")
            si["assays"].add(r["assay_class"] or "Unknown")

    run_rows = [list(k[:10]) + [None if k[10] < 0 else k[10], v[0], round(v[1] / 1e9, 3)]
                for k, v in run_cube.items()]
    run_rows.sort(key=_sort_key)

    # --- sample cube (one sample = one row contribution) -------------------------
    d_projset, d_assayset = _Dict(), _Dict()
    sample_cube: Dict[tuple, int] = defaultdict(int)
    for acc, si in sample_info.items():
        r = si["row"]
        c, a1, pl, ls = geo_idx(acc)
        projset = d_projset(sorted(d_project(p) for p in si["projects"]))
        assayset = d_assayset(sorted(d_assay(x) for x in si["assays"]))
        key = (projset, si["year"], r["collection_year"], c, a1, pl, assayset, d_tier(r["s_status"] or "unscreened"),
               d_ctx(r["urban_context"] or ""), ls, _round(r["lat"], 2), _round(r["lon"], 2))
        sample_cube[key] += 1
    sample_rows = [list(k) + [n] for k, n in sample_cube.items()]
    sample_rows.sort(key=_sort_key)

    shared_dims = {"project": d_project.values, "assay": d_assay.values, "tier": d_tier.values,
                   "context": d_ctx.values}
    samples_doc = {"v": SITE_SCHEMA_VERSION, "unit": "BioSamples", "cols": SAMPLE_CUBE_COLS,
                   "dims": {**shared_dims, "projsets": d_projset.values, "assaysets": d_assayset.values,
                            "loc": LOCATION_STATUSES},
                   "rows": sample_rows}
    cube_doc = {"v": SITE_SCHEMA_VERSION, "unit": "runs", "cols": RUN_CUBE_COLS,
                "dims": {**shared_dims, "ai": d_ai.values}, "rows": run_rows}

    # --- projects index + shards --------------------------------------------
    project_rows = []
    written = unchanged = 0
    keep = set()
    for acc in sorted(by_project):
        rs = by_project[acc]
        p = projects.get(acc) or {}
        samples: Dict[str, int] = {}
        s_rows, r_rows = [], []
        for r in rs:
            sid = r["sample"] or ""
            if sid not in samples:
                samples[sid] = len(s_rows)
                g = loc.get(sid) or {}
                c = countries.get(g.get("country") or "", {})
                pl = places.get(g.get("place") or -1, {})
                a1 = admin1.get(g.get("admin1") or "", {})
                s_rows.append([sid, r["s_title"], r["collection_date_raw"], c.get("name"), a1.get("name"), pl.get("name"),
                               r["lat"], r["lon"], r["s_status"], r["urban_context"], r["eligibility_reason"],
                               r["isolation_source"], r["env_broad_scale"], r["env_local_scale"], r["env_medium"],
                               r["host"], r["location_resolution"], g.get("country"), g.get("place"),
                               g.get("status") or "unresolved", g.get("method"), g.get("note"),
                               r["geo_loc_raw"], r["collection_year"]])
            r_rows.append([r["accession"], samples[sid], r["experiment"], r["assay_class"], r["library_strategy"],
                           r["library_source"], r["library_selection"], r["library_layout"], r["platform"],
                           r["instrument_model"], r["release_date"], r["spots"], r["bases"], r["avg_length"],
                           r["size_mb"], r["scientific_name"], r["urban_status"], r["experiment_title"]])
        years = [r["release_year"] for r in rs if r["release_year"]]
        centers = defaultdict(int)
        for r in rs:
            if r["center_name"]:
                centers[r["center_name"]] += 1
        center = p.get("center_name") or (max(centers, key=centers.get) if centers else "")
        shard = {
            "v": SITE_SCHEMA_VERSION,
            "project": {"accession": acc, "title": p.get("title"), "description": p.get("description"),
                        "organism": p.get("organism"), "data_type": p.get("data_type"), "center_name": center,
                        "submission_date": p.get("submission_date"), "last_update": p.get("last_update")},
            "sample_cols": SAMPLE_COLS, "samples": s_rows,
            "run_cols": RUN_COLS, "runs": r_rows,
            "ai": {r["accession"]: ai[r["accession"]] for r in rs if r["accession"] in ai},
        }
        name = shard_name(acc)
        keep.add(name)
        if _write_if_changed(os.path.join(shard_dir, name), _dump(shard)):
            written += 1
        else:
            unchanged += 1
        n_samples = len([s for s in samples if s])
        project_rows.append([acc, p.get("title") or "", center, p.get("data_type") or "",
                             p.get("submission_date") or "", len(rs), n_samples,
                             min(years) if years else None, max(years) if years else None])

    removed = 0
    for name in os.listdir(shard_dir):
        if name.endswith(".json") and name not in keep:
            os.remove(os.path.join(shard_dir, name))
            removed += 1

    summary = _summary(con, project_rows)
    ix_files = _write_accession_index(out_dir, by_project, [r[0] for r in project_rows])
    files_extra = {"population.json": _dump({
        "v": 1, "source": "GeoNames countryInfo.txt (CC BY 4.0)", "cols": ["numeric", "iso2", "name", "continent", "population"],
        "rows": [[c["iso_numeric"], iso, c["name"], c["continent"], population.get(iso)]
                 for iso, c in sorted(countries.items()) if population.get(iso)]})}
    from .cohorts import build_collections, write_templates
    collections = build_collections(con, out_dir)
    write_templates(os.path.join(os.path.dirname(out_dir), "assets", "cohort-templates.json"))

    files = {
        "summary.json": _dump(summary),
        "geo.json": _dump(geo_doc),
        "samples.json": _dump(samples_doc),
        "cube.json": _dump(cube_doc),
        "projects.json": _dump({"v": SITE_SCHEMA_VERSION,
                                "cols": ["accession", "title", "center_name", "data_type", "submission_date",
                                         "runs", "samples", "first_year", "last_year"],
                                "rows": project_rows}),
    }
    # Pages that only read generated_utc and the part count keep working.
    files["srr_records_manifest.json"] = _dump({"generated_utc": summary["generated_utc"], "total_records": len(runs),
                                                "parts": [], "format": "compact-v3", "see": "manifest.json"})
    files_extra["analytics.json"] = _dump(_analytics(con))
    files.update(files_extra)
    for name, data in files.items():
        _write_if_changed(os.path.join(out_dir, name), data)

    legacy_removed = []
    for name in os.listdir(out_dir):
        if LEGACY_FILES.match(name):
            os.remove(os.path.join(out_dir, name))
            legacy_removed.append(name)

    sizes = {n: len(d) for n, d in files.items()}
    shard_bytes = sum(os.path.getsize(os.path.join(shard_dir, n)) for n in os.listdir(shard_dir))
    manifest = {
        "site_schema_version": SITE_SCHEMA_VERSION,
        "database_schema_version": SCHEMA_VERSION,
        "generated_utc": summary["generated_utc"],
        "database_built_utc": meta.get("built_utc"),
        "query_profiles_version": meta.get("query_profiles_version"),
        "eligibility_rules_version": meta.get("rules_version"),
        "geo_rules_version": meta.get("geo_rules_version"),
        "geonames_downloaded_utc": meta.get("geonames_downloaded_utc"),
        "counts": {"runs": summary["totalRuns"], "biosamples": summary["totalBioSamples"],
                   "projects": summary["totalProjects"], "sample_cube_rows": len(sample_rows),
                   "run_cube_rows": len(run_rows)},
        "files": {n: {"bytes": b} for n, b in sizes.items()},
        "project_shards": {"dir": "p/", "count": len(keep), "bytes": shard_bytes},
        "accession_index": {"dir": "ix/", "files": ix_files},
        "collections": [c["slug"] for c in collections["collections"]],
    }
    _write_if_changed(os.path.join(out_dir, "manifest.json"), json.dumps(manifest, indent=2).encode())
    con.close()

    log("  " + " · ".join(f"{n} {sizes[n] / 1e6:.2f} MB" for n in ("summary.json", "geo.json", "samples.json",
                                                                       "cube.json", "projects.json")))
    log(f"  sample cube {num(len(sample_rows))} rows, run cube {num(len(run_rows))} rows")
    log(f"  p/: {num(len(keep))} project files ({shard_bytes / 1e6:.0f} MB): {num(written)} written, "
        f"{num(unchanged)} unchanged, {num(removed)} removed")
    if legacy_removed:
        log(f"  removed old run-record files: {', '.join(sorted(legacy_removed))}")
    tiers = "  ".join(f"{t['name']} {t['biosamples']:,}" for t in summary["eligibility"])
    log(f"{paint('Website data ready', 'good')} in {fmt_duration(time.time() - t0)}  (BioSamples: {tiers})")
    return manifest


def _country_population() -> Dict[str, int]:
    """Country populations from GeoNames countryInfo.txt (shipped with the catalog), for per-capita map layers."""
    from .config import DATA_DIR
    path = os.path.join(DATA_DIR, "reference", "geonames", "countryInfo.txt")
    out: Dict[str, int] = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) > 7 and f[7].isdigit():
                out[f[0]] = int(f[7])
    return out


def ix_bucket(accession: str) -> str:
    """Lookup-index file for an accession: its last two digits (00-99), or 'xx'."""
    digits = re.sub(r"\D", "", accession or "")
    return digits[-2:].zfill(2) if digits else "xx"


def _write_accession_index(out_dir: str, by_project: Dict[str, List[sqlite3.Row]], project_order: List[str]) -> int:
    """docs/db/ix/<NN>.json: {accession: index into projects.json rows} for every BioSample and run."""
    pos = {acc: i for i, acc in enumerate(project_order)}
    buckets: Dict[str, Dict[str, int]] = defaultdict(dict)
    for acc, rs in by_project.items():
        i = pos.get(acc)
        if i is None:
            continue
        for r in rs:
            buckets[ix_bucket(r["accession"])][r["accession"]] = i
            if r["sample"]:
                b = buckets[ix_bucket(r["sample"])]
                b[r["sample"]] = min(b.get(r["sample"], i), i)
    ix_dir = os.path.join(out_dir, "ix")
    os.makedirs(ix_dir, exist_ok=True)
    for name, entries in buckets.items():
        _write_if_changed(os.path.join(ix_dir, f"{name}.json"),
                          _dump({"v": 1, "project_index": "projects.json rows", "entries": dict(sorted(entries.items()))}))
    return len(buckets)


def _analytics(con: sqlite3.Connection) -> Dict[str, Any]:
    """Precomputed catalog-structure statistics for the Analytics page (docs/db/analytics.json)."""
    import math
    urban = "('confirmed_urban','probable_urban')"
    # Study concentration: urban BioSamples per BioProject -> Lorenz curve, Gini, top shares
    sizes = sorted(n for (n,) in con.execute(f"""
        SELECT count(DISTINCT ps.sample_accession) FROM project_samples ps JOIN samples s ON s.accession = ps.sample_accession
        WHERE s.urban_status IN {urban} GROUP BY ps.project_accession"""))
    total = sum(sizes) or 1
    cum, lorenz, acc = 0, [[0.0, 0.0]], 0
    for i, n in enumerate(sizes, 1):
        acc += n
        if i == len(sizes) or i % max(1, len(sizes) // 100) == 0:
            lorenz.append([round(i / len(sizes), 4), round(acc / total, 4)])
    k = len(sizes)
    gini = (2 * sum((i + 1) * n for i, n in enumerate(sizes)) / (k * total) - (k + 1) / k) if k else 0
    top1 = max(1, math.ceil(0.01 * k))
    conc = {"studies": k, "gini": round(gini, 4), "lorenz": lorenz,
            "top1pct_share": round(sum(sizes[-top1:]) / total, 4), "top10_share": round(sum(sizes[-10:]) / total, 4)}
    # Collection-to-release delay (years) for urban samples with a full collection date
    delays = [d for (d,) in con.execute(f"""
        SELECT (julianday(min(r.release_date)) - julianday(s.collection_date)) / 365.25
        FROM samples s JOIN runs r ON r.sample_accession = s.accession
        WHERE s.urban_status IN {urban} AND s.collection_date IS NOT NULL AND r.release_date IS NOT NULL
        GROUP BY s.accession""") if d is not None]
    neg = sum(1 for d in delays if d < 0)
    pos = sorted(d for d in delays if d >= 0)
    bins = [{"label": f"{y}" if y < 10 else "10+", "samples": sum(1 for d in pos if (y <= d < y + 1) or (y == 10 and d >= 10))}
            for y in range(0, 11)]
    delay = {"samples": len(delays), "negative": neg, "median_years": pos[len(pos) // 2] if pos else 0, "bins": bins}
    # Library size: log10 bases per run, shotgun (WGS) runs
    lib = {}
    for assay in ("WGS", "16S", "Amplicon", "Metatranscriptomics"):
        b = sorted(x for (x,) in con.execute("""SELECT r.bases FROM runs r JOIN experiments e ON e.accession = r.experiment_accession
                                                WHERE e.assay_class = ? AND r.bases > 0""", (assay,)))
        edges = [6 + 0.5 * i for i in range(13)]  # 1 Mbp .. 1 Tbp
        hist = [{"label": f"{lo:g}", "runs": sum(1 for x in b if lo <= math.log10(x) < lo + 0.5)} for lo in edges]
        lib[assay] = {"assay": assay, "runs": len(b), "median_bases": b[len(b) // 2] if b else 0, "bins": hist}
    # Completeness by primary assay (urban samples; a sample counts under each assay it has)
    fields = ["has_place", "has_coordinates", "has_collection_date", "has_env_broad_scale", "has_isolation_source"]
    rows = []
    for assay, n, place, coords, date, env, iso in con.execute(f"""
        SELECT a.assay_class, count(*),
               avg(l.status IN ('verified','probable')) * 100, avg(s.lat IS NOT NULL AND s.lon IS NOT NULL) * 100,
               avg(s.collection_date IS NOT NULL) * 100, avg(s.env_broad_scale IS NOT NULL) * 100,
               avg(s.isolation_source IS NOT NULL) * 100
        FROM (SELECT DISTINCT r.sample_accession AS acc, coalesce(e.assay_class,'Unknown') AS assay_class
              FROM runs r JOIN experiments e ON e.accession = r.experiment_accession) a
        JOIN samples s ON s.accession = a.acc LEFT JOIN sample_locations l ON l.sample_accession = s.accession
        WHERE s.urban_status IN {urban} GROUP BY a.assay_class ORDER BY count(*) DESC"""):
        rows.append({"assay": assay, "samples": n, **{f: round(v or 0, 1) for f, v in zip(fields, (place, coords, date, env, iso))}})
    return {"v": 1, "unit": "urban (confirmed or probable) BioSamples unless stated", "study_concentration": conc,
            "release_delay": delay, "library_size": lib["WGS"], "library_size_by_assay": lib,
            "completeness": {"fields": fields, "rows": rows}}


def _summary(con: sqlite3.Connection, project_rows: List[List[Any]]) -> Dict[str, Any]:
    """Headline numbers straight from SQLite. Every list says what it counts."""
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    one = lambda sql, *a: con.execute(sql, a).fetchone()[0]
    acc = "l.country_status IN ('verified','probable')"
    placed = "l.status IN ('verified','probable')"

    total_runs = one("SELECT count(*) FROM runs")
    total_samples = one("SELECT count(DISTINCT sample_accession) FROM runs WHERE sample_accession IS NOT NULL")
    total_projects = one("SELECT count(DISTINCT project_accession) FROM runs WHERE project_accession IS NOT NULL")

    tiers = ("confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental", "unscreened")
    run_tier = dict(q("SELECT coalesce(urban_status,'unscreened'), count(*) FROM runs GROUP BY 1"))
    sample_tier = dict(q("SELECT coalesce(urban_status,'unscreened'), count(*) FROM samples GROUP BY 1"))

    countries = q(f"""
        SELECT c.iso2, c.name, count(DISTINCT l.sample_accession) AS samples,
               (SELECT count(*) FROM runs r JOIN sample_locations l2 ON l2.sample_accession = r.sample_accession
                 WHERE l2.country_iso2 = c.iso2 AND l2.country_status IN ('verified','probable')) AS runs
        FROM sample_locations l JOIN countries c ON c.iso2 = l.country_iso2
        WHERE {acc} GROUP BY c.iso2 ORDER BY samples DESC, c.name""")
    cities = q(f"""
        SELECT p.geonames_id, p.name, p.country_iso2, count(DISTINCT l.sample_accession) AS samples,
               (SELECT count(*) FROM runs r JOIN sample_locations l2 ON l2.sample_accession = r.sample_accession
                 WHERE l2.place_id = p.geonames_id) AS runs
        FROM sample_locations l JOIN places p ON p.geonames_id = l.place_id
        WHERE {placed} GROUP BY p.geonames_id ORDER BY samples DESC, p.name LIMIT 200""")
    loc_status = dict(q("SELECT status, count(*) FROM sample_locations GROUP BY 1"))
    country_status = dict(q("SELECT country_status, count(*) FROM sample_locations GROUP BY 1"))
    placed_runs = one(f"SELECT count(*) FROM runs r JOIN sample_locations l ON l.sample_accession = r.sample_accession "
                      f"WHERE {placed}")
    placed_samples = one(f"SELECT count(*) FROM sample_locations l WHERE {placed}")
    n_countries = one(f"SELECT count(DISTINCT country_iso2) FROM sample_locations l WHERE {acc}")
    n_places = one(f"SELECT count(DISTINCT place_id) FROM sample_locations l WHERE {placed}")

    years = q("SELECT release_year, count(*) FROM runs WHERE release_year IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")
    cyears = q("SELECT collection_year, count(*) FROM samples WHERE collection_year IS NOT NULL GROUP BY 1 ORDER BY 1")
    assays = q("""SELECT coalesce(e.assay_class,'Unknown'), count(*), count(DISTINCT r.sample_accession)
                  FROM runs r LEFT JOIN experiments e ON e.accession = r.experiment_accession GROUP BY 1 ORDER BY 2 DESC""")
    centers = q("SELECT coalesce(center_name,'(unknown)'), count(*) FROM runs GROUP BY 1 ORDER BY 2 DESC LIMIT 100")

    largest = sorted((r for r in project_rows if r[0]), key=lambda r: -r[5])[:25]
    top_projects = [{"accession": r[0], "title": r[1], "run_count": r[5], "biosample_count": r[6],
                     "years": [y for y in (r[7], r[8]) if y]} for r in largest]
    return {
        "generated_utc": utc_now(),
        "units": {"runs": "sequencing runs", "biosamples": "distinct BioSamples", "projects": "distinct BioProjects"},
        "totalRuns": total_runs,
        "totalBioSamples": total_samples,
        "totalProjects": total_projects,
        "runsWithoutProject": one("SELECT count(*) FROM runs WHERE project_accession IS NULL"),
        "totalCountries": n_countries,
        "totalCities": n_places,
        "geoResolvedRuns": placed_runs,
        "geoResolvedBioSamples": placed_samples,
        "downloadableRuns": total_runs,
        "location": {"unit": "BioSamples", "place_status": {k: loc_status.get(k, 0) for k in LOCATION_STATUSES},
                     "country_status": {k: country_status.get(k, 0)
                                        for k in ("verified", "probable", "conflicting", "unresolved")}},
        "eligibility": [{"name": t, "runs": run_tier.get(t, 0), "biosamples": sample_tier.get(t, 0)}
                        for t in tiers if run_tier.get(t) or sample_tier.get(t)],
        "years": [{"name": str(y), "count": n} for y, n in years],                      # runs by release year
        "collectionYears": [{"name": str(y), "count": n} for y, n in cyears],           # BioSamples
        "assays": [{"name": a, "count": n, "biosamples": s} for a, n, s in assays],     # count = runs
        "countries": [{"iso2": c, "name": name, "count": runs, "biosamples": s, "runs": runs}
                      for c, name, s, runs in countries],
        "cities": [{"geonames_id": g, "name": name, "country": c, "count": runs, "biosamples": s, "runs": runs}
                   for g, name, c, s, runs in cities],
        "centers": [{"name": n, "count": k} for n, k in centers],                       # runs
        "largestProject": top_projects[0] if top_projects else None,
        "topProjects": top_projects,
    }
