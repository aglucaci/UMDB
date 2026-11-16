"""Website data for GitHub Pages, built from the SQLite database.

The site no longer ships every run record. It loads three small files and
fetches one project's details only when someone opens it:

  docs/db/summary.json     headline numbers and top lists (index, analytics)
  docs/db/cube.json        runs aggregated by project, release year, country,
                           city, assay, eligibility tier, context, AI review and
                           rounded coordinates; every filter and chart on the
                           site works from this (about 20x fewer rows than runs)
  docs/db/projects.json    one row per BioProject (title, centre, counts)
  docs/db/p/<ACC>.json     one project's samples and runs, loaded on demand
  docs/db/manifest.json    build metadata and file list

Files whose content has not changed are left untouched, so a harvest only
rewrites the projects it changed. Download URLs and verbatim NCBI records stay
in the database and the Parquet releases.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List

from .config import DOCS_DIR
from .database import DB_PATH, SCHEMA_VERSION
from .utils import log, paint, num, fmt_duration, utc_now, scrub_secrets

SITE_SCHEMA_VERSION = "2.0.0"
SITE_DB_DIR = os.path.join(DOCS_DIR, "db")
SHARD_DIR = os.path.join(SITE_DB_DIR, "p")
NO_PROJECT = "_none"
# Files of the old run-record export, removed when the compact site is built.
LEGACY_FILES = re.compile(r"^(srr_records(_part\d{3})?\.json|biosamples\.json|srr_index\.json|bioprojects\.json)$")

SAMPLE_COLS = ["accession", "title", "collection_date", "country", "region", "city", "lat", "lon",
               "urban_status", "urban_context", "eligibility_reason", "isolation_source",
               "env_broad_scale", "env_local_scale", "env_medium", "host", "location_resolution"]
RUN_COLS = ["accession", "sample", "experiment", "assay_class", "library_strategy", "library_source",
            "library_selection", "library_layout", "platform", "instrument_model", "release_date",
            "spots", "bases", "avg_length", "size_mb", "scientific_name", "urban_status", "experiment_title"]
CUBE_COLS = ["project", "year", "country", "city", "assay", "tier", "context", "ai",
             "lat", "lon", "runs", "samples", "gbases"]


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
        key = json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
        i = self.index.get(key)
        if i is None:
            i = self.index[key] = len(self.values)
            self.values.append(value)
        return i


def _round(x: Any, nd: int) -> Any:
    return None if x is None else round(float(x), nd)


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

    ai: Dict[str, Dict[str, Any]] = {}
    for r in con.execute("SELECT * FROM ai_annotations"):
        ai[r["run_accession"]] = {k: r[k] for k in ("urban_origin", "metadata_sufficiency", "final_country",
                                                   "final_city", "final_assay_class") if r[k]}
        ai[r["run_accession"]]["reviewed"] = True

    projects = {r["accession"]: dict(r) for r in con.execute(
        "SELECT accession, title, description, organism, data_type, center_name, submission_date, last_update FROM projects")}

    runs = con.execute("""
        SELECT r.accession, r.project_accession AS project, r.sample_accession AS sample, r.experiment_accession AS experiment,
               r.release_date, r.release_year, r.spots, r.bases, r.avg_length, r.size_mb, r.scientific_name,
               r.center_name, r.urban_status, e.assay_class, e.library_strategy, e.library_source,
               e.library_selection, e.library_layout, e.platform, e.instrument_model, e.title AS experiment_title,
               s.title AS s_title, s.collection_date_raw, s.country, s.region, s.city, s.lat, s.lon,
               s.urban_status AS s_status, s.urban_context, s.eligibility_reason, s.isolation_source,
               s.env_broad_scale, s.env_local_scale, s.env_medium, s.host, s.location_resolution
        FROM runs r
        LEFT JOIN experiments e ON e.accession = r.experiment_accession
        LEFT JOIN samples s ON s.accession = r.sample_accession
        ORDER BY r.project_accession, r.sample_accession, r.accession
    """).fetchall()
    log(f"  {num(len(runs))} runs, {num(len(projects))} projects, {num(len(ai))} AI reviews")

    # --- cube + per-project shards ---------------------------------------
    d_project, d_country, d_city, d_assay, d_tier, d_ctx, d_ai = (_Dict() for _ in range(7))
    cube: Dict[tuple, List[Any]] = {}
    by_project: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    tally = {k: defaultdict(int) for k in ("years", "assays", "countries", "cities", "centers")}
    tier_runs, tier_samples = defaultdict(int), defaultdict(set)
    geo_resolved = downloadable = 0
    all_samples = set()

    for r in runs:
        a = ai.get(r["accession"], {})
        country = a.get("final_country") or r["country"] or ""
        city = a.get("final_city") or r["city"] or ""
        assay = a.get("final_assay_class") or r["assay_class"] or "Unknown"
        tier = r["urban_status"] or "unscreened"
        ctx = r["urban_context"] or ""
        year = r["release_year"]
        key = (d_project(r["project"] or ""), year, d_country(r["country"] or ""), d_city(r["city"] or ""),
               d_assay(r["assay_class"] or "Unknown"), d_tier(tier), d_ctx(ctx), d_ai(a) if a else -1,
               _round(r["lat"], 1), _round(r["lon"], 1))
        cell = cube.get(key)
        if cell is None:
            cell = cube[key] = [0, set(), 0]
        cell[0] += 1
        if r["sample"]:
            cell[1].add(r["sample"])
            all_samples.add(r["sample"])
        cell[2] += r["bases"] or 0
        by_project[r["project"] or ""].append(r)

        tally["years"][str(year) if year else "(unknown)"] += 1
        tally["assays"][assay] += 1
        tally["countries"][country or "(unknown)"] += 1
        tally["cities"][city or "(unknown)"] += 1
        tally["centers"][r["center_name"] or "(unknown)"] += 1
        tier_runs[tier] += 1
        if r["sample"]:
            tier_samples[tier].add(r["sample"])
        geo_resolved += bool(country and city)
        downloadable += 1  # every run in the catalog has RunInfo and an SRA accession

    rows = []
    for k, (n, samples, bases) in cube.items():
        rows.append([k[0], k[1], k[2], k[3], k[4], k[5], k[6], None if k[7] < 0 else k[7],
                     k[8], k[9], n, len(samples), round(bases / 1e9, 3)])
    rows.sort(key=lambda x: (x[0], -(x[1] or 0), x[2], x[3], x[4], x[5], x[6], x[7] if x[7] is not None else -1,
                             x[8] if x[8] is not None else 999, x[9] if x[9] is not None else 999))
    cube_doc = {
        "v": SITE_SCHEMA_VERSION, "cols": CUBE_COLS,
        "dims": {"project": d_project.values, "country": d_country.values, "city": d_city.values,
                 "assay": d_assay.values, "tier": d_tier.values, "context": d_ctx.values, "ai": d_ai.values},
        "rows": rows,
    }

    # --- projects index + shards --------------------------------------------
    project_rows = []
    written = unchanged = 0
    keep = set()
    for acc in sorted(by_project):
        rs = by_project[acc]
        p = projects.get(acc) or {}
        samples: Dict[str, int] = {}
        sample_rows, run_rows = [], []
        for r in rs:
            sid = r["sample"] or ""
            if sid not in samples:
                samples[sid] = len(sample_rows)
                sample_rows.append([sid, r["s_title"], r["collection_date_raw"], r["country"], r["region"], r["city"],
                                    r["lat"], r["lon"], r["s_status"], r["urban_context"], r["eligibility_reason"],
                                    r["isolation_source"], r["env_broad_scale"], r["env_local_scale"],
                                    r["env_medium"], r["host"], r["location_resolution"]])
            run_rows.append([r["accession"], samples[sid], r["experiment"], r["assay_class"], r["library_strategy"],
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
            "sample_cols": SAMPLE_COLS, "samples": sample_rows,
            "run_cols": RUN_COLS, "runs": run_rows,
            "ai": {r["accession"]: ai[r["accession"]] for r in rs if r["accession"] in ai},
        }
        name = shard_name(acc)
        keep.add(name)
        if _write_if_changed(os.path.join(shard_dir, name), _dump(shard)):
            written += 1
        else:
            unchanged += 1
        project_rows.append([acc, p.get("title") or "", center, p.get("data_type") or "",
                             p.get("submission_date") or "", len(rs), len(samples),
                             min(years) if years else None, max(years) if years else None])

    removed = 0
    for name in os.listdir(shard_dir):
        if name.endswith(".json") and name not in keep:
            os.remove(os.path.join(shard_dir, name))
            removed += 1

    def top(d):
        return [{"name": k, "count": v} for k, v in sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))]

    largest = sorted(project_rows, key=lambda r: -r[5])[:25]
    summary = {
        "generated_utc": utc_now(),
        "totalRuns": len(runs),
        "totalProjects": len(by_project),
        "totalBioSamples": len(all_samples),
        "totalCountries": len([k for k in tally["countries"] if k != "(unknown)"]),
        "totalCities": len([k for k in tally["cities"] if k != "(unknown)"]),
        "geoResolvedRuns": geo_resolved,
        "downloadableRuns": downloadable,
        "years": top(tally["years"]), "assays": top(tally["assays"]), "countries": top(tally["countries"]),
        "cities": top(tally["cities"]), "centers": top(tally["centers"]),
        "eligibility": [{"name": t, "runs": tier_runs[t], "biosamples": len(tier_samples[t])}
                        for t in ("confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental",
                                  "unscreened") if tier_runs.get(t)],
        "largestProject": None,
        "topProjects": [{"accession": r[0], "title": r[1], "run_count": r[5], "biosample_count": r[6],
                         "years": [y for y in (r[7], r[8]) if y]} for r in largest],
    }
    if largest:
        summary["largestProject"] = summary["topProjects"][0]

    files = {
        "summary.json": _dump(summary),
        "cube.json": _dump(cube_doc),
        "projects.json": _dump({"v": SITE_SCHEMA_VERSION,
                                "cols": ["accession", "title", "center_name", "data_type", "submission_date",
                                         "runs", "samples", "first_year", "last_year"],
                                "rows": project_rows}),
    }
    # Pages that only read generated_utc and the part count keep working.
    files["srr_records_manifest.json"] = _dump({"generated_utc": summary["generated_utc"], "total_records": len(runs),
                                                "parts": [], "format": "compact-v2", "see": "manifest.json"})
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
        "counts": {"runs": len(runs), "projects": len(by_project), "biosamples": len(all_samples),
                   "cube_rows": len(rows)},
        "files": {n: {"bytes": b} for n, b in sizes.items()},
        "project_shards": {"dir": "p/", "count": len(keep), "bytes": shard_bytes},
    }
    _write_if_changed(os.path.join(out_dir, "manifest.json"), json.dumps(manifest, indent=2).encode())
    con.close()

    log(f"  cube.json: {num(len(rows))} rows ({sizes['cube.json'] / 1e6:.1f} MB) · projects.json "
        f"{sizes['projects.json'] / 1e6:.1f} MB · summary.json {sizes['summary.json'] / 1e6:.2f} MB")
    log(f"  p/: {num(len(keep))} project files ({shard_bytes / 1e6:.0f} MB): {num(written)} written, "
        f"{num(unchanged)} unchanged, {num(removed)} removed")
    if legacy_removed:
        log(f"  removed old run-record files: {', '.join(sorted(legacy_removed))}")
    tiers = "  ".join(f"{t['name']} {t['runs']:,}" for t in summary["eligibility"])
    log(f"{paint('Website data ready', 'good')} in {fmt_duration(time.time() - t0)}  ({tiers})")
    return manifest
