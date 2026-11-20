"""Versioned UMDB data releases: the catalog, plus STRIGA's analyses of it.

    umdb release --version 1.0.0 --analysis ../STRIGA/results

Writes dist/releases/<version>/ (release contract `umdb-release/1`, which UMA
reads to build the website) and the bundle dist/releases/umdb-<version>.tar.gz:

  manifest.json            contract, version, build metadata, licences, row counts, sizes, SHA-256 of
                           every file, and `analysis` (which STRIGA version analysed this catalog)
  schema.json              columns and types of every table
  checksums.sha256         `shasum -a 256 -c checksums.sha256` verifies the release
  .zenodo.json             deposit metadata for a versioned Zenodo record (DOI)
  umdb.sqlite.zst          the query database (.gz when zstd is unavailable)
  parquet/                 normalized tables: projects, samples, experiments, runs, project_samples,
                           ai_annotations, countries, admin1, places, sample_locations; the
                           convenience views umdb_samples and umdb_runs; and three curated layers:
                             studies           one row per BioProject with sample/run counts and years
                             locations         each sample's accepted country/region/place with names
                             curation_evidence each sample's tier, context, reason and evidence
                             environmental_metadata  MIxS/environment fields and submitted attributes
  tsv/                     umdb_samples.tsv.gz, umdb_runs.tsv.gz for any tool
  indexes/coverage.json    urban BioSamples, BioProjects and places per country, with population
  collections/             collections.json and <slug>.tsv.gz run lists (see cohorts.py)
  reference/geonames/      countryInfo.txt and cities500.zip used to validate locations (CC BY 4.0)
  documentation/           DATA_DICTIONARY.md, ARCHITECTURE.md, INCLUSION_CRITERIA.md, CURATION.md, LICENSE,
                           cohort-templates.json (research-package templates used by UMA's Workspace)
  analysis/                STRIGA results (striga_manifest.json, report.md/json, hypotheses_status.tsv,
                           hypotheses/<ID>/, cohort/, covariates/ with the outside sources, profiles/),
                           only when --analysis is given. The analysed database must be this one:
                           its SHA-256 in striga_manifest.json is checked.

Parquet needs pyarrow (pip install -r requirements-release.txt). Everything
else in UMDB stays standard-library only. Releases are not committed to git:
attach the folder to a GitHub Release, upload it to object storage, or deposit
it in Zenodo for a DOI.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tarfile
import time
from typing import Any, Dict, List

from .database import DB_PATH, SCHEMA_VERSION
from .utils import log, paint, num, fmt_duration, warn
from .config import DATA_DIR

GEONAMES_DIR = os.path.join(DATA_DIR, "reference", "geonames")

RELEASE_ROOT = os.path.join("dist", "releases")
CONTRACT = "umdb-release/1"
STRIGA_CONTRACTS = {"striga-results/1"}
# STRIGA output that is not copied into a release: the snapshot tables duplicate the catalog,
# and Nextflow's own run records are not results.
ANALYSIS_SKIP = {"snapshot/samples.tsv.gz", "snapshot/runs.tsv.gz"}
ANALYSIS_SKIP_DIRS = {"pipeline_info", "work", ".nextflow"}

# Large verbatim columns stay in SQLite and the git JSONL; releases carry the
# curated fields plus the submitted BioSample attributes.
DROP_COLUMNS = {"projects": {"raw_json"}, "runs": {"runinfo_json"}}
DATE_COLUMNS = {"collection_date"}
TABLES = ["projects", "samples", "experiments", "runs", "project_samples", "ai_annotations",
          "countries", "admin1", "places", "sample_locations"]
VIEWS = ["umdb_samples", "umdb_runs"]
TSV_VIEWS = ["umdb_samples", "umdb_runs"]
# Curated layers, defined as queries so the release does not depend on new tables.
DERIVED = {
    "studies": """
        SELECT p.accession AS bioproject, p.title, p.center_name, p.data_type, p.submission_date,
               count(DISTINCT r.sample_accession) AS biosamples, count(r.accession) AS runs,
               sum(r.bases) AS bases, min(r.release_year) AS first_release_year, max(r.release_year) AS last_release_year
        FROM projects p LEFT JOIN runs r ON r.project_accession = p.accession
        GROUP BY p.accession""",
    "locations": """
        SELECT l.sample_accession AS biosample, l.country_iso2, co.name AS country, l.country_status,
               l.admin1_code, a1.name AS region, l.place_id, pl.name AS place, pl.population AS place_population,
               pl.lat AS place_lat, pl.lon AS place_lon, l.status AS place_status, l.method, l.distance_km,
               l.coord_decimals, s.lat AS reported_lat, s.lon AS reported_lon, s.geo_loc_raw AS submitted_text,
               l.geo_rules_version
        FROM sample_locations l JOIN samples s ON s.accession = l.sample_accession
        LEFT JOIN countries co ON co.iso2 = l.country_iso2
        LEFT JOIN admin1 a1 ON a1.code = l.admin1_code
        LEFT JOIN places pl ON pl.geonames_id = l.place_id""",
    "curation_evidence": """
        SELECT accession AS biosample, urban_status, urban_context, urban_contexts, environmental_sample,
               eligibility_reason, eligibility_evidence, rules_version
        FROM samples""",
    "environmental_metadata": """
        SELECT accession AS biosample, isolation_source, env_broad_scale, env_local_scale, env_medium, host,
               collection_date_raw, collection_date, collection_year, attributes_json
        FROM samples""",
}
DOCS = ["DATA_DICTIONARY.md", "ARCHITECTURE.md", "INCLUSION_CRITERIA.md", "CURATION.md", "LICENSE"]
LICENSES = {
    "umdb_curation": "CC-BY-NC-SA-4.0: UMDB's annotations, eligibility tiers, validated locations, collections, "
                     "derived tables, code and documentation",
    "upstream_records": "Sequence data and submitter metadata remain governed by the INSDC policies of their source "
                        "archives (NCBI SRA/BioSample/BioProject, ENA, DDBJ); UMDB redistributes metadata only",
    "geonames": "Place names and identifiers from GeoNames, CC BY 4.0",
}


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _column_types(con: sqlite3.Connection, name: str) -> List[tuple]:
    """(column, declared type) for a table or view."""
    rows = con.execute(f"PRAGMA table_info({name})").fetchall()
    out = [(r[1], (r[2] or "").upper()) for r in rows]
    if any(not t for _, t in out):  # views: infer from the first non-null value
        sample = con.execute(f"SELECT * FROM {name} LIMIT 2000").fetchall()
        inferred = []
        for i, (col, t) in enumerate(out):
            if t:
                inferred.append((col, t)); continue
            kinds = {type(r[i]).__name__ for r in sample if r[i] is not None}
            inferred.append((col, "INTEGER" if kinds == {"int"} else "REAL" if kinds <= {"int", "float"} and kinds else "TEXT"))
        out = inferred
    return out


def _arrow_type(pa, col: str, decl: str):
    if col in DATE_COLUMNS:
        return pa.date32()
    if "INT" in decl:
        return pa.int64()
    if "REAL" in decl or "FLOA" in decl or "DOUB" in decl:
        return pa.float64()
    return pa.string()


def check_analysis(analysis_dir: str, db_path: str, allow_unverified: bool = False) -> Dict[str, Any]:
    """Read STRIGA's striga_manifest.json and make sure it analysed exactly this database."""
    mf = os.path.join(analysis_dir, "striga_manifest.json")
    if not os.path.exists(mf):
        raise FileNotFoundError(f"{mf} not found: --analysis must be a STRIGA results folder (STRIGA >= 0.2.0)")
    with open(mf, encoding="utf-8") as fh:
        m = json.load(fh)
    if m.get("contract") not in STRIGA_CONTRACTS:
        raise RuntimeError(f"{mf}: unsupported STRIGA results contract {m.get('contract')!r}")
    analysed = (m.get("input") or {}).get("database_sha256")
    if not analysed:
        if not allow_unverified:
            raise RuntimeError("the STRIGA run did not hash its input database (--hash_db false), so it cannot be "
                               "matched to this release; rerun STRIGA with hashing or pass --allow-unverified-analysis")
        warn("bundling STRIGA results whose input database could not be verified")
    elif analysed != _sha256(db_path):
        raise RuntimeError(f"STRIGA analysed a different database (SHA-256 {analysed[:12]}…) than {db_path} "
                           f"({_sha256(db_path)[:12]}…); rerun STRIGA on this catalog or release the database it used")
    return m


def _copy_analysis(analysis_dir: str, out: str, files: Dict[str, Dict[str, Any]]) -> None:
    for root, dirs, names in os.walk(analysis_dir):
        dirs[:] = [d for d in dirs if d not in ANALYSIS_SKIP_DIRS]
        for name in names:
            src = os.path.join(root, name)
            rel = os.path.relpath(src, analysis_dir).replace(os.sep, "/")
            if rel in ANALYSIS_SKIP or name.startswith("."):
                continue
            dst = os.path.join(out, "analysis", rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(src, dst)
            files[f"analysis/{rel}"] = {}


def write_release(version: str = "", db_path: str = DB_PATH, out_root: str = RELEASE_ROOT,
                  analysis_dir: str = "", allow_unverified_analysis: bool = False,
                  geonames_dir: str = GEONAMES_DIR, bundle: bool = True) -> str:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Parquet releases need pyarrow: pip install -r requirements-release.txt") from exc
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} not found; run `umdb db build` first")

    t0 = time.time()
    striga = check_analysis(analysis_dir, db_path, allow_unverified_analysis) if analysis_dir else None
    version = version or dt.date.today().strftime("%Y.%m.%d")
    out = os.path.join(out_root, version)
    if os.path.exists(out) and os.listdir(out):
        raise FileExistsError(f"{out} already exists; choose another --version or delete it")
    os.makedirs(out, exist_ok=True)
    con = sqlite3.connect(db_path)
    meta = dict(con.execute("SELECT key, value FROM meta"))
    schema: Dict[str, Any] = {}
    files: Dict[str, Dict[str, Any]] = {}
    log(f"Writing release {paint(version, 'num')} to {out}/ from {db_path}")

    os.makedirs(os.path.join(out, "parquet"), exist_ok=True)
    os.makedirs(os.path.join(out, "tsv"), exist_ok=True)

    def write_table(name: str, cols: List[tuple], rows: List[tuple]) -> None:
        names = [c for c, _ in cols]
        arrays = []
        for i, (col, decl) in enumerate(cols):
            typ = _arrow_type(pa, col, decl)
            values = [r[i] for r in rows]
            if typ == pa.date32():
                values = [_date(v) for v in values]
            arrays.append(pa.array(values, type=typ))
        rel = f"parquet/{name}.parquet"
        pq.write_table(pa.Table.from_arrays(arrays, names=names), os.path.join(out, rel), compression="zstd")
        schema[name] = {"rows": len(rows), "columns": [{"name": c, "type": str(_arrow_type(pa, c, t))} for c, t in cols]}
        files[rel] = {"rows": len(rows)}
        log(f"  {rel}: {num(len(rows))} rows, {os.path.getsize(os.path.join(out, rel)) / 1e6:.1f} MB")
        if name in TSV_VIEWS:
            trel = f"tsv/{name}.tsv.gz"
            with gzip.open(os.path.join(out, trel), "wt", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh, delimiter="\t", lineterminator="\n")
                w.writerow(names)
                w.writerows([["" if v is None else v for v in r] for r in rows])
            files[trel] = {"rows": len(rows)}

    for name in TABLES + VIEWS:
        cols = [(c, t) for c, t in _column_types(con, name) if c not in DROP_COLUMNS.get(name, set())]
        rows = con.execute(f"SELECT {', '.join(c for c, _ in cols)} FROM {name}").fetchall()
        write_table(name, cols, rows)
    for name, sql in DERIVED.items():
        cur = con.execute(sql)
        rows = cur.fetchall()
        cols = [(d[0], _infer(rows, i)) for i, d in enumerate(cur.description)]
        write_table(name, cols, rows)

    # ---- coverage index and collections (UMA builds its own website indexes from this release)
    from .cohorts import build_collections, write_templates
    os.makedirs(os.path.join(out, "indexes"), exist_ok=True)
    with open(os.path.join(out, "indexes", "coverage.json"), "w", encoding="utf-8") as fh:
        json.dump(_coverage(con, geonames_dir), fh, indent=1)
    files["indexes/coverage.json"] = {}
    coll = build_collections(con, os.path.join(out, "collections_tmp"))
    shutil.move(os.path.join(out, "collections_tmp", "collections"), os.path.join(out, "collections"))
    shutil.move(os.path.join(out, "collections_tmp", "collections.json"), os.path.join(out, "collections", "collections.json"))
    shutil.rmtree(os.path.join(out, "collections_tmp"))
    for c in coll["collections"]:
        files[f"collections/{c['slug']}.tsv.gz"] = {"rows": c["summary"]["runs"]}
    files["collections/collections.json"] = {}

    # ---- documentation and the database itself
    os.makedirs(os.path.join(out, "documentation"), exist_ok=True)
    for doc in DOCS:
        if os.path.exists(doc):
            shutil.copyfile(doc, os.path.join(out, "documentation", doc))
            files[f"documentation/{doc}"] = {}
    write_templates(os.path.join(out, "documentation", "cohort-templates.json"))
    files["documentation/cohort-templates.json"] = {}
    con.close()

    # ---- the gazetteer the locations were validated against (STRIGA and UMA read it from the release)
    for name in ("countryInfo.txt", "cities500.zip"):
        src = os.path.join(geonames_dir, name)
        if os.path.exists(src):
            os.makedirs(os.path.join(out, "reference", "geonames"), exist_ok=True)
            shutil.copyfile(src, os.path.join(out, "reference", "geonames", name))
            files[f"reference/geonames/{name}"] = {}
        else:
            warn(f"{src} not found; the release has no GeoNames {name} (run `umdb geo fetch`)")

    # ---- STRIGA's analyses of this catalog
    if striga:
        _copy_analysis(analysis_dir, out, files)
        log(f"  analysis/: STRIGA {striga['pipeline'].get('version')} results "
            f"({sum(1 for f in files if f.startswith('analysis/'))} files)")
    dbrel = _compress_db(db_path, out)
    files[dbrel] = {}

    with open(os.path.join(out, "schema.json"), "w", encoding="utf-8") as fh:
        json.dump({"schema_version": SCHEMA_VERSION, "tables": schema}, fh, indent=2)
    files["schema.json"] = {}
    for fname in files:
        p = os.path.join(out, fname)
        files[fname].update({"bytes": os.path.getsize(p), "sha256": _sha256(p)})
    counts = {k[len("count_"):]: int(v) for k, v in meta.items() if k.startswith("count_")}
    manifest = {
        "name": "UMDB: Urban Microbiome Database",
        "contract": CONTRACT,
        "version": version,
        "doi": None,  # filled in after the Zenodo deposit (see MAINTAINING.md)
        "url": f"https://github.com/aglucaci/UMDB/releases/tag/v{version}",
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "schema_version": SCHEMA_VERSION,
        "database_built_utc": meta.get("built_utc"),
        "query_profiles_version": meta.get("query_profiles_version"),
        "eligibility_rules_version": meta.get("rules_version"),
        "geo_rules_version": meta.get("geo_rules_version"),
        "license": "CC-BY-NC-SA-4.0",
        "licenses": LICENSES,
        "counts": counts,
        "collections": [c["slug"] for c in coll["collections"]],
        "analysis": None if not striga else {
            "pipeline": striga.get("pipeline"), "created_utc": striga.get("created_utc"),
            "database_verified": bool((striga.get("input") or {}).get("database_sha256")),
            "hypotheses": striga.get("hypotheses")},
        "files": files,
    }
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    with open(os.path.join(out, ".zenodo.json"), "w", encoding="utf-8") as fh:
        json.dump(_zenodo(version, meta, counts, striga), fh, indent=2)
    with open(os.path.join(out, "checksums.sha256"), "w", encoding="utf-8") as fh:
        for fname in sorted(files) + ["manifest.json", ".zenodo.json"]:
            fh.write(f"{_sha256(os.path.join(out, fname))}  {fname}\n")
    total = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(out) for f in fs) / 1e6
    if bundle:  # one asset for GitHub Releases / Zenodo; `uma fetch` unpacks it
        tar = os.path.join(out_root, f"umdb-{version}.tar.gz")
        with tarfile.open(tar, "w:gz") as tf:
            tf.add(out, arcname=f"umdb-{version}")
        log(f"  bundle {tar}: {os.path.getsize(tar) / 1e6:.1f} MB")
    log(f"{paint('Release ready', 'good')}: {out}/ ({total:.1f} MB) in {fmt_duration(time.time() - t0)}")
    return out


def _date(v: Any):
    try:
        return dt.date.fromisoformat(v) if v else None
    except ValueError:
        return None


def _infer(rows: List[tuple], i: int) -> str:
    kinds = {type(r[i]).__name__ for r in rows[:5000] if r[i] is not None}
    if kinds == {"int"}:
        return "INTEGER"
    if kinds and kinds <= {"int", "float"}:
        return "REAL"
    return "TEXT"


def _compress_db(db_path: str, out: str) -> str:
    """umdb.sqlite.zst (Python 3.14+ compression.zstd) or umdb.sqlite.gz."""
    try:
        from compression import zstd  # type: ignore[import-not-found]
        rel, opener = "umdb.sqlite.zst", lambda p: zstd.open(p, "wb", level=10)
    except ImportError:
        rel, opener = "umdb.sqlite.gz", lambda p: gzip.open(p, "wb", compresslevel=6)
    with open(db_path, "rb") as src, opener(os.path.join(out, rel)) as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    log(f"  {rel}: {os.path.getsize(os.path.join(out, rel)) / 1e6:.1f} MB")
    return rel


def _country_population(geonames_dir: str) -> Dict[str, int]:
    """Country populations from GeoNames countryInfo.txt."""
    path = os.path.join(geonames_dir, "countryInfo.txt")
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


def _coverage(con: sqlite3.Connection, geonames_dir: str = GEONAMES_DIR) -> Dict[str, Any]:
    """Urban BioSamples, BioProjects and places per validated country, with GeoNames population."""
    pop = _country_population(geonames_dir)
    rows = con.execute("""
        SELECT l.country_iso2, co.name, co.continent,
               count(DISTINCT s.accession) AS biosamples,
               count(DISTINCT CASE WHEN s.urban_status IN ('confirmed_urban','probable_urban')
                                   THEN s.accession END) AS urban_biosamples,
               count(DISTINCT ps.project_accession) AS bioprojects,
               count(DISTINCT CASE WHEN l.status IN ('verified','probable') THEN l.place_id END) AS places
        FROM sample_locations l JOIN samples s ON s.accession = l.sample_accession
        JOIN countries co ON co.iso2 = l.country_iso2
        LEFT JOIN project_samples ps ON ps.sample_accession = s.accession
        WHERE l.country_status IN ('verified','probable')
        GROUP BY l.country_iso2 ORDER BY biosamples DESC""").fetchall()
    out = []
    for iso, name, cont, n, u, bp, pl in rows:
        p = pop.get(iso)
        out.append({"iso2": iso, "name": name, "continent": cont, "biosamples": n, "urban_biosamples": u,
                    "bioprojects": bp, "places": pl, "population": p,
                    "urban_biosamples_per_million": round(u / p * 1e6, 3) if p else None})
    return {"unit": "BioSamples with a validated country", "countries": out}


def _zenodo(version: str, meta: Dict[str, str], counts: Dict[str, int], striga: Any = None) -> Dict[str, Any]:
    analysis = (f" Includes analyses by STRIGA {striga['pipeline'].get('version')} "
                f"(https://github.com/aglucaci/STRIGA) run on this exact catalog." if striga else "")
    return {
        "title": f"UMDB: Urban Microbiome Database, release {version}",
        "upload_type": "dataset",
        "version": version,
        "creators": [{"name": "Lucaci, Alexander G."}],
        "description": (f"Versioned release of UMDB, a rule-screened catalog of public urban microbiome sequencing "
                        f"metadata: {counts.get('runs', 0):,} runs, {counts.get('samples', 0):,} BioSamples and "
                        f"{counts.get('projects', 0):,} BioProjects. Database built {meta.get('built_utc')}, "
                        f"eligibility rules {meta.get('rules_version')}. Includes Parquet tables, the SQLite "
                        f"database, curated collections, the GeoNames reference and documentation.{analysis} Browse it at "
                        f"https://aglucaci.github.io/UMA/."),
        "license": "cc-by-nc-sa-4.0",
        "keywords": ["urban microbiome", "metagenomics", "wastewater", "built environment", "SRA", "metadata"],
        "related_identifiers": [
            {"identifier": "https://github.com/aglucaci/UMDB", "relation": "isSupplementTo", "scheme": "url"},
            {"identifier": "https://github.com/aglucaci/STRIGA", "relation": "isSupplementTo", "scheme": "url"},
            {"identifier": "https://aglucaci.github.io/UMA/", "relation": "isDocumentedBy", "scheme": "url"}],
    }
