"""UMDB SQLite database: normalized, indexed, full-text searchable.

`data/umdb.sqlite` is *derived*: it is rebuilt from the git-tracked JSONL
catalog (`data/srr_catalog_*.jsonl`, the append-only ingestion log) plus the
BioSample/BioProject caches, applying the current geography, assay and
eligibility rules. It is git-ignored; run `umdb db build` to (re)create it.

BioSample is the principal unit of biological observation. Runs and
experiments link to their sample and project; the sample carries location,
environment and the urban eligibility tier.

Schema overview (SCHEMA_VERSION):
  projects        one row per BioProject
  samples         one row per BioSample (location, environment, eligibility)
  experiments     one row per SRA experiment (library, assay class)
  runs            one row per sequencing run (sizes, dates, download URL)
  project_samples link table (a sample can appear in several projects)
  ai_annotations  AI-proposed annotations, kept separate from rule output
  search          FTS5 full-text index over project and sample text
  meta            build metadata (versions, counts, timestamps)
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional

from .config import DATA_DIR, BIOSAMPLE_CACHE, BIOPROJECT_CACHE, AI_CURATION_CACHE, QUERY_PROFILES_VERSION
from .utils import read_json, utc_now, log, paint, num, fmt_duration, TIER_STYLE

SCHEMA_VERSION = "1.0.0"
DB_PATH = os.path.join(DATA_DIR, "umdb.sqlite")

# Most to least urban; a sample takes the most urban tier among its runs
# (runs of one sample share the BioSample evidence; differences come from
# run-level labels such as the organism, so the strongest evidence wins).
TIER_ORDER = ["confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental"]

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE projects (
    accession        TEXT PRIMARY KEY,          -- PRJNA…
    uid              TEXT,
    title            TEXT,
    description      TEXT,
    organism         TEXT,
    data_type        TEXT,
    center_name      TEXT,
    submission_date  TEXT,
    last_update      TEXT,
    raw_json         TEXT                       -- full cached esummary record
);

CREATE TABLE samples (
    accession             TEXT PRIMARY KEY,     -- SAMN… / SAMEA… / SAMD…
    title                 TEXT,
    organism              TEXT,
    collection_date_raw   TEXT,                 -- verbatim
    collection_date       TEXT,                 -- ISO YYYY-MM-DD when fully specified
    collection_year       INTEGER,
    geo_loc_raw           TEXT,                 -- verbatim location string
    country               TEXT,
    region                TEXT,
    city                  TEXT,
    city_status           TEXT,                 -- parsed / ambiguous / region_only / placeholder / missing
    lat                   REAL,
    lon                   REAL,
    location_resolution   TEXT,                 -- coordinate / city / region / country / unknown
    location_validation   TEXT,                 -- plausible / ambiguous / unverified / missing
    urban_status          TEXT,                 -- eligibility tier (see TIER_ORDER)
    urban_context         TEXT,                 -- primary context category
    urban_contexts        TEXT,                 -- JSON list
    environmental_sample  INTEGER,              -- 0/1
    eligibility_reason    TEXT,
    eligibility_evidence  TEXT,                 -- JSON list of {source, field, value, signal, terms}
    rules_version         TEXT,
    isolation_source      TEXT,
    env_broad_scale       TEXT,
    env_local_scale       TEXT,
    env_medium            TEXT,
    host                  TEXT,
    attributes_json       TEXT                  -- every submitted attribute
);

CREATE TABLE experiments (
    accession          TEXT PRIMARY KEY,        -- SRX / ERX / DRX
    sra_uid            TEXT,
    title              TEXT,
    study_accession    TEXT,                    -- SRP / ERP / DRP
    project_accession  TEXT REFERENCES projects(accession),
    library_strategy   TEXT,
    library_source     TEXT,
    library_selection  TEXT,
    library_layout     TEXT,
    platform           TEXT,
    instrument_model   TEXT,
    assay_class        TEXT,
    assay_tags         TEXT,                    -- JSON list
    assay_confidence   TEXT
);

CREATE TABLE runs (
    accession             TEXT PRIMARY KEY,     -- SRR / ERR / DRR
    experiment_accession  TEXT REFERENCES experiments(accession),
    sample_accession      TEXT REFERENCES samples(accession),
    project_accession     TEXT REFERENCES projects(accession),
    release_date          TEXT,
    load_date             TEXT,
    release_year          INTEGER,
    spots                 INTEGER,
    bases                 INTEGER,
    avg_length            INTEGER,
    size_mb               REAL,
    scientific_name       TEXT,                 -- SRA organism label
    tax_id                TEXT,
    center_name           TEXT,                 -- submitter; NOT the collection site
    download_url          TEXT,
    urban_status          TEXT,                 -- run-level eligibility tier
    eligibility_reason    TEXT,
    ingested_utc          TEXT,
    runinfo_json          TEXT                  -- verbatim SRA RunInfo row
);

CREATE TABLE project_samples (
    project_accession  TEXT NOT NULL,
    sample_accession   TEXT NOT NULL,
    PRIMARY KEY (project_accession, sample_accession)
);

CREATE TABLE ai_annotations (
    run_accession        TEXT PRIMARY KEY REFERENCES runs(accession),
    model                TEXT,
    reviewed_utc         TEXT,
    status               TEXT,                  -- proposed (not yet accepted into rule output)
    urban_origin         TEXT,
    metadata_sufficiency TEXT,
    confidence           TEXT,
    environment_type     TEXT,
    final_country        TEXT,
    final_city           TEXT,
    final_assay_class    TEXT,
    reasoning_summary    TEXT,
    evidence             TEXT                   -- JSON list
);

CREATE VIRTUAL TABLE search USING fts5(
    accession UNINDEXED,
    kind UNINDEXED,                             -- project / sample
    title,
    body,
    tokenize = 'porter unicode61'
);
"""

INDEXES = """
CREATE INDEX idx_samples_urban       ON samples(urban_status);
CREATE INDEX idx_samples_context     ON samples(urban_context);
CREATE INDEX idx_samples_country     ON samples(country);
CREATE INDEX idx_samples_city        ON samples(city);
CREATE INDEX idx_samples_date        ON samples(collection_date);
CREATE INDEX idx_samples_year        ON samples(collection_year);
CREATE INDEX idx_samples_cohort      ON samples(urban_status, urban_context, country, collection_date);
CREATE INDEX idx_runs_experiment     ON runs(experiment_accession);
CREATE INDEX idx_runs_sample         ON runs(sample_accession);
CREATE INDEX idx_runs_project        ON runs(project_accession);
CREATE INDEX idx_runs_release_year   ON runs(release_year);
CREATE INDEX idx_experiments_project ON experiments(project_accession);
CREATE INDEX idx_experiments_assay   ON experiments(assay_class);
CREATE INDEX idx_project_samples_s   ON project_samples(sample_accession);
"""

# Denormalized views: one row per sample / per run with the context people filter on.
VIEWS = """
CREATE VIEW umdb_samples AS
SELECT s.accession AS biosample_accession,
       (SELECT group_concat(project_accession, ';') FROM project_samples ps WHERE ps.sample_accession = s.accession) AS bioproject_accessions,
       s.urban_status, s.urban_context, s.environmental_sample,
       s.country, s.region, s.city, s.city_status, s.lat, s.lon,
       s.location_resolution, s.location_validation,
       s.collection_date, s.collection_year, s.collection_date_raw,
       s.isolation_source, s.env_broad_scale, s.env_local_scale, s.env_medium,
       s.organism, s.title,
       (SELECT count(*) FROM runs r WHERE r.sample_accession = s.accession) AS run_count,
       (SELECT group_concat(DISTINCT e.assay_class) FROM runs r JOIN experiments e ON e.accession = r.experiment_accession
         WHERE r.sample_accession = s.accession) AS assay_classes,
       s.eligibility_reason, s.rules_version
FROM samples s;

CREATE VIEW umdb_runs AS
SELECT r.accession AS run_accession, r.experiment_accession, r.sample_accession AS biosample_accession,
       r.project_accession AS bioproject_accession, p.title AS bioproject_title,
       e.assay_class, e.library_strategy, e.library_source, e.library_selection, e.library_layout,
       e.platform, e.instrument_model,
       r.release_date, r.release_year, r.spots, r.bases, r.avg_length, r.size_mb,
       r.scientific_name, r.center_name, r.download_url,
       r.urban_status AS run_urban_status, s.urban_status AS sample_urban_status, s.urban_context,
       s.country, s.region, s.city, s.lat, s.lon, s.collection_date, s.collection_year
FROM runs r
LEFT JOIN experiments e ON e.accession = r.experiment_accession
LEFT JOIN samples s     ON s.accession = r.sample_accession
LEFT JOIN projects p    ON p.accession = r.project_accession;
"""

_DATE_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?")


def _none(v: Any) -> Any:
    """Empty strings and placeholders become NULL."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ("missing", "not collected", "not applicable", "na", "n/a", "none",
                                   "not provided", "unknown", "null", "restricted access"):
            return None
        return s
    return v


def _int(v: Any) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _float(v: Any) -> Optional[float]:
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def parse_collection_date(raw: Any) -> tuple:
    """Return (iso_date_or_None, year_or_None) from a BioSample collection_date."""
    raw = _none(raw)
    if not raw:
        return None, None
    m = _DATE_RE.match(str(raw).replace("/", "-"))
    if not m:
        return None, None
    year = int(m.group(1))
    if not 1900 <= year <= 2100:
        return None, None
    if m.group(2) and m.group(3):
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return iso, year
    return None, year


def _attr(attrs: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        if k in attrs and _none(attrs[k]):
            return _none(attrs[k])
    return None


def build_database(path: str = DB_PATH) -> Dict[str, int]:
    """(Re)build the SQLite database from the catalog and caches.

    Builds into a temporary file and swaps it in only on success, so a failed
    build never leaves a half-written database in place.
    """
    from .exports import _find_year_catalog_prefixes, latest_record_positions, _iter_catalog, refresh_derived_annotations

    t0 = time.time()
    prefixes = _find_year_catalog_prefixes()
    bs_cache = read_json(BIOSAMPLE_CACHE, {})
    bp_cache = read_json(BIOPROJECT_CACHE, {})
    ai_cache = read_json(AI_CURATION_CACHE, {})
    winners = latest_record_positions(prefixes)
    log(f"Building {path} from {num(len(winners))} runs, {num(len(bs_cache))} cached BioSamples, "
        f"{num(len(bp_cache))} cached BioProjects")

    tmp = path + ".building"
    for p in (tmp, tmp + "-journal"):
        if os.path.exists(p):
            os.remove(p)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(tmp)
    try:
        con.executescript("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
        con.executescript(SCHEMA)
        counts = _populate(con, prefixes, winners, bs_cache, bp_cache, ai_cache)
        log("Creating indexes, views and the full-text index")
        con.executescript(INDEXES)
        con.executescript(VIEWS)
        _populate_search(con)
        meta = {
            "schema_version": SCHEMA_VERSION,
            "built_utc": utc_now(),
            "query_profiles_version": QUERY_PROFILES_VERSION,
            "source": "data/srr_catalog_*.jsonl + data/cache/*.json",
            **{f"count_{k}": str(v) for k, v in counts.items()},
        }
        from .eligibility import RULES_VERSION
        meta["rules_version"] = RULES_VERSION
        con.executemany("INSERT INTO meta(key, value) VALUES (?, ?)", meta.items())
        con.commit()
        con.execute("ANALYZE")
        con.commit()
    except BaseException:
        con.close()
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    con.close()
    os.replace(tmp, path)
    size = os.path.getsize(path) / 1e6
    log(f"{paint('Database ready', 'good')}: {path} ({size:.1f} MB) in {fmt_duration(time.time() - t0)}")
    for k in ("projects", "samples", "experiments", "runs", "ai_annotations"):
        log(f"  {k:15} {num(counts.get(k, 0))}")
    return counts


def _populate(con: sqlite3.Connection, prefixes: List[str], winners: Dict[str, int],
              bs_cache: Dict[str, Any], bp_cache: Dict[str, Any], ai_cache: Dict[str, Any]) -> Dict[str, int]:
    from .exports import _iter_catalog, refresh_derived_annotations

    projects: Dict[str, Dict[str, Any]] = {}
    experiments: Dict[str, tuple] = {}
    samples: Dict[str, Dict[str, Any]] = {}
    project_samples = set()
    run_rows: List[tuple] = []
    ai_rows: List[tuple] = []
    seen = 0

    for pos, _, raw in _iter_catalog(prefixes):
        srr = str(raw.get("srr") or (raw.get("runinfo_row") or {}).get("Run") or "").strip()
        if not srr or winners.get(srr) != pos:
            continue
        rec = refresh_derived_annotations(raw, bs_cache, bp_cache)
        row = rec.get("runinfo_row") or {}
        elig = rec.get("eligibility") or {}
        geo = rec.get("geo") or {}
        assay = rec.get("assay") or {}
        prj = _none(row.get("BioProject")) or _none((rec.get("bioproject") or {}).get("accession"))
        bsa = _none(row.get("BioSample"))
        exp = _none(row.get("Experiment"))

        if prj and prj not in projects:
            full = bp_cache.get(prj) if isinstance(bp_cache.get(prj), dict) else (rec.get("bioproject") or {})
            projects[prj] = full or {}
        if prj and bsa:
            project_samples.add((prj, bsa))

        if exp and exp not in experiments:
            experiments[exp] = (
                exp, _none(rec.get("sra_uid")), _none(rec.get("title")), _none(row.get("SRAStudy")), prj,
                _none(row.get("LibraryStrategy")), _none(row.get("LibrarySource")), _none(row.get("LibrarySelection")),
                _none(row.get("LibraryLayout")), _none(row.get("Platform")), _none(row.get("Model")),
                _none(assay.get("assay_class")), json.dumps(assay.get("assay_tags") or []), _none(assay.get("confidence")),
            )

        status = elig.get("status")
        if bsa:
            cur = samples.get(bsa)
            if cur is None or (status in TIER_ORDER and TIER_ORDER.index(status) < TIER_ORDER.index(cur["status"])):
                samples[bsa] = {"status": status if status in TIER_ORDER else "uncertain", "elig": elig, "geo": geo}

        rel = _none(row.get("ReleaseDate"))
        run_rows.append((
            srr, exp, bsa, prj, rel, _none(row.get("LoadDate")), _int(str(rel)[:4]) if rel else None,
            _int(row.get("spots")), _int(row.get("bases")), _int(row.get("avgLength")), _float(row.get("size_MB")),
            _none(row.get("ScientificName")), _none(row.get("TaxID")), _none(row.get("CenterName")),
            _none(row.get("download_path")), status, (elig.get("reasons") or [None])[0],
            _none((rec.get("provenance") or {}).get("ingested_utc")), json.dumps(row, ensure_ascii=False),
        ))
        ai = ai_cache.get(srr)
        if isinstance(ai, dict) and ai.get("reviewed"):
            ai_rows.append((
                srr, ai.get("model"), ai.get("reviewed_utc"), "proposed", ai.get("urban_origin"),
                ai.get("metadata_sufficiency"), ai.get("confidence"), _none(ai.get("environment_type")),
                _none(ai.get("final_country")), _none(ai.get("final_city")), _none(ai.get("final_assay_class")),
                _none(ai.get("reasoning_summary")), json.dumps(ai.get("evidence") or []),
            ))
        seen += 1
        if seen % 20000 == 0:
            log(f"  processed {num(seen)} runs")

    con.executemany("INSERT INTO projects VALUES (?,?,?,?,?,?,?,?,?,?)", [
        (acc, _none(p.get("uid")), _none(p.get("title")), _none(p.get("description")), _none(p.get("organism")),
         _none(p.get("data_type")), _none(p.get("center_name")), _none(p.get("submission_date")),
         _none(p.get("last_update")), json.dumps(p, ensure_ascii=False))
        for acc, p in projects.items()
    ])

    sample_rows = []
    for acc, s in samples.items():
        bsd = bs_cache.get(acc) if isinstance(bs_cache.get(acc), dict) else {}
        attrs = bsd.get("attributes") or {}
        geo, elig = s["geo"], s["elig"]
        cd_raw = _attr(attrs, "collection_date", "collection date") or _none(geo.get("collection_date"))
        cd_iso, cd_year = parse_collection_date(cd_raw)
        ctx = elig.get("urban_context") or []
        sample_rows.append((
            acc, _none(bsd.get("title")), _none(bsd.get("organism")), cd_raw, cd_iso, cd_year,
            _none(geo.get("raw")), _none(geo.get("country")), _none(geo.get("region")), _none(geo.get("city")),
            _none(geo.get("city_status")), _float(geo.get("lat")), _float(geo.get("lon")),
            elig.get("location_resolution"), elig.get("location_validation"),
            s["status"], ctx[0] if ctx else None, json.dumps(ctx),
            1 if elig.get("environmental_sample") else 0, (elig.get("reasons") or [None])[0],
            json.dumps(elig.get("evidence") or [], ensure_ascii=False), elig.get("rules_version"),
            _attr(attrs, "isolation_source", "isolation source", "isolation-source"),
            _attr(attrs, "env_broad_scale", "env_biome"), _attr(attrs, "env_local_scale", "env_feature"),
            _attr(attrs, "env_medium", "env_material"), _attr(attrs, "host", "host scientific name"),
            json.dumps(attrs, ensure_ascii=False),
        ))
    con.executemany(f"INSERT INTO samples VALUES ({','.join('?' * 28)})", sample_rows)
    con.executemany(f"INSERT INTO experiments VALUES ({','.join('?' * 14)})", experiments.values())
    con.executemany(f"INSERT INTO runs VALUES ({','.join('?' * 19)})", run_rows)
    con.executemany("INSERT INTO project_samples VALUES (?, ?)", sorted(project_samples))
    con.executemany(f"INSERT INTO ai_annotations VALUES ({','.join('?' * 13)})", ai_rows)
    return {"projects": len(projects), "samples": len(sample_rows), "experiments": len(experiments),
            "runs": len(run_rows), "ai_annotations": len(ai_rows), "project_samples": len(project_samples)}


def _populate_search(con: sqlite3.Connection) -> None:
    con.execute("""INSERT INTO search(accession, kind, title, body)
                   SELECT accession, 'project', coalesce(title, ''), coalesce(description, '') FROM projects""")
    con.execute("""INSERT INTO search(accession, kind, title, body)
                   SELECT accession, 'sample', coalesce(title, ''),
                          trim(coalesce(isolation_source, '') || ' ' || coalesce(env_broad_scale, '') || ' ' ||
                               coalesce(env_local_scale, '') || ' ' || coalesce(env_medium, '') || ' ' ||
                               coalesce(geo_loc_raw, '') || ' ' || coalesce(organism, ''))
                   FROM samples""")


def db_info(path: str = DB_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} does not exist; run `umdb db build` first")
    con = sqlite3.connect(path)
    try:
        meta = dict(con.execute("SELECT key, value FROM meta"))
        tiers = con.execute("SELECT urban_status, count(*) FROM samples GROUP BY 1 ORDER BY count(*) DESC").fetchall()
    finally:
        con.close()
    return {"meta": meta, "sample_tiers": tiers, "size_mb": os.path.getsize(path) / 1e6}
