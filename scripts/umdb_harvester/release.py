"""Versioned UMDB data releases from the SQLite database.

Writes, into dist/releases/<version>/:

  projects.parquet, samples.parquet, experiments.parquet, runs.parquet,
  project_samples.parquet, ai_annotations.parquet   normalized tables
  umdb_samples.parquet   one row per BioSample with its accepted context
  umdb_runs.parquet      one row per run joined to sample and experiment
  umdb_samples.tsv.gz, umdb_runs.tsv.gz              the same, for any tool
  schema.json            columns and types of every file
  manifest.json          version, build metadata, row counts, sizes, checksums
  checksums.sha256       `shasum -a 256 -c checksums.sha256` verifies the files

Parquet needs pyarrow (pip install -r requirements-release.txt). Everything
else in UMDB stays standard-library only. Releases are not committed to git:
attach the folder to a GitHub Release or deposit it in Zenodo for a DOI.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import sqlite3
import time
from typing import Any, Dict, List

from .database import DB_PATH, SCHEMA_VERSION
from .utils import log, paint, num, fmt_duration

RELEASE_ROOT = os.path.join("dist", "releases")

# Large verbatim columns stay in SQLite and the git JSONL; releases carry the
# curated fields plus the submitted BioSample attributes.
DROP_COLUMNS = {"projects": {"raw_json"}, "runs": {"runinfo_json"}}
DATE_COLUMNS = {"collection_date"}
TABLES = ["projects", "samples", "experiments", "runs", "project_samples", "ai_annotations",
          "countries", "admin1", "places", "sample_locations"]
VIEWS = ["umdb_samples", "umdb_runs"]
TSV_VIEWS = ["umdb_samples", "umdb_runs"]


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


def write_release(version: str = "", db_path: str = DB_PATH, out_root: str = RELEASE_ROOT) -> str:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Parquet releases need pyarrow: pip install -r requirements-release.txt") from exc
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} not found; run `umdb db build` first")

    t0 = time.time()
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

    for name in TABLES + VIEWS:
        cols = [(c, t) for c, t in _column_types(con, name) if c not in DROP_COLUMNS.get(name, set())]
        names = [c for c, _ in cols]
        rows = con.execute(f"SELECT {', '.join(names)} FROM {name}").fetchall()
        arrays = []
        for i, (col, decl) in enumerate(cols):
            typ = _arrow_type(pa, col, decl)
            values = [r[i] for r in rows]
            if typ == pa.date32():
                values = [dt.date.fromisoformat(v) if v else None for v in values]
            arrays.append(pa.array(values, type=typ))
        table = pa.Table.from_arrays(arrays, names=names)
        path = os.path.join(out, f"{name}.parquet")
        pq.write_table(table, path, compression="zstd")
        schema[name] = {"rows": len(rows), "columns": [{"name": c, "type": str(_arrow_type(pa, c, t))} for c, t in cols]}
        files[f"{name}.parquet"] = {"rows": len(rows)}
        log(f"  {name}.parquet: {num(len(rows))} rows, {os.path.getsize(path) / 1e6:.1f} MB")

        if name in TSV_VIEWS:
            tsv = os.path.join(out, f"{name}.tsv.gz")
            with gzip.open(tsv, "wt", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh, delimiter="\t", lineterminator="\n")
                w.writerow(names)
                w.writerows([["" if v is None else v for v in r] for r in rows])
            files[f"{name}.tsv.gz"] = {"rows": len(rows)}
            log(f"  {name}.tsv.gz: {os.path.getsize(tsv) / 1e6:.1f} MB")
    con.close()

    with open(os.path.join(out, "schema.json"), "w", encoding="utf-8") as fh:
        json.dump({"schema_version": SCHEMA_VERSION, "tables": schema}, fh, indent=2)
    for fname in list(files) + ["schema.json"]:
        p = os.path.join(out, fname)
        files.setdefault(fname, {}).update({"bytes": os.path.getsize(p), "sha256": _sha256(p)})
    manifest = {
        "name": "UMDB: Urban Microbiome Database",
        "version": version,
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "schema_version": SCHEMA_VERSION,
        "database_built_utc": meta.get("built_utc"),
        "query_profiles_version": meta.get("query_profiles_version"),
        "eligibility_rules_version": meta.get("rules_version"),
        "license": "CC-BY-NC-SA-4.0",
        "counts": {k[len("count_"):]: int(v) for k, v in meta.items() if k.startswith("count_")},
        "files": files,
    }
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    with open(os.path.join(out, "checksums.sha256"), "w", encoding="utf-8") as fh:
        for fname in sorted(files) + ["manifest.json"]:
            fh.write(f"{_sha256(os.path.join(out, fname))}  {fname}\n")
    total = sum(os.path.getsize(os.path.join(out, f)) for f in os.listdir(out)) / 1e6
    log(f"{paint('Release ready', 'good')}: {out}/ ({total:.1f} MB) in {fmt_duration(time.time() - t0)}")
    return out
