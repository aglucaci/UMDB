#!/usr/bin/env python3
"""Freeze an analysis-ready snapshot of the UMDB SQLite database.

Writes, into --outdir:
  samples.tsv.gz   one row per BioSample: tier, context, validated location (with
                   GeoNames population), collection date, assays, platforms,
                   sequencing volume, submitting centre, data-quality flags
  runs.tsv.gz      one row per run: sample, project, assay, platform, layout, bases
  snapshot.json    database meta (schema, rules and gazetteer versions, build time),
                   SHA-256 of the database, row counts and flag counts

Everything downstream reads these files, never the database, so one pipeline run
analyses exactly one frozen catalog state. Standard library + pandas only.
"""
import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

SAMPLES_SQL = """
SELECT s.accession                     AS biosample,
       s.urban_status, s.urban_context, s.environmental_sample,
       s.collection_date, s.collection_year, s.collection_date_raw,
       s.lat, s.lon,
       s.env_broad_scale IS NOT NULL   AS has_env_broad_scale,
       s.env_local_scale IS NOT NULL   AS has_env_local_scale,
       s.env_medium IS NOT NULL        AS has_env_medium,
       s.isolation_source IS NOT NULL  AS has_isolation_source,
       l.country_iso2, l.country_status, co.continent, co.name AS country_name,
       l.admin1_code, l.place_id, pl.name AS place_name, pl.population AS place_population,
       pl.lat AS place_lat, pl.lon AS place_lon, l.status AS location_status,
       l.coord_decimals, ps.project, ps.n_projects
FROM samples s
LEFT JOIN (SELECT sample_accession, min(project_accession) AS project, count(*) AS n_projects
           FROM project_samples GROUP BY sample_accession) ps ON ps.sample_accession = s.accession
LEFT JOIN sample_locations l ON l.sample_accession = s.accession
LEFT JOIN countries co       ON co.iso2 = l.country_iso2
LEFT JOIN places pl          ON pl.geonames_id = l.place_id
"""

RUNS_SQL = """
SELECT r.accession AS run, r.sample_accession AS biosample, r.project_accession AS project,
       r.release_date, r.release_year, r.spots, r.bases, r.avg_length,
       r.center_name, r.urban_status AS run_urban_status,
       e.assay_class, e.library_strategy, e.library_source, e.library_layout,
       e.platform, e.instrument_model
FROM runs r LEFT JOIN experiments e ON e.accession = r.experiment_accession
"""


def sha256(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(chunk), b""):
            h.update(b)
    return h.hexdigest()


def mode_by(runs, col):
    """Most frequent non-null value of col per BioSample (ties: alphabetical), vectorised."""
    c = runs.dropna(subset=[col]).groupby(["biosample", col]).size().reset_index(name="n")
    c = c.sort_values(["biosample", "n", col], ascending=[True, False, True])
    return c.drop_duplicates("biosample").set_index("biosample")[col]


def set_join(runs, col):
    """Sorted ';'-joined distinct non-null values of col per BioSample."""
    d = runs[["biosample", col]].dropna().drop_duplicates().sort_values(["biosample", col])
    return d.groupby("biosample")[col].agg(";".join)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--outdir", default=".")
    ap.add_argument("--no-hash", action="store_true", help="skip hashing the database (large files)")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)

    con = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
    samples = pd.read_sql_query(SAMPLES_SQL, con)
    runs = pd.read_sql_query(RUNS_SQL, con)
    con.close()

    build_date = (meta.get("built_utc") or datetime.now(timezone.utc).isoformat())[:10]
    build_year = int(build_date[:4])

    # ---- per-sample run aggregates
    g = runs.groupby("biosample")
    agg = pd.DataFrame({
        "n_runs": g.size(),
        "total_bases": g["bases"].sum(min_count=1),
        "max_run_bases": g["bases"].max(),
        "assays": set_join(runs, "assay_class"),
        "platforms": set_join(runs, "platform"),
        "first_release_date": g["release_date"].min(),
        "first_release_year": g["release_year"].min(),
    })
    agg["primary_assay"] = mode_by(runs, "assay_class")
    agg["primary_platform"] = mode_by(runs, "platform")
    agg["primary_instrument"] = mode_by(runs, "instrument_model")
    agg["center"] = mode_by(runs, "center_name")
    samples = samples.merge(agg, left_on="biosample", right_index=True, how="left")

    # ---- data-quality flags (used to exclude samples from temporal or spatial tests)
    cy = pd.to_numeric(samples["collection_year"], errors="coerce")
    cd = samples["collection_date"].fillna("")
    rel = samples["first_release_date"].fillna("").str[:10]
    samples["flag_date_future"] = ((cd != "") & (cd > build_date)) | (cy > build_year)
    samples["flag_date_after_release"] = (cd != "") & (rel != "") & (cd > rel)
    samples["flag_date_implausible"] = cy < 1950
    samples["flag_coords_conflict"] = samples["location_status"].eq("conflicting")
    samples["has_place"] = samples["place_id"].notna()
    samples["has_coordinates"] = samples["lat"].notna() & samples["lon"].notna()
    samples["has_collection_date"] = cd != ""
    samples["has_collection_year"] = cy.notna()
    samples["date_ok"] = ~(samples["flag_date_future"] | samples["flag_date_after_release"]
                           | samples["flag_date_implausible"])
    for c in ("has_env_broad_scale", "has_env_local_scale", "has_env_medium", "has_isolation_source"):
        samples[c] = samples[c].astype(bool)

    samples.to_csv(os.path.join(a.outdir, "samples.tsv.gz"), sep="\t", index=False)
    runs.to_csv(os.path.join(a.outdir, "runs.tsv.gz"), sep="\t", index=False)

    flags = {c: int(samples[c].sum()) for c in samples.columns if c.startswith("flag_")}
    doc = {
        "database": os.path.basename(a.db),
        "database_sha256": None if a.no_hash else sha256(a.db),
        "build_date": build_date,
        "meta": meta,
        "rows": {"samples": int(len(samples)), "runs": int(len(runs))},
        "flags": flags,
        "snapshot_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(os.path.join(a.outdir, "snapshot.json"), "w") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    with open(os.path.join(a.outdir, "build_date.txt"), "w") as fh:
        fh.write(build_date + "\n")
    print(json.dumps(doc["rows"]), json.dumps(flags))


if __name__ == "__main__":
    main()
