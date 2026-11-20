"""Reproducible research cohorts and curated collections.

A cohort is a JSON document that says which BioSamples to take from one UMDB
release, using the same filters as the website Explorer (the `filters` object
mirrors docs/assets/db.js in the UMA repository), plus optional balancing:

    {
      "name": "Temperate wastewater",
      "filters": {"tier": ["confirmed_urban", "probable_urban"], "context": ["municipal_wastewater"],
                  "assay": ["WGS"], "cmin": 2020, "cmax": 2025, "country": "", "region": "",
                  "city": "", "loc": "place", "q": "", "project": "", "ymin": null, "ymax": null},
      "balance": {"max_per_cell": 50, "max_per_project": 300, "target": 0, "seed": 20261119}
    }

`export_cohort` writes a research package (README, cohort.json with the
release and database versions, metadata.tsv, runs.tsv that STRIGA accepts
as --cohort, accessions.txt, download_fastq.sh, an nf-core/taxprofiler
samplesheet.csv, cohort_summary.json, analysis.py, analysis.R, CITATION.bib).
The website Workspace writes the same package in the browser; both use the same
seeded shuffle (mulberry32), so a balanced cohort is identical either way.

COLLECTIONS are curated, citable cohorts with written inclusion criteria; the
release ships their statistics and run lists (collections/), which UMA shows.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sqlite3
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .database import DB_PATH, SCHEMA_VERSION
from .utils import log, num, paint, utc_now

URBAN = ["confirmed_urban", "probable_urban"]
ACCEPTED = ("verified", "probable")
DEFAULT_SEED = 20261119

COLLECTIONS: List[Dict[str, Any]] = [
    {
        "slug": "global-wastewater",
        "title": "Global Wastewater",
        "subtitle": "Municipal wastewater shotgun metagenomes",
        "filters": {"tier": URBAN, "context": ["municipal_wastewater"], "assay": ["WGS"], "loc": "country"},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "Primary urban context: municipal wastewater (influent, sewage, treatment-plant samples)",
            "At least one shotgun metagenome (WGS) run",
            "Country validated against ISO 3166 (verified or probable)",
        ],
        "use": "Resistome, pathogen and virome surveillance; cross-city comparisons of sewage communities.",
        "workflow": "STRIGA Track B (sylph or MetaPhlAn 4, KMA/ResFinder), or nf-core/taxprofiler with samplesheet.csv",
    },
    {
        "slug": "global-transit",
        "title": "Global Transit",
        "subtitle": "Public transportation surface and air microbiomes",
        "filters": {"tier": URBAN, "context": ["transit"]},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "Primary urban context: transit (subway, metro, rail, bus surfaces and air)",
            "Any sequencing assay",
        ],
        "use": "Transit microbiomes across cities (MetaSUB-like comparisons) with assay-aware analyses.",
        "workflow": "Shotgun runs: STRIGA Track B; amplicon runs: DADA2 or QIIME 2",
    },
    {
        "slug": "urban-air",
        "title": "Urban Air",
        "subtitle": "Air, aerosols and settled dust",
        "filters": {"tier": URBAN, "context": ["air_dust"]},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "Primary urban context: air and dust (aerosols, particulate matter, settled dust)",
            "Any sequencing assay",
        ],
        "use": "Airborne microbial exposure, seasonality and pollution associations.",
        "workflow": "Low-biomass samples: check negative controls in the source studies before pooling",
    },
    {
        "slug": "urban-greenspace",
        "title": "Urban Greenspace",
        "subtitle": "Urban soil, parks and vegetation",
        "filters": {"tier": URBAN, "context": ["soil_greenspace"]},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "Primary urban context: soil and greenspace (parks, lawns, street trees, urban soil)",
            "Any sequencing assay",
        ],
        "use": "Homogenization of urban soils; comparisons with non-urban soils from the same regions.",
        "workflow": "16S/ITS runs: DADA2; shotgun runs: STRIGA Track B",
    },
    {
        "slug": "urban-virome",
        "title": "Urban Virome",
        "subtitle": "Viral RNA and metatranscriptomics candidates",
        "filters": {"tier": URBAN, "assay": ["Viral RNA", "Metatranscriptomics"]},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "At least one Viral RNA or metatranscriptomic run",
            "Any urban context",
        ],
        "use": "RNA virus discovery and viral persistence; includes targeted SARS-CoV-2 runs, so check library strategy.",
        "workflow": "Assembly + geNomad (STRIGA --viral) or targeted variant pipelines",
    },
    {
        "slug": "urban-resistome",
        "title": "Urban Resistome",
        "subtitle": "Shotgun metagenomes from every urban habitat",
        "filters": {"tier": URBAN, "assay": ["WGS"], "loc": "place"},
        "inclusion": [
            "Eligibility tier confirmed or probable urban",
            "At least one shotgun metagenome (WGS) run",
            "Place validated against GeoNames (verified or probable)",
        ],
        "use": "Antimicrobial resistance across connected urban habitats (UMDB-AMR).",
        "workflow": "STRIGA Track B with --amr (KMA against ResFinder)",
    },
]


# ----------------------------------------------------------------- filters

def default_filters() -> Dict[str, Any]:
    return {"q": "", "country": "", "region": "", "city": "", "tier": list(URBAN), "context": [], "assay": [],
            "loc": "any", "cmin": None, "cmax": None, "ymin": None, "ymax": None, "project": ""}


def normalize_filters(f: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = default_filters()
    for k, v in (f or {}).items():
        if k in out:
            out[k] = v
    return out


def filters_sql(f: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """WHERE clause over `samples s LEFT JOIN sample_locations l` with the Explorer's semantics."""
    f = normalize_filters(f)
    where, args = [], []
    if f["tier"]:
        where.append(f"s.urban_status IN ({','.join('?' * len(f['tier']))})")
        args += list(f["tier"])
    if f["context"]:
        where.append(f"coalesce(s.urban_context,'') IN ({','.join('?' * len(f['context']))})")
        args += list(f["context"])
    if f["assay"]:
        where.append("EXISTS (SELECT 1 FROM runs r JOIN experiments e ON e.accession = r.experiment_accession "
                     f"WHERE r.sample_accession = s.accession AND coalesce(e.assay_class,'Unknown') IN "
                     f"({','.join('?' * len(f['assay']))}))")
        args += list(f["assay"])
    if f["country"]:
        where.append("l.country_iso2 = ? AND l.country_status IN ('verified','probable')")
        args.append(f["country"].upper())
    if f["region"]:
        where.append("l.admin1_code = ? AND l.country_status IN ('verified','probable')")
        args.append(f["region"])
    if f["city"]:
        where.append("l.place_id = ? AND l.status IN ('verified','probable')")
        args.append(int(f["city"]))
    if f["loc"] == "verified":
        where.append("l.status = 'verified'")
    elif f["loc"] == "place":
        where.append("l.status IN ('verified','probable')")
    elif f["loc"] == "country":
        where.append("l.country_status IN ('verified','probable')")
    if f["cmin"] is not None:
        where.append("s.collection_year >= ?"); args.append(int(f["cmin"]))
    if f["cmax"] is not None:
        where.append("s.collection_year <= ?"); args.append(int(f["cmax"]))
    if f["ymin"] is not None or f["ymax"] is not None:
        sub = "(SELECT min(release_year) FROM runs r WHERE r.sample_accession = s.accession)"
        if f["ymin"] is not None:
            where.append(f"{sub} >= ?"); args.append(int(f["ymin"]))
        if f["ymax"] is not None:
            where.append(f"{sub} <= ?"); args.append(int(f["ymax"]))
    if f["project"]:
        where.append("EXISTS (SELECT 1 FROM project_samples ps WHERE ps.sample_accession = s.accession "
                     "AND ps.project_accession = ?)")
        args.append(f["project"])
    words = [w for w in str(f["q"] or "").lower().split() if w]
    if words:
        conds = " AND ".join("lower(p.accession || ' ' || coalesce(p.title,'') || ' ' || coalesce(p.center_name,'')) "
                             "LIKE ?" for _ in words)
        where.append(f"EXISTS (SELECT 1 FROM project_samples ps JOIN projects p ON p.accession = ps.project_accession "
                     f"WHERE ps.sample_accession = s.accession AND {conds})")
        args += [f"%{w}%" for w in words]
    return (" AND ".join(where) or "1=1"), args


# ----------------------------------------------------------------- seeded balancing

def mulberry32(seed: int):
    """The same 32-bit PRNG as UMA's docs/assets/workspace.js, so both shuffle identically."""
    state = seed & 0xFFFFFFFF

    def rand() -> float:
        nonlocal state
        state = (state + 0x6D2B79F5) & 0xFFFFFFFF
        t = state
        t = ((t ^ (t >> 15)) * (t | 1)) & 0xFFFFFFFF
        t ^= (t + (((t ^ (t >> 7)) * (t | 61)) & 0xFFFFFFFF)) & 0xFFFFFFFF
        return ((t ^ (t >> 14)) & 0xFFFFFFFF) / 4294967296
    return rand


def balance(samples: List[Dict[str, Any]], max_per_cell: int = 0, max_per_project: int = 0, target: int = 0,
            seed: int = DEFAULT_SEED) -> List[Dict[str, Any]]:
    """Seeded shuffle, then keep samples while (place, habitat) and project caps allow, up to target."""
    if not (max_per_cell or max_per_project or target):
        return samples
    order = sorted(samples, key=lambda s: s["biosample"])
    rand = mulberry32(int(seed))
    for i in range(len(order) - 1, 0, -1):
        j = int(rand() * (i + 1))
        order[i], order[j] = order[j], order[i]
    cell, proj, out = Counter(), Counter(), []
    for s in order:
        key = (s.get("place_id") or f"country:{s.get('country_iso2') or '-'}", s.get("habitat") or "")
        if max_per_cell and cell[key] >= max_per_cell:
            continue
        if max_per_project and proj[s["bioproject"]] >= max_per_project:
            continue
        cell[key] += 1
        proj[s["bioproject"]] += 1
        out.append(s)
        if target and len(out) >= target:
            break
    return sorted(out, key=lambda s: s["biosample"])


# ----------------------------------------------------------------- selection

SAMPLE_SQL = """
SELECT s.accession AS biosample,
       (SELECT min(ps.project_accession) FROM project_samples ps WHERE ps.sample_accession = s.accession) AS bioproject,
       s.urban_status, coalesce(s.urban_context,'') AS habitat,
       CASE WHEN l.country_status IN ('verified','probable') THEN l.country_iso2 END AS country_iso2,
       CASE WHEN l.country_status IN ('verified','probable') THEN co.name END AS country,
       CASE WHEN l.country_status IN ('verified','probable') THEN a1.name END AS region,
       CASE WHEN l.status IN ('verified','probable') THEN l.place_id END AS place_id,
       CASE WHEN l.status IN ('verified','probable') THEN pl.name END AS city,
       s.lat, s.lon, coalesce(l.status,'unresolved') AS location_status,
       s.collection_date, s.collection_year, s.collection_date_raw
FROM samples s
LEFT JOIN sample_locations l ON l.sample_accession = s.accession
LEFT JOIN countries co ON co.iso2 = l.country_iso2
LEFT JOIN admin1 a1 ON a1.code = l.admin1_code
LEFT JOIN places pl ON pl.geonames_id = l.place_id
"""

RUN_SQL = """
SELECT r.accession AS run, r.sample_accession AS biosample, r.project_accession AS project,
       e.library_layout, r.bases, r.spots, e.instrument_model, coalesce(e.assay_class,'Unknown') AS assay_class,
       e.platform, e.library_strategy, r.release_date, r.download_url
FROM runs r LEFT JOIN experiments e ON e.accession = r.experiment_accession
WHERE r.sample_accession IN ({ph})
ORDER BY r.sample_accession, r.accession
"""


def _rows(con: sqlite3.Connection, sql: str, args: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    cur = con.execute(sql, list(args))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def select_samples(con: sqlite3.Connection, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    where, args = filters_sql(filters)
    return _rows(con, f"{SAMPLE_SQL} WHERE {where} ORDER BY s.accession", args)


def select_runs(con: sqlite3.Connection, samples: List[Dict[str, Any]], assays: Iterable[str] = ()) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    accs = [s["biosample"] for s in samples]
    for i in range(0, len(accs), 900):
        chunk = accs[i:i + 900]
        out += _rows(con, RUN_SQL.format(ph=",".join("?" * len(chunk))), chunk)
    assays = set(assays or [])
    return [r for r in out if not assays or r["assay_class"] in assays]


def evaluate(con: sqlite3.Connection, cohort: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Samples and runs of a cohort document, including set operations and balancing.

    A cohort is either {"filters": {...}} or {"op": "union|intersection|difference", "a": cohort, "b": cohort}.
    """
    def samples_of(c: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        if c.get("op"):
            a, b = samples_of(c["a"]), samples_of(c["b"])
            if c["op"] == "union":
                return {**a, **b}
            if c["op"] == "intersection":
                return {k: v for k, v in a.items() if k in b}
            if c["op"] == "difference":
                return {k: v for k, v in a.items() if k not in b}
            raise ValueError(f"unknown cohort operation {c['op']!r}")
        return {s["biosample"]: s for s in select_samples(con, c.get("filters") or {})}

    samples = sorted(samples_of(cohort).values(), key=lambda s: s["biosample"])
    bal = cohort.get("balance") or {}
    samples = balance(samples, int(bal.get("max_per_cell") or 0), int(bal.get("max_per_project") or 0),
                      int(bal.get("target") or 0), int(bal.get("seed") or DEFAULT_SEED))
    assays = _all_assays(cohort)
    runs = select_runs(con, samples, assays)
    by_sample = defaultdict(list)
    for r in runs:
        by_sample[r["biosample"]].append(r)
    for s in samples:
        rs = by_sample.get(s["biosample"], [])
        s["n_runs"] = len(rs)
        s["assays"] = ";".join(sorted({r["assay_class"] for r in rs}))
    samples = [s for s in samples if s["n_runs"]]
    return samples, runs


def _all_assays(c: Dict[str, Any]) -> List[str]:
    """Assays to keep runs for: those named by the filters (all if any branch names none)."""
    if c.get("op"):
        a, b = _all_assays(c["a"]), _all_assays(c["b"])
        return [] if not a or not b else sorted(set(a) | set(b))
    return list((c.get("filters") or {}).get("assay") or [])


# ----------------------------------------------------------------- package

META_COLS = ["biosample", "bioproject", "urban_status", "habitat", "country_iso2", "country", "region", "place_id",
             "city", "lat", "lon", "location_status", "collection_date", "collection_year", "collection_date_raw",
             "assays", "n_runs"]
RUN_COLS = ["run", "biosample", "project", "library_layout", "bases", "spots", "instrument_model", "assay_class",
            "platform", "library_strategy", "release_date"]


def _tsv(rows: List[Dict[str, Any]], cols: List[str]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator="\n")
    w.writerow(cols)
    for r in rows:
        w.writerow(["" if r.get(c) is None else r.get(c) for c in cols])
    return buf.getvalue()


def summary(samples: List[Dict[str, Any]], runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    def top(key, n=500):
        return dict(Counter(s.get(key) or "(none)" for s in samples).most_common(n))
    return {
        "biosamples": len(samples), "runs": len(runs),
        "bioprojects": len({s["bioproject"] for s in samples}),
        "countries": len({s["country_iso2"] for s in samples if s.get("country_iso2")}),
        "cities": len({s["place_id"] for s in samples if s.get("place_id")}),
        "gigabases": round(sum(r.get("bases") or 0 for r in runs) / 1e9, 3),
        "by_habitat": top("habitat"), "by_country": top("country"),
        "by_collection_year": dict(sorted(Counter(str(s.get("collection_year") or "(none)") for s in samples).items())),
        "by_assay": dict(Counter(r["assay_class"] for r in runs).most_common()),
        "by_layout": dict(Counter(r.get("library_layout") or "(none)" for r in runs).most_common()),
    }


def package_files(cohort: Dict[str, Any], samples: List[Dict[str, Any]], runs: List[Dict[str, Any]],
                  release: Dict[str, Any]) -> Dict[str, str]:
    """Text files of a research package (same names and content as the website Workspace export)."""
    name = cohort.get("name") or "UMDB cohort"
    summ = summary(samples, runs)
    manifest = {
        "umdb_cohort_version": "1.0",
        "name": name,
        "description": cohort.get("description", ""),
        "created_utc": utc_now(),
        "release": release,
        "definition": {k: cohort[k] for k in ("filters", "op", "a", "b", "balance") if k in cohort},
        "counts": {k: summ[k] for k in ("biosamples", "runs", "bioprojects", "countries", "cities", "gigabases")},
        "files": ["README.md", "cohort.json", "metadata.tsv", "runs.tsv", "accessions.txt", "download_fastq.sh",
                  "samplesheet.csv", "cohort_summary.json", "analysis.py", "analysis.R", "CITATION.bib"],
    }
    paired = {r["run"]: (r.get("library_layout") or "").upper() == "PAIRED" for r in runs}
    sheet = ["sample,run_accession,instrument_platform,fastq_1,fastq_2,fasta"]
    for r in runs:
        plat = (r.get("platform") or "ILLUMINA").upper()
        f1 = f"fastq/{r['run']}_1.fastq.gz" if paired[r["run"]] else f"fastq/{r['run']}.fastq.gz"
        f2 = f"fastq/{r['run']}_2.fastq.gz" if paired[r["run"]] else ""
        sheet.append(f"{r['biosample']},{r['run']},{plat},{f1},{f2},")
    acc = "\n".join(r["run"] for r in runs) + "\n"
    return {
        "README.md": readme(name, cohort, summ, release),
        "cohort.json": json.dumps(manifest, indent=2) + "\n",
        "metadata.tsv": _tsv(samples, META_COLS),
        "runs.tsv": _tsv(runs, RUN_COLS),
        "accessions.txt": acc,
        "download_fastq.sh": DOWNLOAD_SH,
        "samplesheet.csv": "\n".join(sheet) + "\n",
        "cohort_summary.json": json.dumps(summ, indent=2) + "\n",
        "analysis.py": ANALYSIS_PY,
        "analysis.R": ANALYSIS_R,
        "CITATION.bib": citation_bib(release),
    }


README_TEMPLATE = """# {NAME}

A research cohort from **UMDB (Urban Microbiome Database)**, https://aglucaci.github.io/UMA/

| | |
| --- | --- |
| BioSamples | {SAMPLES} |
| Sequencing runs | {RUNS} ({GBP} Gbp) |
| BioProjects | {PROJECTS} |
| Countries / cities | {COUNTRIES} / {CITIES} |
| Habitats | {HABITATS} |
| UMDB database built | {BUILT} |
| Eligibility rules | {RULES} |
| Balancing | {BALANCE} |

`cohort.json` records the exact definition (filters, set operations, balancing and seed) and the UMDB
release it was drawn from. Rebuilding it from the same release gives the same samples:

```bash
umdb cohort export cohort.json -o rebuilt/
```

## Files

| File | Contents |
| --- | --- |
| `metadata.tsv` | One row per BioSample: eligibility tier, habitat, validated country/region/city, coordinates, collection date, assays |
| `runs.tsv` | One row per sequencing run; also the `--cohort` input of the STRIGA Nextflow pipeline |
| `accessions.txt` | Run accessions, one per line (nf-core/fetchngs, SRA Toolkit, ENA) |
| `download_fastq.sh` | Downloads every run with SRA Toolkit (`prefetch` + `fasterq-dump`) into `fastq/` |
| `samplesheet.csv` | nf-core/taxprofiler samplesheet pointing at `fastq/` |
| `cohort_summary.json` | Counts by habitat, country, year, assay and layout |
| `analysis.py`, `analysis.R` | Starter analyses: load the metadata, summarise, plot |
| `CITATION.bib` | How to cite UMDB |

## Caveats

* Habitat, urban tier and location come from UMDB's rule-based screen and gazetteer validation, not from the
  submitters; read the evidence on each sample's UMDB page before relying on one sample.
* Coordinates are submitter-reported; a city is a validated GeoNames place, not a sampling point.
* Studies differ in protocol, depth and season: model BioProject as a random effect or block on it.
* Sequence data are governed by the INSDC policies of their source archives; UMDB's curation and annotations are
  CC BY-NC-SA 4.0.
"""


def readme(name: str, cohort: Dict[str, Any], summ: Dict[str, Any], release: Dict[str, Any]) -> str:
    bal = cohort.get("balance") or {}
    habitats = ", ".join(f"{(k or 'unassigned').replace('_', ' ')} {v:,}" for k, v in list(summ["by_habitat"].items())[:8])
    values = {"NAME": name, "SAMPLES": f"{summ['biosamples']:,}", "RUNS": f"{summ['runs']:,}",
              "GBP": f"{summ['gigabases']:,}", "PROJECTS": f"{summ['bioprojects']:,}", "COUNTRIES": summ["countries"],
              "CITIES": summ["cities"], "HABITATS": habitats, "BUILT": release.get("database_built_utc") or "?",
              "RULES": release.get("eligibility_rules_version") or "?", "BALANCE": json.dumps(bal) if bal else "none"}
    out = README_TEMPLATE
    for k, v in values.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def write_templates(path: str) -> None:
    """documentation/cohort-templates.json in a release: the package templates UMA's Workspace uses."""
    doc = {"readme": README_TEMPLATE, "download": DOWNLOAD_SH, "py": ANALYSIS_PY, "r": ANALYSIS_R}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _write_if_changed(path, (json.dumps(doc, indent=1) + "\n").encode("utf-8"))


DOWNLOAD_SH = """#!/usr/bin/env bash
# Download every run in accessions.txt with SRA Toolkit (https://github.com/ncbi/sra-tools).
# Usage: bash download_fastq.sh [threads]
set -euo pipefail
threads="${1:-4}"
mkdir -p fastq sra
while read -r run; do
  [ -z "$run" ] && continue
  if ls "fastq/${run}"*.fastq.gz >/dev/null 2>&1; then continue; fi
  prefetch --max-size u --output-directory sra "$run"
  fasterq-dump --split-3 --skip-technical --threads "$threads" --outdir fastq "sra/$run"
  gzip -f fastq/"$run"*.fastq
  rm -rf "sra/$run"
done < accessions.txt
"""

ANALYSIS_PY = '''"""Starter analysis of a UMDB cohort. Requires pandas and matplotlib."""
import json

import matplotlib.pyplot as plt
import pandas as pd

meta = pd.read_csv("metadata.tsv", sep="\\t", keep_default_na=False, na_values=[""])
runs = pd.read_csv("runs.tsv", sep="\\t")
cohort = json.load(open("cohort.json"))
print(cohort["name"], cohort["counts"])

print(meta.groupby("habitat").agg(samples=("biosample", "size"), studies=("bioproject", "nunique"),
                                  countries=("country_iso2", "nunique")))
print(runs.groupby("assay_class")["bases"].describe())

fig, ax = plt.subplots(1, 2, figsize=(10, 3.5))
meta["collection_year"].value_counts().sort_index().plot.bar(ax=ax[0], title="BioSamples by collection year")
meta["country"].value_counts().head(15).plot.barh(ax=ax[1], title="Top countries")
fig.tight_layout()
fig.savefig("cohort_overview.png", dpi=150)
'''

ANALYSIS_R = '''# Starter analysis of a UMDB cohort. Requires readr, dplyr and ggplot2.
library(readr); library(dplyr); library(ggplot2)

meta <- read_tsv("metadata.tsv", na = "", show_col_types = FALSE)
runs <- read_tsv("runs.tsv", show_col_types = FALSE)

meta %>% group_by(habitat) %>%
  summarise(samples = n(), studies = n_distinct(bioproject), countries = n_distinct(country_iso2))

ggplot(meta, aes(collection_year, fill = habitat)) + geom_bar() +
  labs(x = "Collection year", y = "BioSamples") + theme_minimal()
ggsave("cohort_overview.png", width = 7, height = 4, dpi = 150)
'''


def citation_bib(release: Dict[str, Any]) -> str:
    built = (release.get("database_built_utc") or "")[:10]
    return f"""@misc{{umdb,
  title  = {{UMDB: Urban Microbiome Database}},
  author = {{Lucaci, Alexander G.}},
  year   = {{{built[:4] or '2026'}}},
  url    = {{https://aglucaci.github.io/UMA/}},
  note   = {{Database built {built or 'unknown'}; eligibility rules {release.get('eligibility_rules_version', '?')}}}
}}
"""


def release_info(con: sqlite3.Connection) -> Dict[str, Any]:
    meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
    return {"database_schema_version": meta.get("schema_version", SCHEMA_VERSION),
            "database_built_utc": meta.get("built_utc"),
            "eligibility_rules_version": meta.get("rules_version"),
            "geo_rules_version": meta.get("geo_rules_version"),
            "query_profiles_version": meta.get("query_profiles_version")}


def export_cohort(cohort: Dict[str, Any], out_dir: str, db_path: str = DB_PATH) -> Dict[str, Any]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} not found; run `umdb db build` first")
    con = sqlite3.connect(db_path)
    samples, runs = evaluate(con, cohort)
    files = package_files(cohort, samples, runs, release_info(con))
    con.close()
    os.makedirs(out_dir, exist_ok=True)
    for name, text in files.items():
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fh:
            fh.write(text)
    os.chmod(os.path.join(out_dir, "download_fastq.sh"), 0o755)
    try:  # Parquet copy of the metadata when pyarrow is available
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.Table.from_pylist([{c: s.get(c) for c in META_COLS} for s in samples]),
                       os.path.join(out_dir, "metadata.parquet"), compression="zstd")
    except ImportError:
        pass
    counts = json.loads(files["cohort.json"])["counts"]
    log(f"{paint('Cohort written', 'good')}: {out_dir}/  ({num(counts['biosamples'])} BioSamples, "
        f"{num(counts['runs'])} runs, {num(counts['bioprojects'])} BioProjects)")
    return counts


def collection_cohort(slug: str) -> Dict[str, Any]:
    for c in COLLECTIONS:
        if c["slug"] == slug:
            return {"name": f"UMDB collection: {c['title']}", "description": c["subtitle"],
                    "collection": slug, "filters": normalize_filters(c["filters"])}
    raise KeyError(f"unknown collection {slug!r}; choose from {', '.join(c['slug'] for c in COLLECTIONS)}")


# ----------------------------------------------------------------- release

def build_collections(con: sqlite3.Connection, out_dir: str) -> Dict[str, Any]:
    """collections.json (definitions + statistics) and collections/<slug>.tsv.gz (runs) for a release."""
    cdir = os.path.join(out_dir, "collections")
    os.makedirs(cdir, exist_ok=True)
    docs = []
    for c in COLLECTIONS:
        cohort = collection_cohort(c["slug"])
        samples, runs = evaluate(con, cohort)
        meta_by = {s["biosample"]: s for s in samples}
        path = os.path.join(cdir, f"{c['slug']}.tsv.gz")
        cols = ["run", "biosample", "project", "assay_class", "library_layout", "platform", "bases",
                "country_iso2", "place_id", "city", "habitat", "collection_date"]
        buf = io.StringIO()
        w = csv.writer(buf, delimiter="\t", lineterminator="\n")
        w.writerow(cols)
        for r in runs:
            s = meta_by.get(r["biosample"], {})
            row = {**r, **{k: s.get(k) for k in ("country_iso2", "place_id", "city", "habitat", "collection_date")}}
            w.writerow(["" if row.get(k) is None else row.get(k) for k in cols])
        data = gzip.compress(buf.getvalue().encode("utf-8"), mtime=0)
        _write_if_changed(path, data)
        docs.append({**{k: c[k] for k in ("slug", "title", "subtitle", "inclusion", "use", "workflow")},
                     "filters": normalize_filters(c["filters"]), "summary": summary(samples, runs),
                     "runs_file": f"collections/{c['slug']}.tsv.gz", "runs_file_bytes": len(data)})
        log(f"  collection {c['slug']}: {num(len(samples))} BioSamples, {num(len(runs))} runs")
    doc = {"v": 1, "generated_utc": utc_now(), "release": release_info(con), "collections": docs}
    _write_if_changed(os.path.join(out_dir, "collections.json"),
                      json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return doc


def _write_if_changed(path: str, data: bytes) -> bool:
    try:
        with open(path, "rb") as fh:
            if fh.read() == data:
                return False
    except FileNotFoundError:
        pass
    with open(path + ".tmp", "wb") as fh:
        fh.write(data)
    os.replace(path + ".tmp", path)
    return True
