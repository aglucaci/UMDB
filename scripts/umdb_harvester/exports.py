from __future__ import annotations
import os, re, glob
from typing import Any, Dict, Iterator, List

from .config import DATA_DIR, DB_DIR, DOCS_LATEST_SRR, MAX_OUTPUT_BYTES
from .utils import (
    utc_now, read_json, write_json, iter_jsonl_glob,
    write_json_array_chunked, file_size
)

def _find_year_catalog_prefixes() -> List[str]:
    """Return one base path per catalog year (data/srr_catalog_<YEAR>.jsonl).

    iter_jsonl_glob(base) already reads the base file plus its _partNNN
    files, so part files must not be returned themselves. They used to be
    (the srr_catalog_*.jsonl glob matches them), which counted every
    part-file record twice.
    """
    prefixes = set()
    for p in glob.glob(os.path.join(DATA_DIR, "srr_catalog_*.jsonl")):
        prefixes.add(re.sub(r"_part[0-9]{3}\.jsonl$", ".jsonl", p))

    def year_key(path: str) -> int:
        m = re.search(r"srr_catalog_(\d{4})", os.path.basename(path))
        return int(m.group(1)) if m else 0

    return sorted(prefixes, key=year_key)

def _run_id(rec: Dict[str, Any]) -> str:
    return str(rec.get("srr") or (rec.get("runinfo_row") or {}).get("Run") or "").strip()


def _iter_catalog(prefixes: List[str]) -> Iterator[tuple]:
    """Yield (position, base_path, record) across all catalog files in a fixed order."""
    pos = 0
    for base in prefixes:
        for rec in iter_jsonl_glob(base):
            yield pos, base, rec
            pos += 1


def latest_record_positions(prefixes: List[str]) -> Dict[str, int]:
    """Map each run to the position of its newest record.

    A run can appear more than once after a --refresh harvest. The record
    with the latest provenance.ingested_utc wins; ties go to the record read
    last.
    """
    best: Dict[str, tuple] = {}
    for pos, _, rec in _iter_catalog(prefixes):
        srr = _run_id(rec)
        if not srr:
            continue
        key = (str((rec.get("provenance") or {}).get("ingested_utc") or ""), pos)
        if srr not in best or key >= best[srr]:
            best[srr] = key
    return {srr: key[1] for srr, key in best.items()}


def compact_catalog() -> Dict[str, int]:
    """Rewrite data/srr_catalog_*.jsonl keeping only the newest record per run.

    One pass over the catalog writes the kept records to a temporary
    directory (re-chunked under 50 MB per year); only after every year has
    been written are the old files replaced. Records without a run accession
    are kept. Superseded rows are removed for good; earlier versions remain
    in git history.
    """
    import shutil
    import tempfile
    from .utils import append_jsonl

    prefixes = _find_year_catalog_prefixes()
    winners = latest_record_positions(prefixes)
    keep_pos = set(winners.values())
    stats = {"records_before": 0, "records_after": 0, "superseded_removed": 0}
    tmp_dir = tempfile.mkdtemp(prefix="umdb_compact_", dir=DATA_DIR)
    buffers: Dict[str, List[Dict[str, Any]]] = {}

    def flush(base: str):
        if buffers.get(base):
            append_jsonl(os.path.join(tmp_dir, os.path.basename(base)), buffers[base], max_bytes=MAX_OUTPUT_BYTES)
            buffers[base] = []

    try:
        for pos, base, rec in _iter_catalog(prefixes):
            stats["records_before"] += 1
            if _run_id(rec) and pos not in keep_pos:
                stats["superseded_removed"] += 1
                continue
            buffers.setdefault(base, []).append(rec)
            stats["records_after"] += 1
            if len(buffers[base]) >= 2000:
                flush(base)
        for base in list(buffers):
            flush(base)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    for base in prefixes:
        root, ext = os.path.splitext(base)
        for old in ([base] if os.path.exists(base) else []) + glob.glob(f"{root}_part[0-9][0-9][0-9]{ext}"):
            os.remove(old)
    for name in sorted(os.listdir(tmp_dir)):
        shutil.move(os.path.join(tmp_dir, name), os.path.join(DATA_DIR, name))
    os.rmdir(tmp_dir)
    return stats


def _safe_int(value: Any):
    try:
        return int(str(value).strip())
    except Exception:
        return None

def _norm(value: Any) -> str:
    return str(value or "").strip()

def _year_from_record(rec: Dict[str, Any]):
    raw = ((rec.get("runinfo_row") or {}).get("ReleaseDate") or (rec.get("runinfo_row") or {}).get("LoadDate") or "")
    m = re.match(r"^(\d{4})-", str(raw))
    return _safe_int(m.group(1)) if m else None

def _get_run(rec: Dict[str, Any]) -> str:
    return _norm(rec.get("srr") or (rec.get("runinfo_row") or {}).get("Run"))

def _get_bioproject(rec: Dict[str, Any]) -> str:
    return _norm((rec.get("runinfo_row") or {}).get("BioProject") or (rec.get("bioproject") or {}).get("accession"))

def _get_biosample(rec: Dict[str, Any]) -> str:
    return _norm((rec.get("runinfo_row") or {}).get("BioSample") or (rec.get("geo") or {}).get("biosample_accession"))

def _get_ai(rec: Dict[str, Any]) -> Dict[str, Any]:
    ai = rec.get("ai_curation")
    return ai if isinstance(ai, dict) else {}

def _get_country(rec: Dict[str, Any]) -> str:
    ai = _get_ai(rec)
    return _norm(ai.get("final_country") or (rec.get("geo") or {}).get("country"))

def _get_city(rec: Dict[str, Any]) -> str:
    ai = _get_ai(rec)
    return _norm(ai.get("final_city") or (rec.get("geo") or {}).get("city"))

def _get_assay(rec: Dict[str, Any]) -> str:
    ai = _get_ai(rec)
    return _norm(ai.get("final_assay_class") or (rec.get("assay") or {}).get("assay_class") or (rec.get("runinfo_row") or {}).get("LibraryStrategy") or "Unknown")

def _get_center(rec: Dict[str, Any]) -> str:
    return _norm((rec.get("runinfo_row") or {}).get("CenterName") or (rec.get("bioproject") or {}).get("center_name"))

def _get_title(rec: Dict[str, Any]) -> str:
    return _norm((rec.get("bioproject") or {}).get("title") or rec.get("title"))

def _is_known_geo(value: Any) -> bool:
    s = _norm(value).lower()
    return bool(s) and s != "(unknown)"

def _tally_map(counts: Dict[str, int]) -> List[Dict[str, Any]]:
    return [
        {"name": name, "count": count}
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]

def build_summary(records_iter: Iterator[Dict[str, Any]], generated_utc: str = "") -> Dict[str, Any]:
    total_runs = 0
    biosamples = set()
    geo_resolved_runs = 0
    downloadable_runs = 0
    years: Dict[str, int] = {}
    assays: Dict[str, int] = {}
    countries: Dict[str, int] = {}
    cities: Dict[str, int] = {}
    centers: Dict[str, int] = {}
    eligibility_runs: Dict[str, int] = {}
    eligibility_samples: Dict[str, set] = {}
    projects: Dict[str, Dict[str, Any]] = {}

    for rec in records_iter:
        total_runs += 1
        biosample = _get_biosample(rec)
        if biosample:
          biosamples.add(biosample)

        country = _get_country(rec)
        city = _get_city(rec)
        assay = _get_assay(rec) or "Unknown"
        center = _get_center(rec)
        bp = _get_bioproject(rec) or "(unassigned)"
        title = _get_title(rec)
        run = _get_run(rec)
        year = _year_from_record(rec)

        if _is_known_geo(country) and _is_known_geo(city):
            geo_resolved_runs += 1
        if _norm((rec.get("runinfo_row") or {}).get("download_path")):
            downloadable_runs += 1

        tier = _norm((rec.get("eligibility") or {}).get("status")) or "unscreened"
        eligibility_runs[tier] = eligibility_runs.get(tier, 0) + 1
        if biosample:
            eligibility_samples.setdefault(tier, set()).add(biosample)

        if year is not None:
            years[str(year)] = years.get(str(year), 0) + 1
        assays[assay or "Unknown"] = assays.get(assay or "Unknown", 0) + 1
        countries[country or "(unknown)"] = countries.get(country or "(unknown)", 0) + 1
        cities[city or "(unknown)"] = cities.get(city or "(unknown)", 0) + 1
        centers[center or "(unknown)"] = centers.get(center or "(unknown)", 0) + 1

        row = projects.setdefault(bp, {
            "accession": bp,
            "title": title,
            "runs": set(),
            "biosamples": set(),
            "countries": set(),
            "cities": set(),
            "assays": set(),
            "centers": set(),
            "years": set(),
        })
        if run:
            row["runs"].add(run)
        if biosample:
            row["biosamples"].add(biosample)
        if country:
            row["countries"].add(country)
        if city:
            row["cities"].add(city)
        if assay:
            row["assays"].add(assay)
        if center:
            row["centers"].add(center)
        if year is not None:
            row["years"].add(year)
        if not row["title"] and title:
            row["title"] = title

    project_rows = [
        {
            "accession": row["accession"],
            "title": row["title"],
            "run_count": len(row["runs"]),
            "biosample_count": len(row["biosamples"]),
            "country_count": len([x for x in row["countries"] if _is_known_geo(x)]),
            "city_count": len([x for x in row["cities"] if _is_known_geo(x)]),
            "assay_count": len(row["assays"]),
            "center_count": len([x for x in row["centers"] if x and x != "(unknown)"]),
            "years": sorted(row["years"]),
        }
        for row in projects.values()
    ]
    project_rows.sort(key=lambda row: (-row["run_count"], row["accession"]))
    largest_project = project_rows[0] if project_rows else None

    return {
        "generated_utc": generated_utc or utc_now(),
        "totalRuns": total_runs,
        "totalProjects": len(project_rows),
        "totalBioSamples": len(biosamples),
        "totalCountries": len([x for x in countries if _is_known_geo(x)]),
        "totalCities": len([x for x in cities if _is_known_geo(x)]),
        "geoResolvedRuns": geo_resolved_runs,
        "downloadableRuns": downloadable_runs,
        "years": _tally_map(years),
        "assays": _tally_map(assays),
        "countries": _tally_map(countries),
        "cities": _tally_map(cities),
        "centers": _tally_map(centers),
        "eligibility": [
            {"name": tier, "runs": eligibility_runs.get(tier, 0), "biosamples": len(eligibility_samples.get(tier, set()))}
            for tier in ("confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental", "unscreened")
            if eligibility_runs.get(tier)
        ],
        "largestProject": largest_project,
        "topProjects": project_rows[:25],
    }

def refresh_derived_annotations(rec: Dict[str, Any], bs_cache: Dict[str, Any]) -> Dict[str, Any]:
    """Recompute rule-derived geo and assay fields with the current rules.

    Catalog JSONL files store whatever the classifiers produced at ingest time,
    so rule fixes would otherwise only apply to newly harvested runs. Inputs
    are the stored SRA run row and the cached BioSample record, so this is
    deterministic and needs no network access.
    """
    from .assay import classify_assay
    from .biosample import infer_geo
    from .eligibility import assess_eligibility

    rec = dict(rec)
    row = rec.get("runinfo_row") if isinstance(rec.get("runinfo_row"), dict) else {}
    bs_acc = _norm(row.get("BioSample") or (rec.get("geo") or {}).get("biosample_accession"))
    bsd = bs_cache.get(bs_acc) if bs_acc else None
    bsd = bsd if isinstance(bsd, dict) and not bsd.get("error") else {}
    title = rec.get("title") or ""

    rec["assay"] = classify_assay(row, title, bsd)
    if bsd:
        rec["geo"] = infer_geo(bsd, fallbacks=[
            title, row.get("SampleName", "") or "", row.get("Sample", "") or "",
            row.get("Study", "") or "", row.get("BioProject", "") or "",
        ])
    rec["eligibility"] = assess_eligibility(rec, bsd, rec.get("bioproject") or {})
    return rec


def rebuild_srr_exports_chunked():
    prefixes = _find_year_catalog_prefixes()
    from .config import BIOPROJECT_CACHE, BIOSAMPLE_CACHE, AI_CURATION_CACHE
    ai_cache = read_json(AI_CURATION_CACHE, {})
    bs_cache = read_json(BIOSAMPLE_CACHE, {})

    # After a --refresh harvest a run can have several records; publish only
    # the newest one, and never publish a run twice.
    winners = latest_record_positions(prefixes)

    def all_records() -> Iterator[Dict[str, Any]]:
        for pos, _, rec in _iter_catalog(prefixes):
            srr = _run_id(rec)
            if srr and winners.get(srr) != pos:
                continue
            rec = refresh_derived_annotations(rec, bs_cache)
            if srr and isinstance(ai_cache.get(srr), dict):
                rec["ai_curation"] = ai_cache[srr]
            yield rec

    manifest = write_json_array_chunked(
        out_prefix=os.path.join(DB_DIR, "srr_records"),
        records_iter=all_records(),
        max_bytes=MAX_OUTPUT_BYTES,
    )

    years = set()
    for p in prefixes:
        m = re.search(r"srr_catalog_(\d{4})", os.path.basename(p))
        if m:
            years.add(int(m.group(1)))
    # Years of the srr_catalog_<YYYY>.jsonl files (harvest year), NOT run
    # release years; see summary.json "years" for the release-date distribution.
    manifest["catalog_file_years"] = sorted(years)
    manifest.pop("years", None)

    write_json(os.path.join(DB_DIR, "srr_records_manifest.json"), manifest)
    write_json(os.path.join(DB_DIR, "srr_index.json"), {
        "generated": utc_now(),
        "total_srr_records": manifest["total_records"],
        "parts": [os.path.basename(x["path"]) for x in manifest["parts"]],
        "catalog_file_years": manifest["catalog_file_years"],
    })

    bp_cache = read_json(BIOPROJECT_CACHE, {})
    if bp_cache:
        write_json(os.path.join(DB_DIR, "bioprojects.json"), bp_cache)
    if bs_cache:
        write_json(os.path.join(DB_DIR, "biosamples.json"), bs_cache)
    if ai_cache:
        write_json(os.path.join(DB_DIR, "ai_curation.json"), ai_cache)

    summary_records = all_records

    write_json(
        os.path.join(DB_DIR, "summary.json"),
        build_summary(summary_records(), generated_utc=manifest.get("generated_utc", "")),
    )

def write_latest_srr_safe(latest_items: List[Dict[str, Any]]):
    payload = {"generated_utc": utc_now(), "count": len(latest_items), "items": latest_items}

    tmp = DOCS_LATEST_SRR + ".tmp"
    import json
    os.makedirs(os.path.dirname(DOCS_LATEST_SRR) or ".", exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    sz = file_size(tmp)

    if sz <= MAX_OUTPUT_BYTES:
        os.replace(tmp, DOCS_LATEST_SRR)
        return

    lo, hi = 0, len(latest_items)
    best = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        test = {"generated_utc": utc_now(), "count": len(latest_items), "items": latest_items[:mid]}
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(test, f, ensure_ascii=False, indent=2)
        if file_size(tmp) <= MAX_OUTPUT_BYTES:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    final = {"generated_utc": utc_now(), "count": len(latest_items), "items": latest_items[:best]}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DOCS_LATEST_SRR)
