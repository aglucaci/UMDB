from __future__ import annotations
import os, re, glob
from typing import Any, Dict, Iterator, List

from .config import DATA_DIR, DB_DIR, DOCS_LATEST_SRR, MAX_OUTPUT_BYTES
from .utils import (
    utc_now, read_json, write_json, iter_jsonl_glob,
    write_json_array_chunked, file_size
)

def _find_year_catalog_prefixes() -> List[str]:
    prefixes = set()
    for p in glob.glob(os.path.join(DATA_DIR, "srr_catalog_*.jsonl")):
        prefixes.add(p)
    for p in glob.glob(os.path.join(DATA_DIR, "srr_catalog_*_part[0-9][0-9][0-9].jsonl")):
        base = re.sub(r"_part[0-9]{3}\.jsonl$", ".jsonl", p)
        prefixes.add(base)

    def year_key(path: str) -> int:
        m = re.search(r"srr_catalog_(\d{4})", os.path.basename(path))
        return int(m.group(1)) if m else 0

    return sorted(prefixes, key=year_key)

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
        "largestProject": largest_project,
        "topProjects": project_rows[:25],
    }

def rebuild_srr_exports_chunked():
    prefixes = _find_year_catalog_prefixes()
    from .config import BIOPROJECT_CACHE, BIOSAMPLE_CACHE, AI_CURATION_CACHE
    ai_cache = read_json(AI_CURATION_CACHE, {})

    def all_records() -> Iterator[Dict[str, Any]]:
        for base in prefixes:
            for rec in iter_jsonl_glob(base):
                srr = (rec.get("srr") or rec.get("runinfo_row", {}).get("Run") or "").strip()
                if srr and isinstance(ai_cache.get(srr), dict):
                    rec = dict(rec)
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
    manifest["years"] = sorted(years)

    write_json(os.path.join(DB_DIR, "srr_records_manifest.json"), manifest)
    write_json(os.path.join(DB_DIR, "srr_index.json"), {
        "generated": utc_now(),
        "total_srr_records": manifest["total_records"],
        "parts": [os.path.basename(x["path"]) for x in manifest["parts"]],
        "years": manifest["years"],
    })

    bp_cache = read_json(BIOPROJECT_CACHE, {})
    if bp_cache:
        write_json(os.path.join(DB_DIR, "bioprojects.json"), bp_cache)
    bs_cache = read_json(BIOSAMPLE_CACHE, {})
    if bs_cache:
        write_json(os.path.join(DB_DIR, "biosamples.json"), bs_cache)
    if ai_cache:
        write_json(os.path.join(DB_DIR, "ai_curation.json"), ai_cache)

    def summary_records() -> Iterator[Dict[str, Any]]:
        for base in prefixes:
            for rec in iter_jsonl_glob(base):
                srr = (rec.get("srr") or rec.get("runinfo_row", {}).get("Run") or "").strip()
                if srr and isinstance(ai_cache.get(srr), dict):
                    rec = dict(rec)
                    rec["ai_curation"] = ai_cache[srr]
                yield rec

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
