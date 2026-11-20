from __future__ import annotations
import os, re, glob
from typing import Any, Dict, Iterator, List

from .config import DATA_DIR, LATEST_SRR, MAX_OUTPUT_BYTES
from .utils import utc_now, iter_jsonl_glob, file_size, slim_record

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

    Records are also slimmed (see utils.slim_record). One pass over the catalog writes the kept records to a temporary
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
            buffers.setdefault(base, []).append(slim_record(rec))
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


def _norm(value: Any) -> str:
    return str(value or "").strip()

def refresh_derived_annotations(rec: Dict[str, Any], bs_cache: Dict[str, Any],
                                bp_cache: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
    # Eligibility reads the full BioProject text (description), which slim run
    # records no longer carry: use the cached project record when available.
    bp_acc = _norm(row.get("BioProject") or (rec.get("bioproject") or {}).get("accession")).upper()
    full_bp = (bp_cache or {}).get(bp_acc) if bp_acc else None
    bp = full_bp if isinstance(full_bp, dict) and not full_bp.get("error") else (rec.get("bioproject") or {})
    if bp is full_bp:
        rec["bioproject"] = full_bp  # cached project is authoritative; slim_record trims it
    rec["eligibility"] = assess_eligibility(rec, bsd, bp)
    return slim_record(rec)


def write_latest_srr_safe(latest_items: List[Dict[str, Any]]):
    payload = {"generated_utc": utc_now(), "count": len(latest_items), "items": latest_items}

    tmp = LATEST_SRR + ".tmp"
    import json
    os.makedirs(os.path.dirname(LATEST_SRR) or ".", exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    sz = file_size(tmp)

    if sz <= MAX_OUTPUT_BYTES:
        os.replace(tmp, LATEST_SRR)
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
    os.replace(tmp, LATEST_SRR)
