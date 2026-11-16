from __future__ import annotations
import csv
from typing import Any, Dict, List, Set, Tuple

import time

from .utils import inc, utc_now, append_lines, append_jsonl_one, log, warn, paint, num, fmt_duration, slim_record
from .config import SEEN_SRA_UIDS, SEEN_SRR_RUNS, BIOPROJECT_RE
from .ncbi import efetch_runinfo_text, runinfo_url, parallel_map, RUNINFO_BATCH_SIZE
from .biosample import get_biosample_details, infer_geo, fetch_biosamples
from .assay import classify_assay
from .bioproject import get_bioproject_details, fetch_bioprojects

def _runinfo_csv(text: str) -> List[Dict[str, str]]:
    # A multi-ID response may repeat the header between IDs.
    return [r for r in csv.DictReader(text.splitlines()) if (r.get("Run") or "").strip() not in ("", "Run")]


def parse_runinfo_rows(uid: str, max_rows: int = 200000) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    text, _ = efetch_runinfo_text(uid)
    rows = _runinfo_csv(text)
    if max_rows and len(rows) > max_rows:
        rows = rows[:max_rows]
    cols = list(rows[0].keys()) if rows else []
    return rows, {"url": runinfo_url(uid), "columns": cols, "rows": len(rows)}


def fetch_runinfo_for_uids(
    uids: List[str], summaries: Dict[str, Dict[str, Any]], max_rows: int = 200000,
    batch_size: int = RUNINFO_BATCH_SIZE,
) -> Tuple[Dict[str, List[Dict[str, str]]], Dict[str, str]]:
    """RunInfo rows per experiment UID, 200 experiments per request, in parallel.

    Rows are matched to UIDs through the experiment accession from esummary.
    UIDs without one, and every UID of a batch that fails, are fetched one by
    one. Returns ({uid: rows}, {uid: error}) for UIDs that still failed.
    """
    by_uid: Dict[str, List[Dict[str, str]]] = {u: [] for u in uids}
    errors: Dict[str, str] = {}
    acc_to_uid = {}
    single: List[str] = []
    for u in uids:
        acc = ((summaries.get(u) or {}).get("experiment_accession") or "").strip()
        if acc:
            acc_to_uid[acc] = u
        else:
            single.append(u)
    batched = [u for u in uids if u not in set(single)]
    batches = [batched[i:i + batch_size] for i in range(0, len(batched), batch_size)]

    def fetch_batch(batch: List[str]):
        try:
            text, _ = efetch_runinfo_text(",".join(batch))
            return batch, _runinfo_csv(text), None
        except Exception as e:
            return batch, [], e

    for batch, rows, err in parallel_map(fetch_batch, batches):
        if err is not None:
            warn(f"RunInfo batch of {len(batch)} failed ({err}); fetching them one by one")
            single.extend(batch)
            continue
        for r in rows:
            u = acc_to_uid.get((r.get("Experiment") or "").strip())
            if u is not None and u in batch:
                by_uid[u].append(r)

    def fetch_one(u: str):
        try:
            return u, parse_runinfo_rows(u, max_rows=max_rows)[0], None
        except Exception as e:
            return u, [], e

    for u, rows, err in parallel_map(fetch_one, single):
        by_uid[u] = rows
        if err is not None:
            errors[u] = str(err)
    if max_rows:
        for u, rows in by_uid.items():
            if len(rows) > max_rows:
                by_uid[u] = rows[:max_rows]
    return by_uid, errors

def debug_paths(tag: str) -> Dict[str, str]:
    from .config import DEBUG_DIR
    return {
        "report": f"{DEBUG_DIR}/report_{tag}.json",
        "decision": f"{DEBUG_DIR}/decision_log_{tag}.ndjson",
        "initial": f"{DEBUG_DIR}/initial_{tag}.json",
    }

# Accessions already re-fetched in this process, so a refresh fetches each
# BioSample/BioProject once rather than once per run.
_REFRESHED_BIOSAMPLES: Set[str] = set()
_REFRESHED_BIOPROJECTS: Set[str] = set()


def _force_once(acc: str, refresh: bool, done: Set[str]) -> bool:
    if not refresh or acc in done:
        return False
    done.add(acc)
    return True


def build_srr_records_for_sra_uid(
    sra_uid: str,
    sra_summary: Dict[str, Any],
    biosample_cache: Dict[str, Any],
    fetch_biosample: bool,
    bp_cache: Dict[str, Any],
    bp_uid_cache: Dict[str, str],
    fetch_bioproject: bool,
    debug: bool,
    decision_log_path: str,
    runinfo_max_rows: int,
    refresh: bool = False,
    runinfo_rows: "List[Dict[str, str]] | None" = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Run records for one experiment. With runinfo_rows (prefetched in a
    batch) and the BioSample/BioProject caches already filled, this makes no
    network requests."""
    if runinfo_rows is None:
        rows, runinfo_dbg = parse_runinfo_rows(sra_uid, max_rows=runinfo_max_rows)
    else:
        rows = runinfo_rows
        runinfo_dbg = {"url": runinfo_url(sra_uid), "columns": list(rows[0].keys()) if rows else [], "rows": len(rows)}
    title = (sra_summary.get("title") or "").strip()
    out: List[Dict[str, Any]] = []
    for r in rows:
        srr = (r.get("Run") or "").strip()
        if not srr:
            continue

        biosample_acc = (r.get("BioSample") or "").strip()
        biosample_details = {}
        if fetch_biosample and biosample_acc:
            force = _force_once(biosample_acc, refresh, _REFRESHED_BIOSAMPLES)
            cached = biosample_acc in biosample_cache and not force
            biosample_details = get_biosample_details(biosample_acc, biosample_cache, force=force)
            bs_err = (biosample_details or {}).get("error")
            log(f"      BioSample {biosample_acc}: {'cached' if cached else 're-fetched' if force else 'fetched'}"
                + (f" {paint('ERROR ' + str(bs_err)[:80], 'err')}" if bs_err
                   else f" ({len((biosample_details or {}).get('attributes') or {})} attributes)"), level=2)

        geo = infer_geo(
            biosample_details,
            fallbacks=[
                title,
                r.get("SampleName", "") or "",
                r.get("Sample", "") or "",
                r.get("Study", "") or "",
                r.get("BioProject", "") or "",
            ],
        )

        assay = classify_assay(r, title, biosample_details)

        prj = (r.get("BioProject") or "").strip().upper()
        if not prj and (sra_summary.get("bioproject_guess") or ""):
            prj = (sra_summary.get("bioproject_guess") or "").strip().upper()

        bioproject_details = {}
        if fetch_bioproject and prj and BIOPROJECT_RE.match(prj):
            force = _force_once(prj, refresh, _REFRESHED_BIOPROJECTS)
            cached = prj in bp_cache and not force
            bioproject_details = get_bioproject_details(prj, bp_cache, bp_uid_cache, force=force)
            if not cached:
                log(f"      BioProject {prj}: {'re-fetched' if force else 'fetched'}: "
                    f"{(bioproject_details or {}).get('title', '')[:70]}", level=2)

        out.append(slim_record({
            "srr": srr,
            "sra_uid": sra_uid,
            "title": title,
            "runinfo_row": r,
            "geo": geo,
            "assay": assay,
            "bioproject": bioproject_details,
            "ncbi": {
                "sra_uid_url": f"https://www.ncbi.nlm.nih.gov/sra/?term={sra_uid}",
                "srr_url": f"https://www.ncbi.nlm.nih.gov/sra/?term={srr}",
                "bioproject_url": bioproject_details.get("ncbi", {}).get("bioproject_url", "") if isinstance(bioproject_details, dict) else "",
            },
            "debug": {
                "runinfo_url": runinfo_dbg.get("url", ""),
                "runinfo_columns": runinfo_dbg.get("columns", []),
                "biosample_url": (biosample_details or {}).get("efetch_url", ""),
            },
            "provenance": {"ingested_utc": utc_now(), "source": "ncbi_eutils"},
        }))

    if debug:
        append_jsonl_one(decision_log_path, {
            "uid": sra_uid,
            "decision": "flattened_to_srr",
            "runs_emitted": len(out),
            "runinfo_url": runinfo_dbg.get("url", ""),
            "runinfo_rows": runinfo_dbg.get("rows", 0),
        })

    return out, runinfo_dbg

def ingest_uids_to_srr(
    tag: str,
    uids: List[str],
    summaries: Dict[str, Dict[str, Any]],
    biosample_cache: Dict[str, Any],
    bp_cache: Dict[str, Any],
    bp_uid_cache: Dict[str, str],
    seen_sra: Set[str],
    seen_srr: Set[str],
    fetch_biosample: bool,
    fetch_bioproject: bool,
    debug: bool,
    runinfo_max_rows: int,
    refresh: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Flatten SRA experiments to run records.

    refresh=False skips experiments and runs already ingested. refresh=True
    re-ingests them and re-fetches their BioSample/BioProject; the new record
    is appended and supersedes the old one at export/compaction (newest
    provenance.ingested_utc wins).
    """
    from .utils import write_json
    counters: Dict[str, int] = {}
    paths = debug_paths(tag)
 
    inc(counters, "uids_input", len(uids))
    new_uids = list(uids) if refresh else [u for u in uids if u not in seen_sra]
    inc(counters, "uids_new", len(new_uids))
    skipped = len(uids) - len(new_uids)
    if new_uids:
        log(f"  {'Refreshing' if refresh else 'Ingesting'} {num(len(new_uids))} experiment(s)"
            + (f", skipping {num(skipped)} already in the catalog" if skipped else ""))
    elif uids:
        log(f"  All {num(len(uids))} experiment(s) already in the catalog; nothing to ingest "
            f"{paint('(use --refresh to re-fetch)', 'dim')}")

    if debug:
        write_json(paths["initial"], {
            "tag": tag,
            "uids_input_count": len(uids),
            "uids_new_count": len(new_uids),
            "uids_new_sample": new_uids[:50],
        })

    added_srr: List[Dict[str, Any]] = []
    sra_mark_seen: List[str] = []
    srr_mark_seen: List[str] = []

    # Network work happens in three batched stages; building the records
    # afterwards reads only the prefetched rows and the caches.
    runinfo_by_uid: Dict[str, List[Dict[str, str]]] = {}
    runinfo_errors: Dict[str, str] = {}
    if new_uids:
        t = time.time()
        runinfo_by_uid, runinfo_errors = fetch_runinfo_for_uids(new_uids, summaries, max_rows=runinfo_max_rows)
        n_rows = sum(len(v) for v in runinfo_by_uid.values())
        log(f"    RunInfo: {num(n_rows)} run(s) for {num(len(new_uids))} experiment(s) in {fmt_duration(time.time() - t)}"
            + (f", {paint(f'{len(runinfo_errors)} failed', 'err')}" if runinfo_errors else ""))
        all_rows = [r for rows in runinfo_by_uid.values() for r in rows]

        if fetch_biosample:
            t = time.time()
            accs = list(dict.fromkeys((r.get("BioSample") or "").strip() for r in all_rows))
            need = [a for a in accs if a and (_force_once(a, refresh, _REFRESHED_BIOSAMPLES) or a not in biosample_cache)]
            if need:
                fetch_biosamples(need, biosample_cache)
            log(f"    BioSamples: {num(len(need))} {'re-fetched' if refresh else 'fetched'}, "
                f"{num(len([a for a in accs if a]) - len(need))} from cache in {fmt_duration(time.time() - t)}")

        if fetch_bioproject:
            t = time.time()
            prjs = []
            for uid in new_uids:
                guess = ((summaries.get(uid) or {}).get("bioproject_guess") or "").strip().upper()
                for r in runinfo_by_uid.get(uid, []):
                    prjs.append((r.get("BioProject") or "").strip().upper() or guess)
            prjs = [p for p in dict.fromkeys(prjs) if p and BIOPROJECT_RE.match(p)]
            need = [p for p in prjs if _force_once(p, refresh, _REFRESHED_BIOPROJECTS) or p not in bp_cache]

            try:
                fetch_bioprojects(need, bp_cache, bp_uid_cache)
            except Exception as e:
                warn(f"BioProject fetch failed: {e}")
            if need or prjs:
                log(f"    BioProjects: {num(len(need))} {'re-fetched' if refresh else 'fetched'}, "
                    f"{num(len(prjs) - len(need))} from cache in {fmt_duration(time.time() - t)}")

    for i, uid in enumerate(new_uids, 1):
        ssum = summaries.get(uid, {"uid": uid, "title": "", "bioproject_guess": "", "items": {}})
        try:
            if uid in runinfo_errors:
                raise RuntimeError(f"RunInfo fetch failed: {runinfo_errors[uid]}")
            srr_rows, _ = build_srr_records_for_sra_uid(
                sra_uid=uid,
                sra_summary=ssum,
                biosample_cache=biosample_cache,
                fetch_biosample=fetch_biosample,
                bp_cache=bp_cache,
                bp_uid_cache=bp_uid_cache,
                fetch_bioproject=fetch_bioproject,
                debug=debug,
                decision_log_path=paths["decision"],
                runinfo_max_rows=runinfo_max_rows,
                refresh=False,  # BioSamples/BioProjects were already refreshed above
                runinfo_rows=runinfo_by_uid.get(uid, []),
            )

            emitted = 0
            for r in srr_rows:
                srr = (r.get("srr") or "").strip()
                if not srr:
                    continue
                if srr in seen_srr:
                    if not refresh:
                        inc(counters, "skip_seen_srr")
                        continue
                    inc(counters, "srr_refreshed")
                else:
                    seen_srr.add(srr)
                    srr_mark_seen.append(srr)
                added_srr.append(r)
                emitted += 1

            inc(counters, "srr_emitted", emitted)
            inc(counters, "sra_uids_processed_ok")
            log(f"    [{i}/{len(new_uids)}] SRA {uid}: {len(srr_rows)} run(s), {emitted} written"
                f"  {paint((ssum.get('title') or '')[:70], 'dim')}", level=2)

            if uid not in seen_sra:
                seen_sra.add(uid)
                sra_mark_seen.append(uid)

        except Exception as e:
            inc(counters, "sra_uid_errors")
            warn(f"SRA experiment {uid} failed: {e}")
            if debug:
                append_jsonl_one(paths["decision"], {"uid": uid, "decision": "error", "error": str(e)})


    if sra_mark_seen:
        append_lines(SEEN_SRA_UIDS, sra_mark_seen)
    if srr_mark_seen:
        append_lines(SEEN_SRR_RUNS, srr_mark_seen)

    report = {"tag": tag, "generated_utc": utc_now(), "counters": counters, "debug_files": paths if debug else {}}
    if debug:
        write_json(paths["report"], report)

    return added_srr, report
