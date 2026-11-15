from __future__ import annotations
import argparse, datetime as dt, json
from typing import Any, Dict, List

from .config import (
    DEFAULT_QUERY, QUERY_PROFILES, DEFAULT_QUERY_PROFILE_NAMES, QUERY_PROFILES_VERSION,
    BIOSAMPLE_CACHE, BIOPROJECT_CACHE, BIOPROJECT_UID_CACHE, AI_CURATION_CACHE, OPENAI_MODEL, OPENAI_API_KEY,
    DOCS_LATEST_DEBUG, DATA_DIR, DB_DIR
)
import sys
import time

from .utils import ensure_dirs, load_set, read_json, write_json, append_jsonl, iter_jsonl_glob
from .utils import log, header, warn, paint, num, fmt_duration, VERBOSITY
from . import ncbi as ncbi_mod
from .ncbi import esearch_recent, esearch_day, esearch_history, esummary_sra
from .ingest import ingest_uids_to_srr, debug_paths
from .exports import rebuild_srr_exports_chunked, write_latest_srr_safe
from .ai_curation import curate_records

def print_report_summary(report: Dict[str, Any]):
    tag = report.get("tag", "run")
    counters = report.get("counters", {}) if isinstance(report.get("counters", {}), dict) else {}
    ai = report.get("ai_curation", {}) if isinstance(report.get("ai_curation", {}), dict) else {}
    parts = [f"tag={tag}"]

    if "records_considered" in report:
        parts.append(f"records_considered={report.get('records_considered', 0)}")
    if counters:
        parts.extend([
            f"uids_input={counters.get('uids_input', 0)}",
            f"uids_new={counters.get('uids_new', 0)}",
            f"srr_emitted={counters.get('srr_emitted', 0)}",
        ])
    if ai:
        parts.extend([
            f"ai_reviewed={ai.get('reviewed', 0)}",
            f"ai_skipped_cached={ai.get('skipped_cached', 0)}",
            f"ai_errors={ai.get('errors', 0)}",
        ])

    # Machine-readable lines (parsed by scripts/deep_backfill.sh). Shown in
    # logs and pipes, and in a terminal only with -v.
    if not sys.stdout.isatty() or VERBOSITY >= 2:
        print("[SUMMARY] " + " ".join(parts), flush=True)
        print("[SUMMARY_JSON] " + json.dumps(report, ensure_ascii=False), flush=True)

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_refresh_arg(parser):
        parser.add_argument(
            "--refresh", action="store_true",
            help="re-ingest already-seen experiments/runs and re-fetch their BioSample/BioProject; "
                 "the new record supersedes the old one (run 'compact' to drop superseded rows)")

    def add_ai_args(parser):
        parser.add_argument("--ai-curate", action="store_true")
        parser.add_argument("--ai-model", default=OPENAI_MODEL)
        parser.add_argument("--ai-max-records", type=int, default=0)

    d = sub.add_parser("daily")
    d.add_argument("--days", type=int, default=1)  # kept for compatibility
    d.add_argument("--query", default=DEFAULT_QUERY)
    d.add_argument("--query-profile", action="append", choices=sorted(QUERY_PROFILES.keys()))
    d.add_argument("--max-per-day", type=int, default=500)
    d.add_argument("--debug", action="store_true")
    d.add_argument("--fetch-biosample", action="store_true")
    d.add_argument("--fetch-bioproject", action="store_true")
    d.add_argument("--runinfo-max-rows", type=int, default=200000)
    d.add_argument("--recent-days", type=int, default=7)
    add_refresh_arg(d)
    add_ai_args(d)

    b = sub.add_parser("backfill-year")
    b.add_argument("--year", type=int, required=True)
    b.add_argument("--query", default=DEFAULT_QUERY)
    b.add_argument("--query-profile", action="append", choices=sorted(QUERY_PROFILES.keys()))
    b.add_argument("--max-per-day", type=int, default=500,
                   help="cap on UIDs per profile per day; 0 = no cap (page through every hit)")
    b.add_argument("--debug", action="store_true")
    b.add_argument("--fetch-biosample", action="store_true")
    b.add_argument("--fetch-bioproject", action="store_true")
    b.add_argument("--runinfo-max-rows", type=int, default=200000)
    add_refresh_arg(b)
    add_ai_args(b)

    c = sub.add_parser("crawl")
    c.add_argument("--query", default=DEFAULT_QUERY)
    c.add_argument("--query-profile", action="append", choices=sorted(QUERY_PROFILES.keys()))
    c.add_argument("--page-size", type=int, default=500)
    c.add_argument("--max-total", type=int, default=0)
    c.add_argument("--debug", action="store_true")
    c.add_argument("--fetch-biosample", action="store_true")
    c.add_argument("--fetch-bioproject", action="store_true")
    c.add_argument("--runinfo-max-rows", type=int, default=200000)
    c.add_argument("--stop-after-new-srr", type=int, default=0)
    c.add_argument("--sort", default="date")
    add_refresh_arg(c)
    add_ai_args(c)

    sub.add_parser("compact", help="rewrite data/srr_catalog_*.jsonl keeping only the newest record per run")

    a = sub.add_parser("curate-ai")
    a.add_argument("--model", default=OPENAI_MODEL)
    a.add_argument("--max-records", type=int, default=0)
    a.add_argument("--overwrite", action="store_true")
    a.add_argument("--year", type=int, default=0)
    return ap

def resolve_query_specs(args) -> List[Dict[str, str]]:
    selected_profiles = getattr(args, "query_profile", None) or []
    query = getattr(args, "query", DEFAULT_QUERY)
    use_default_profiles = (query == DEFAULT_QUERY) and not selected_profiles

    if use_default_profiles:
      return [{"name": name, "query": QUERY_PROFILES[name]} for name in DEFAULT_QUERY_PROFILE_NAMES]

    if selected_profiles:
      return [{"name": name, "query": QUERY_PROFILES[name]} for name in selected_profiles]

    return [{"name": "custom", "query": query}]

def run():
    args = build_argparser().parse_args()
    ensure_dirs()

    if args.cmd == "compact":
        from .exports import compact_catalog
        header("umdb compact: keep only the newest record per run")
        t0 = time.time()
        stats = compact_catalog()
        removed = stats["superseded_removed"]
        log(f"{num(stats['records_before'])} records read, "
            f"{paint(f'{removed:,} superseded removed', 'warn' if removed else 'dim')}, "
            f"{num(stats['records_after'])} kept in {fmt_duration(time.time() - t0)}")
        print("[SUMMARY] compact " + " ".join(f"{k}={v}" for k, v in stats.items()), flush=True)
        return

    if ((getattr(args, "ai_curate", False) or args.cmd == "curate-ai") and not OPENAI_API_KEY):
        raise RuntimeError("OPENAI_API_KEY is required for AI-assisted curation")

    from .config import SEEN_SRA_UIDS, SEEN_SRR_RUNS, DOCS_LATEST_SRR

    seen_sra = load_set(SEEN_SRA_UIDS)
    seen_srr = load_set(SEEN_SRR_RUNS)

    biosample_cache = read_json(BIOSAMPLE_CACHE, {})
    bp_cache = read_json(BIOPROJECT_CACHE, {})
    bp_uid_cache = read_json(BIOPROJECT_UID_CACHE, {})
    ai_cache = read_json(AI_CURATION_CACHE, {})

    latest_added: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    query_specs = resolve_query_specs(args)
    t_run = time.time()

    # --- What this run will do ---
    header(f"umdb {args.cmd}")
    if args.cmd != "curate-ai":
        log(f"Query profiles ({len(query_specs)}): "
            + ", ".join(paint(s["name"], "key") for s in query_specs)
            + paint(f"  (version {QUERY_PROFILES_VERSION})", "dim"))
        settings = []
        if args.cmd == "backfill-year":
            settings.append(f"year {num(str(args.year))}")
            settings.append("per-day cap " + (paint("none (all hits)", "num") if args.max_per_day <= 0 else num(args.max_per_day)))
        elif args.cmd == "daily":
            settings.append(f"last {num(args.recent_days)} days, up to {num(args.max_per_day)} per profile")
        elif args.cmd == "crawl":
            settings.append(f"page size {num(args.page_size)}, sort {args.sort}")
            if args.max_total:
                settings.append(f"max {num(args.max_total)} UIDs")
            if args.stop_after_new_srr:
                settings.append(f"stop after {num(args.stop_after_new_srr)} new runs")
        settings.append(paint("REFRESH: re-fetching records already held", "warn") if args.refresh else "new records only")
        settings.append("BioSample " + ("on" if args.fetch_biosample else paint("off", "warn")))
        settings.append("BioProject " + ("on" if args.fetch_bioproject else paint("off", "warn")))
        if getattr(args, "ai_curate", False):
            settings.append(paint(f"AI curation ({args.ai_model})", "magenta"))
        log("Settings: " + " · ".join(settings))
    log(f"Catalog: {num(len(seen_srr))} runs from {num(len(seen_sra))} experiments already ingested; "
        f"cached {num(len(biosample_cache))} BioSamples, {num(len(bp_cache))} BioProjects")
    if not ncbi_mod.NCBI_API_KEY:
        warn("NCBI_API_KEY is not set: NCBI allows 3 requests/s instead of 10 (add it to .env)")

    try:
        if args.cmd == "daily":
            tag = f"recent_{args.recent_days}d"
            paths = debug_paths(tag)
            uids: List[str] = []
            query_reports: List[Dict[str, Any]] = []
            seen_uids = set()
            for spec in query_specs:
                found_uids, esearch_url = esearch_recent("sra", spec["query"], args.recent_days, args.max_per_day)
                log(f"  {paint(spec['name'], 'key')}: {num(len(found_uids))} experiment(s)")
                if len(found_uids) >= args.max_per_day:
                    warn(f"{spec['name']} hit --max-per-day={args.max_per_day}; older matches were not retrieved")
                query_reports.append({
                    "profile": spec["name"],
                    "query": spec["query"],
                    "uids_count": len(found_uids),
                    "esearch_url": esearch_url,
                })
                for uid in found_uids:
                    if uid not in seen_uids:
                        seen_uids.add(uid)
                        uids.append(uid)
            summaries, esummary_url = esummary_sra(uids)

            if args.debug:
                write_json(paths["initial"], {
                    "tag": tag, "queries": query_reports, "recent_days": args.recent_days,
                    "max_per_day": args.max_per_day, "esearch_url": esearch_url,
                    "esummary_url": esummary_url, "uids_count": len(uids), "uids_sample": uids[:50],
                })

            if uids:
                added_srr, report = ingest_uids_to_srr(
                    tag=tag, uids=uids, summaries=summaries,
                    biosample_cache=biosample_cache, bp_cache=bp_cache, bp_uid_cache=bp_uid_cache,
                    seen_sra=seen_sra, seen_srr=seen_srr,
                    fetch_biosample=args.fetch_biosample, fetch_bioproject=args.fetch_bioproject,
                    debug=args.debug, runinfo_max_rows=args.runinfo_max_rows,
                    refresh=args.refresh,
                )
                if added_srr:
                    if args.ai_curate:
                        ai_counts = curate_records(
                            added_srr,
                            ai_cache=ai_cache,
                            biosample_cache=biosample_cache,
                            bioproject_cache=bp_cache,
                            model=args.ai_model,
                            max_records=args.ai_max_records,
                        )
                        report.setdefault("ai_curation", ai_counts)
                    print_report_summary(report)
                    year = dt.date.today().year
                    append_jsonl(f"{DATA_DIR}/srr_catalog_{year}.jsonl", added_srr)

                    for r in added_srr[:800]:
                        bp = r.get("bioproject", {}) if isinstance(r.get("bioproject", {}), dict) else {}
                        latest_added.append({
                            "tag": tag,
                            "srr": r.get("srr", ""),
                            "sra_uid": r.get("sra_uid", ""),
                            "title": r.get("title", ""),
                            "assay_class": (r.get("assay") or {}).get("assay_class", ""),
                            "country": (r.get("geo") or {}).get("country", ""),
                            "city": (r.get("geo") or {}).get("city", ""),
                            "bioproject": (r.get("runinfo_row") or {}).get("BioProject", ""),
                            "bioproject_title": bp.get("title", ""),
                            "url": (r.get("ncbi") or {}).get("srr_url", ""),
                        })
                reports.append(report)
            else:
                report = {
                    "tag": tag,
                    "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                    "counters": {"uids_input": 0, "uids_new": 0, "srr_emitted": 0},
                    "queries": query_reports,
                    "urls": {"esummary": esummary_url},
                }
                print_report_summary(report)
                reports.append(report)

        elif args.cmd == "backfill-year":
            year = args.year
            start = dt.date(year, 1, 1)
            today = dt.date.today()
            last = min(dt.date(year, 12, 31), today)
            days_total = (last - start).days + 1 if last >= start else 0
            year_runs = year_refreshed = year_experiments = 0
            t_year = time.time()
            if days_total <= 0:
                warn(f"{year} is in the future; nothing to search")
            for n in range(days_total):
                day = start + dt.timedelta(n)
                ds = day.isoformat()
                t_day = time.time()
                uids: List[str] = []
                seen_uids = set()
                hits = []
                for spec in query_specs:
                    found_uids, _ = esearch_day("sra", spec["query"], ds, args.max_per_day)
                    if found_uids:
                        hits.append(f"{spec['name']} {num(len(found_uids))}")
                    if args.max_per_day > 0 and len(found_uids) >= args.max_per_day:
                        warn(f"{ds} profile={spec['name']} hit --max-per-day={args.max_per_day}; "
                             "later records for this day were not retrieved")
                    for uid in found_uids:
                        if uid not in seen_uids:
                            seen_uids.add(uid)
                            uids.append(uid)
                year_experiments += len(uids)
                log(f"{paint(ds, 'bold')} day {n + 1}/{days_total}: "
                    + (f"{num(len(uids))} experiment(s)  " + paint("(" + ", ".join(hits) + ")", "dim") if uids
                       else paint("no matching experiments", "dim")))
                summaries, _ = esummary_sra(uids)

                added_srr, report = ingest_uids_to_srr(
                    tag=ds, uids=uids, summaries=summaries,
                    biosample_cache=biosample_cache, bp_cache=bp_cache, bp_uid_cache=bp_uid_cache,
                    seen_sra=seen_sra, seen_srr=seen_srr,
                    fetch_biosample=args.fetch_biosample, fetch_bioproject=args.fetch_bioproject,
                    debug=args.debug, runinfo_max_rows=args.runinfo_max_rows,
                    refresh=args.refresh,
                )
                if added_srr:
                    if args.ai_curate:
                        ai_counts = curate_records(
                            added_srr,
                            ai_cache=ai_cache,
                            biosample_cache=biosample_cache,
                            bioproject_cache=bp_cache,
                            model=args.ai_model,
                            max_records=args.ai_max_records,
                        )
                        report.setdefault("ai_curation", ai_counts)
                    append_jsonl(f"{DATA_DIR}/srr_catalog_{year}.jsonl", added_srr)
                print_report_summary(report)
                reports.append(report)

                c = report.get("counters", {})
                written, refreshed = c.get("srr_emitted", 0), c.get("srr_refreshed", 0)
                year_runs += written
                year_refreshed += refreshed
                done = n + 1
                eta = (time.time() - t_year) / done * (days_total - done)
                if uids:
                    errors = c.get("sra_uid_errors", 0)
                    log(f"  -> {paint(f'{written:,} runs written', 'good' if written else 'dim')}"
                        + (f" ({refreshed:,} refreshed)" if refreshed else "")
                        + (f", {paint(f'{errors} failed', 'err')}" if errors else "")
                        + f" in {fmt_duration(time.time() - t_day)}"
                        + paint(f"  | {year}: {year_runs:,} runs so far, ETA {fmt_duration(eta)}, "
                                f"{ncbi_mod.STATS['requests']:,} requests", "dim"))

            header(f"{year} complete")
            log(f"{num(days_total)} days searched, {num(year_experiments)} experiment hits, "
                f"{paint(f'{year_runs:,} runs written', 'good')}"
                + (f" ({num(year_refreshed)} refreshes of existing runs)" if year_refreshed else "")
                + f" in {fmt_duration(time.time() - t_year)}")

        elif args.cmd == "crawl":
            log("Paging through every match, newest first")
            retstart = 0
            page = 0
            total_seen = 0
            max_total = int(args.max_total or 0)
            new_srr_total = 0

            while True:
                ids: List[str] = []
                count_total = 0
                seen_page_ids = set()
                for spec in query_specs:
                    found_ids, found_total, _ = esearch_history(
                        "sra", spec["query"], retstart=retstart, retmax=args.page_size, sort=args.sort
                    )
                    count_total = max(count_total, found_total)
                    for uid in found_ids:
                        if uid not in seen_page_ids:
                            seen_page_ids.add(uid)
                            ids.append(uid)
                if not ids:
                    log("No more matches; crawl complete")
                    break

                summaries, _ = esummary_sra(ids)
                tag = f"crawl_{page:06d}"
                log(f"{paint(f'Page {page + 1}', 'bold')}: results {num(retstart + 1)}-{num(retstart + args.page_size)} "
                    f"of {num(count_total)} (largest profile), {num(len(ids))} unique experiments")
                added_srr, report = ingest_uids_to_srr(
                    tag=tag, 
                    uids=ids, 
                    summaries=summaries,
                    biosample_cache=biosample_cache, 
                    bp_cache=bp_cache, 
                    bp_uid_cache=bp_uid_cache,
                    seen_sra=seen_sra, 
                    seen_srr=seen_srr,
                    fetch_biosample=args.fetch_biosample, 
                    fetch_bioproject=args.fetch_bioproject,
                    debug=args.debug, runinfo_max_rows=args.runinfo_max_rows,
                    refresh=args.refresh,
                )

                new_srr_count = report["counters"].get("srr_emitted", 0)
                new_srr_total += new_srr_count

                if added_srr:
                    if args.ai_curate:
                        ai_counts = curate_records(
                            added_srr,
                            ai_cache=ai_cache,
                            biosample_cache=biosample_cache,
                            bioproject_cache=bp_cache,
                            model=args.ai_model,
                            max_records=args.ai_max_records,
                        )
                        report.setdefault("ai_curation", ai_counts)
                    this_year = dt.date.today().year
                    append_jsonl(f"{DATA_DIR}/srr_catalog_{this_year}.jsonl", added_srr)

                print_report_summary(report)
                reports.append(report)
                total_seen += len(ids)
                log(f"  -> {paint(f'{new_srr_count:,} runs written', 'good' if new_srr_count else 'dim')}"
                    + paint(f"  | {new_srr_total:,} this crawl, {total_seen:,} experiments scanned, "
                            f"{ncbi_mod.STATS['requests']:,} requests", "dim"))

                if args.stop_after_new_srr and new_srr_total >= args.stop_after_new_srr:
                    log(f"Stopping: reached --stop-after-new-srr={args.stop_after_new_srr}")
                    break

                retstart += args.page_size
                page += 1
                if retstart >= count_total:
                    log("Reached the end of the result list; crawl complete")
                    break
                if max_total and total_seen >= max_total:
                    log(f"Stopping: reached --max-total={max_total}")
                    break

        else:  # curate-ai
            records: List[Dict[str, Any]] = []
            year_prefix = f"srr_catalog_{args.year}.jsonl" if args.year else None
            from .exports import _find_year_catalog_prefixes
            for base in _find_year_catalog_prefixes():
                if year_prefix and year_prefix not in base:
                    continue
                for rec in iter_jsonl_glob(base):
                    records.append(rec)
                    if args.max_records and len(records) >= args.max_records:
                        break
                if args.max_records and len(records) >= args.max_records:
                    break

            if not records:
                manifest = read_json(f"{DB_DIR}/srr_records_manifest.json", {})
                parts = manifest.get("parts", []) if isinstance(manifest, dict) else []
                for part in parts:
                    part_path = part.get("path", "") if isinstance(part, dict) else ""
                    if not part_path:
                        continue
                    chunk = read_json(part_path, [])
                    if not isinstance(chunk, list):
                        continue
                    for rec in chunk:
                        records.append(rec)
                        if args.max_records and len(records) >= args.max_records:
                            break
                    if args.max_records and len(records) >= args.max_records:
                        break

            log(f"AI curation with {paint(args.model, 'magenta')}: {num(len(records))} record(s) loaded"
                + (" (overwriting cached reviews)" if args.overwrite else ""))
            ai_counts = curate_records(
                records,
                ai_cache=ai_cache,
                biosample_cache=biosample_cache,
                bioproject_cache=bp_cache,
                model=args.model,
                overwrite=args.overwrite,
                max_records=args.max_records,
            )
            log(f"  -> {paint(str(ai_counts.get('reviewed', 0)) + ' reviewed', 'good')}, "
                f"{num(ai_counts.get('skipped_cached', 0))} unchanged (cached), "
                + paint(f"{ai_counts.get('errors', 0)} errors", "err" if ai_counts.get("errors") else "dim"))
            report = {
                "tag": "curate_ai",
                "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                "ai_curation": ai_counts,
                "records_considered": len(records),
            }
            print_report_summary(report)
            reports.append(report)

        write_latest_srr_safe(latest_added[:5000])
        write_json(DOCS_LATEST_DEBUG, {
            "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "query_profiles_version": QUERY_PROFILES_VERSION,
            "query_profiles": [spec["name"] for spec in query_specs],
            "reports": reports
        })

        header("Rebuilding the published database (docs/db)")
        rebuild_srr_exports_chunked()

        totals: Dict[str, int] = {}
        for rep in reports:
            for k, v in (rep.get("counters") or {}).items():
                totals[k] = totals.get(k, 0) + int(v or 0)
        header(f"umdb {args.cmd} finished in {fmt_duration(time.time() - t_run)}")
        if args.cmd != "curate-ai":
            written = totals.get("srr_emitted", 0)
            log(f"{paint(f'{written:,} runs written', 'good' if written else 'dim')}"
                + (f" ({num(totals.get('srr_refreshed', 0))} refreshed)" if totals.get("srr_refreshed") else "")
                + f" from {num(totals.get('sra_uids_processed_ok', 0))} experiments"
                + (f", {paint(str(totals['sra_uid_errors']) + ' experiments failed', 'err')}" if totals.get("sra_uid_errors") else ""))
            log(f"Catalog now: {num(len(seen_srr))} runs from {num(len(seen_sra))} experiments")
        log(ncbi_mod.stats_line())

    except KeyboardInterrupt:
        warn("Interrupted. Caches are saved; completed batches are kept and the run can be resumed.")
        raise
    finally:
        log("Saving caches (BioSample, BioProject, AI)", level=2)
        write_json(BIOSAMPLE_CACHE, biosample_cache)
        write_json(BIOPROJECT_CACHE, bp_cache)
        write_json(BIOPROJECT_UID_CACHE, bp_uid_cache)
        write_json(AI_CURATION_CACHE, ai_cache)
