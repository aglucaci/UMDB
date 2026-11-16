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
from .utils import log, header, warn, paint, num, fmt_duration, VERBOSITY, TIER_STYLE
from . import ncbi as ncbi_mod
from .ncbi import esearch_recent, esearch_range, esearch_history, esummary_sra, parallel_map
from .ingest import ingest_uids_to_srr, debug_paths
from .exports import write_latest_srr_safe
from .ai_curation import curate_records

def _window_kind(args) -> str:
    w = getattr(args, "window", "day")
    if w == "auto":
        return "month" if args.max_per_day <= 0 else "day"
    return w


def date_windows(year: int, kind: str, today: "dt.date | None" = None) -> List[tuple]:
    """(start, end) release-date windows covering a year up to today, newest
    days included; kind is day, week or month."""
    today = today or dt.date.today()
    start, last = dt.date(year, 1, 1), min(dt.date(year, 12, 31), today)
    out = []
    a = start
    while a <= last:
        if kind == "month":
            nxt = dt.date(a.year + (a.month == 12), a.month % 12 + 1, 1)
            b = nxt - dt.timedelta(1)
        elif kind == "week":
            b = a + dt.timedelta(6)
        else:
            b = a
        b = min(b, last)
        out.append((a, b))
        a = b + dt.timedelta(1)
    return out


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

    def add_db_arg(parser):
        parser.add_argument("--no-db", action="store_true", help="skip rebuilding data/umdb.sqlite after the export")

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
    add_db_arg(d)
    add_ai_args(d)

    b = sub.add_parser("backfill-year")
    b.add_argument("--year", type=int, required=True)
    b.add_argument("--query", default=DEFAULT_QUERY)
    b.add_argument("--query-profile", action="append", choices=sorted(QUERY_PROFILES.keys()))
    b.add_argument("--max-per-day", type=int, default=500,
                   help="cap on UIDs per profile per day; 0 = no cap (page through every hit)")
    b.add_argument("--window", choices=["auto", "day", "week", "month"], default="auto",
                   help="release-date span per search: auto = month with no cap, day with a cap. "
                        "Wider windows need far fewer searches; results are identical")
    b.add_argument("--debug", action="store_true")
    b.add_argument("--fetch-biosample", action="store_true")
    b.add_argument("--fetch-bioproject", action="store_true")
    b.add_argument("--runinfo-max-rows", type=int, default=200000)
    add_refresh_arg(b)
    add_db_arg(b)
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
    add_db_arg(c)
    add_ai_args(c)

    sub.add_parser("compact", help="rewrite data/srr_catalog_*.jsonl keeping only the newest record per run")
    sub.add_parser("repair-bioprojects", help="re-fetch cached BioProjects stored under the wrong accession")
    sub.add_parser("scrub-secrets", help="remove api_key=/email= from stored URLs in data/, docs/ and logs/")
    cat = sub.add_parser("catalog", help="move the harvested catalog to/from GitHub Release assets")
    cat.add_argument("action", choices=["push", "pull", "status", "pack"])
    cat.add_argument("--tag", default="catalog-latest", help="release to pull from (default catalog-latest)")
    cat.add_argument("--merge", action="store_true", help="pull: keep local records and add published ones")
    cat.add_argument("--force", action="store_true", help="pull: replace local changes; push: overwrite a newer upload")
    cat.add_argument("--snapshot", action="store_true", help="push: also create a dated catalog-YYYY.MM.DD release")
    sub.add_parser("export", help="rebuild data/umdb.sqlite and the website data in docs/db (offline)")
    sub.add_parser("db-build", help="(re)build data/umdb.sqlite from the catalog and caches")
    sub.add_parser("db-info", help="show database build metadata and sample tiers")
    g = sub.add_parser("geo", help="GeoNames reference files and location checks")
    gsub = g.add_subparsers(dest="geo_cmd", required=True)
    gf = gsub.add_parser("fetch", help="download the GeoNames reference files (about 14 MB) into data/reference/geonames")
    gf.add_argument("--force", action="store_true", help="download again even if the files exist")
    gc = gsub.add_parser("check", help="resolve one location string, e.g. 'USA: Portland, ME'")
    gc.add_argument("location")
    gc.add_argument("--lat", type=float, default=None)
    gc.add_argument("--lon", type=float, default=None)
    rel = sub.add_parser("release", help="write a versioned Parquet/TSV release from data/umdb.sqlite")
    rel.add_argument("--version", default="", help="release version (default: today's date, YYYY.MM.DD)")
    rel.add_argument("--rebuild-db", action="store_true", help="rebuild the database first")

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

def publish_site():
    """Rebuild data/umdb.sqlite from the catalog, then the website data (docs/db)."""
    from .database import build_database
    from .site import build_site
    header("Rebuilding the SQLite database (data/umdb.sqlite)")
    build_database()
    header("Rebuilding the website data (docs/db)")
    build_site()


def run():
    args = build_argparser().parse_args()
    ensure_dirs()

    if args.cmd == "repair-bioprojects":
        from .bioproject import repair_bioproject_cache
        header("umdb repair: BioProject cache")
        bp_cache = read_json(BIOPROJECT_CACHE, {})
        uid_cache = read_json(BIOPROJECT_UID_CACHE, {})
        fixed = repair_bioproject_cache(bp_cache, uid_cache)
        still = [k for k in fixed if (bp_cache.get(k) or {}).get("accession", "").upper() != k.upper()]
        write_json(BIOPROJECT_CACHE, bp_cache)
        write_json(BIOPROJECT_UID_CACHE, uid_cache)
        log(f"{paint(f'{len(fixed) - len(still):,} BioProject(s) repaired', 'good')}"
            + (f", {paint(f'{len(still)} still mismatched', 'warn')}" if still else "")
            + "; run `umdb export` and `umdb db build` to apply them to published records")
        log(ncbi_mod.stats_line())
        return
    if args.cmd == "scrub-secrets":
        from .utils import scrub_files
        header("umdb scrub-secrets")
        changed = scrub_files()
        for path, n in sorted(changed.items()):
            log(f"  {path}: {num(n)} line(s) cleaned")
        log(paint(f"Cleaned {len(changed)} file(s)", "good") if changed else "No credentials found in stored files")
        return
    if args.cmd == "export":
        publish_site()
        return
    if args.cmd == "catalog":
        from . import catalog
        header(f"umdb catalog {args.action}")
        try:
            if args.action == "push":
                catalog.push(snapshot=args.snapshot, force=args.force)
            elif args.action == "pull":
                catalog.pull(tag=args.tag, merge=args.merge, force=args.force)
            elif args.action == "pack":
                catalog.pack()
            else:
                catalog.status()
        except (RuntimeError, FileNotFoundError) as exc:
            from .utils import error
            error(str(exc))
            raise SystemExit(1)
        return
    if args.cmd == "db-build":
        from .database import build_database
        header("umdb db build")
        build_database()
        return
    if args.cmd == "geo":
        from . import geo
        if args.geo_cmd == "fetch":
            header("umdb geo fetch")
            src = geo.fetch_geonames(force=args.force)
            for name, f in src["files"].items():
                log(f"  {name:22} {f['bytes'] / 1e6:6.1f} MB  sha256 {f['sha256'][:16]}…")
            log(f"Saved to {geo.GEONAMES_DIR}/ ({src['license']}). Rebuild with `umdb export`.")
            return
        if not geo.gazetteer_available():
            geo.fetch_geonames()
        gaz = geo.Gazetteer()
        country = args.location.split(":", 1)[0] if ":" in args.location else args.location.split(",", 1)[0]
        a = geo.Resolver(gaz).resolve(args.location, country, "biosample", args.lat, args.lon)
        place = gaz.place_record(a.pop("place_index")) if a.get("place_index") is not None else None
        a.pop("place_index", None)
        for k, v in a.items():
            log(f"  {k:16} {v}")
        if place:
            log(f"  {'place':16} {place['name']} (GeoNames {place['geonames_id']}, {place['feature_code']}, "
                f"population {num(place['population'])}, {place['lat']:.4f}, {place['lon']:.4f})")
        return
    if args.cmd == "db-info":
        from .database import db_info, DB_PATH
        info = db_info()
        header(f"{DB_PATH} ({info['size_mb']:.1f} MB)")
        for k, v in info["meta"].items():
            log(f"  {k:24} {v}")
        log("Samples by eligibility tier:")
        for tier, n in info["sample_tiers"]:
            log(f"  {paint(str(tier).ljust(18), TIER_STYLE.get(tier, 'dim'))} {num(n)}")
        return
    if args.cmd == "release":
        from .release import write_release
        header("umdb release")
        try:
            if args.rebuild_db:
                from .database import build_database
                build_database()
            write_release(args.version)
        except (RuntimeError, FileNotFoundError, FileExistsError) as exc:
            from .utils import error
            error(str(exc))
            raise SystemExit(1)
        return

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

    def save_caches():
        log("Saving caches (BioSample, BioProject, AI)", level=2)
        write_json(BIOSAMPLE_CACHE, biosample_cache)
        write_json(BIOPROJECT_CACHE, bp_cache)
        write_json(BIOPROJECT_UID_CACHE, bp_uid_cache)
        write_json(AI_CURATION_CACHE, ai_cache)

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
            settings.append(f"window {paint(_window_kind(args), 'num')}")
        elif args.cmd == "daily":
            settings.append(f"last {num(args.recent_days)} days, up to {num(args.max_per_day)} per profile")
        elif args.cmd == "crawl":
            settings.append(f"page size {num(args.page_size)}, sort {args.sort}")
            if args.max_total:
                settings.append(f"max {num(args.max_total)} UIDs")
            if args.stop_after_new_srr:
                settings.append(f"stop after {num(args.stop_after_new_srr)} new runs")
        settings.append(f"NCBI {ncbi_mod.NCBI_RPS:g} req/s, {ncbi_mod.NCBI_WORKERS} parallel")
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
            found_all = parallel_map(
                lambda spec: esearch_recent("sra", spec["query"], args.recent_days, args.max_per_day), query_specs)
            for spec, (found_uids, esearch_url) in zip(query_specs, found_all):
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
            windows = date_windows(year, _window_kind(args), dt.date.today())
            days_total = sum((b - a).days + 1 for a, b in windows)
            year_runs = year_refreshed = year_experiments = 0
            t_year = time.time()
            if not windows:
                warn(f"{year} is in the future; nothing to search")
            days_done = 0
            for n, (w_start, w_end) in enumerate(windows):
                ds = w_start.isoformat()
                w_days = (w_end - w_start).days + 1
                cap = args.max_per_day * w_days if args.max_per_day > 0 else 0
                label = (f"{ds} day {days_done + 1}/{days_total}" if w_days == 1 else
                         f"{ds} .. {w_end.isoformat()} (window {n + 1}/{len(windows)})")
                if w_days > 1:
                    ds = f"{ds}_{w_end.isoformat()}"
                t_day = time.time()
                uids: List[str] = []
                seen_uids = set()
                hits = []
                found_all = parallel_map(
                    lambda spec: esearch_range("sra", spec["query"], w_start.isoformat(), w_end.isoformat(), cap),
                    query_specs)
                for spec, (found_uids, _) in zip(query_specs, found_all):
                    if found_uids:
                        hits.append(f"{spec['name']} {num(len(found_uids))}")
                    if cap > 0 and len(found_uids) >= cap:
                        warn(f"{ds} profile={spec['name']} hit --max-per-day={args.max_per_day}; "
                             "later records for this window were not retrieved")
                    for uid in found_uids:
                        if uid not in seen_uids:
                            seen_uids.add(uid)
                            uids.append(uid)
                year_experiments += len(uids)
                log(f"{paint(label, 'bold')}: "
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
                days_done += w_days
                eta = (time.time() - t_year) / days_done * (days_total - days_done)
                if uids:
                    errors = c.get("sra_uid_errors", 0)
                    log(f"  -> {paint(f'{written:,} runs written', 'good' if written else 'dim')}"
                        + (f" ({refreshed:,} refreshed)" if refreshed else "")
                        + (f", {paint(f'{errors} failed', 'err')}" if errors else "")
                        + f" in {fmt_duration(time.time() - t_day)}"
                        + paint(f"  | {year}: {year_runs:,} runs so far, ETA {fmt_duration(eta)}, "
                                f"{ncbi_mod.STATS['requests']:,} requests", "dim"))

            header(f"{year} complete")
            log(f"{num(days_total)} days searched in {num(len(windows))} window(s), {num(year_experiments)} experiment hits, "
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
                found_all = parallel_map(lambda spec: esearch_history(
                    "sra", spec["query"], retstart=retstart, retmax=args.page_size, sort=args.sort), query_specs)
                for found_ids, found_total, _ in found_all:
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

        # Save this run's BioSample/BioProject fetches first: the database and
        # site re-apply the rules from the caches on disk.
        save_caches()
        if getattr(args, "no_db", False):
            warn("--no-db: skipped rebuilding data/umdb.sqlite and the website data (run `umdb export` later)")
        else:
            publish_site()

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
        save_caches()
