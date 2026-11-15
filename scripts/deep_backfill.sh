#!/usr/bin/env bash
# Deep, resumable, year-by-year UMDB harvest.
#
# Runs `backfill-year` for each year with every query profile, paging through
# every hit for every day (no per-day cap), so each year is searched as deeply
# as NCBI allows. Completed years are recorded and skipped on re-run, so you
# can stop with Ctrl-C at any time and resume later.
#
# Usage (from anywhere, or as `umdb backfill` / `umdb deepest`):
#   scripts/deep_backfill.sh --deepest            # everything, 2007 .. now, overwriting old data
#   scripts/deep_backfill.sh                      # new records only, 2007 .. now, newest first
#   scripts/deep_backfill.sh 2018 2024            # a range
#   scripts/deep_backfill.sh 2024 2024 --refresh  # re-fetch one year, superseding old records
#   scripts/deep_backfill.sh --dry-run            # print the plan, run nothing
#
# Options:
#   --deepest          = --refresh --compact --max-per-day 0, all profiles, 2007 .. now
#   --refresh          re-ingest experiments/runs already in the catalog and re-fetch
#                      their BioSample/BioProject metadata; the new record supersedes
#                      the old one (old data is overwritten at compaction)
#   --compact          after each year, rewrite data/srr_catalog_*.jsonl keeping only the
#                      newest record per run (permanent; earlier versions stay in git)
#   --max-per-day N    cap UIDs per profile per day (default 0 = no cap, page through all)
#   --profile NAME     restrict to a query profile (repeatable; default: all five)
#   --oldest-first     process years in ascending order (default: newest first)
#   --ai               also run AI curation on new runs (needs OPENAI_API_KEY)
#   --commit           git-commit data/ and docs/ after each completed year
#   --redo             ignore completion markers and re-run every year
#   --yes              do not ask before overwriting uncommitted data
#   --dry-run          show what would run
#
# Settings (NCBI_API_KEY, NCBI_EMAIL, OPENAI_API_KEY) are read from .env by the
# harvester. Logs and progress markers go to logs/backfill/ (git-ignored).
#
# Expect a long run: each year searches 365 days x every profile, then fetches
# run tables and BioSample/BioProject metadata for every new experiment.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

START_YEAR=2007          # first year of public SRA releases
END_YEAR="$(date +%Y)"
MAX_PER_DAY=0            # 0 = no cap
PROFILE_ARGS=()
ORDER="desc"
AI_ARGS=()
COMMIT=0
REDO=0
DRY_RUN=0
REFRESH=0
COMPACT=0
ASSUME_YES=0

positional=()
while [ $# -gt 0 ]; do
  case "$1" in
    --max-per-day) MAX_PER_DAY="$2"; shift 2 ;;
    --deepest) REFRESH=1; COMPACT=1; MAX_PER_DAY=0; shift ;;
    --refresh) REFRESH=1; shift ;;
    --compact) COMPACT=1; shift ;;
    --yes|-y) ASSUME_YES=1; shift ;;
    --profile) PROFILE_ARGS+=(--query-profile "$2"); shift 2 ;;
    --oldest-first) ORDER="asc"; shift ;;
    --ai) AI_ARGS=(--ai-curate); shift ;;
    --commit) COMMIT=1; shift ;;
    --redo) REDO=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "Unknown option: $1" >&2; exit 2 ;;
    *) positional+=("$1"); shift ;;
  esac
done
[ ${#positional[@]} -ge 1 ] && START_YEAR="${positional[0]}"
[ ${#positional[@]} -ge 2 ] && END_YEAR="${positional[1]}"

if ! [[ "$START_YEAR" =~ ^[0-9]{4}$ && "$END_YEAR" =~ ^[0-9]{4}$ ]] || [ "$START_YEAR" -gt "$END_YEAR" ]; then
  echo "Years must be YYYY and START <= END (got $START_YEAR..$END_YEAR)" >&2
  exit 2
fi
if ! [[ "$MAX_PER_DAY" =~ ^[0-9]+$ ]]; then
  echo "--max-per-day must be a whole number (0 = no cap)" >&2
  exit 2
fi

# Is an NCBI key configured (shell or .env)? Without one NCBI allows 3 req/s.
has_key=0
[ -n "${NCBI_API_KEY:-}" ] && has_key=1
if [ $has_key -eq 0 ] && [ -f .env ] && grep -Eq '^[[:space:]]*(export[[:space:]]+)?NCBI_API_KEY=["'"'"']?[^"'"'"'[:space:]]+' .env; then
  has_key=1
fi
[ $has_key -eq 0 ] && echo "WARNING: no NCBI_API_KEY in your shell or .env; the harvest will be about 3x slower." >&2

STATE_DIR="logs/backfill"
mkdir -p "$STATE_DIR"
MARKER_SUFFIX="done"
[ $REFRESH -eq 1 ] && MARKER_SUFFIX="refresh.done"

# Refresh/compaction overwrite catalog data. Make sure it can be recovered.
if [ $DRY_RUN -eq 0 ] && { [ $REFRESH -eq 1 ] || [ $COMPACT -eq 1 ]; }; then
  if [ -n "$(git status --porcelain -- data 2>/dev/null)" ] && [ $ASSUME_YES -eq 0 ]; then
    echo "data/ has uncommitted changes. --refresh/--compact will overwrite catalog records," >&2
    echo "and uncommitted versions cannot be recovered from git." >&2
    printf "Continue anyway? [y/N] " >&2
    read -r reply
    case "$reply" in y|Y|yes|YES) ;; *) echo "Aborted. Commit data/ first, or pass --yes." >&2; exit 1 ;; esac
  fi
fi

years=()
if [ "$ORDER" = "asc" ]; then
  y=$START_YEAR; while [ "$y" -le "$END_YEAR" ]; do years+=("$y"); y=$((y + 1)); done
else
  y=$END_YEAR; while [ "$y" -ge "$START_YEAR" ]; do years+=("$y"); y=$((y - 1)); done
fi

# Keep the Mac awake while running (macOS only; harmless elsewhere).
KEEP_AWAKE=()
command -v caffeinate >/dev/null 2>&1 && KEEP_AWAKE=(caffeinate -i)

echo "UMDB deep backfill: years ${years[*]}"
echo "  max-per-day=$([ "$MAX_PER_DAY" -eq 0 ] && echo unlimited || echo "$MAX_PER_DAY") profiles=${PROFILE_ARGS[*]:-all} refresh=$REFRESH compact=$COMPACT ai=${AI_ARGS[*]:-off} commit=$COMMIT"
echo "  logs and markers: $STATE_DIR/"

completed=0; skipped=0; failed=()
for year in "${years[@]}"; do
  marker="$STATE_DIR/$year.$MARKER_SUFFIX"
  log="$STATE_DIR/$year.log"
  if [ -f "$marker" ] && [ $REDO -eq 0 ]; then
    echo "[$year] already done ($(cat "$marker")); skipping. Use --redo to re-run."
    skipped=$((skipped + 1)); continue
  fi

  cmd=(python3 -m scripts.umdb_harvester backfill-year --year "$year"
       --max-per-day "$MAX_PER_DAY" --fetch-biosample --fetch-bioproject)
  [ ${#PROFILE_ARGS[@]} -gt 0 ] && cmd+=("${PROFILE_ARGS[@]}")
  [ ${#AI_ARGS[@]} -gt 0 ] && cmd+=("${AI_ARGS[@]}")
  [ $REFRESH -eq 1 ] && cmd+=(--refresh)

  if [ $DRY_RUN -eq 1 ]; then
    echo "[$year] would run: ${KEEP_AWAKE[*]:-} ${cmd[*]}"
    [ $COMPACT -eq 1 ] && echo "[$year] then: python3 -m scripts.umdb_harvester compact"
    continue
  fi

  echo "[$year] started $(date '+%Y-%m-%d %H:%M:%S'); log: $log"
  start=$(date +%s)
  if [ ${#KEEP_AWAKE[@]} -gt 0 ]; then
    "${KEEP_AWAKE[@]}" "${cmd[@]}" 2>&1 | tee "$log"
  else
    "${cmd[@]}" 2>&1 | tee "$log"
  fi
  status=${PIPESTATUS[0]}
  mins=$(( ($(date +%s) - start) / 60 ))

  if [ "$status" -ne 0 ]; then
    echo "[$year] FAILED (exit $status) after ${mins} min; see $log. Re-run the script to retry it." >&2
    failed+=("$year")
    [ "$status" -eq 130 ] && { echo "Interrupted; stopping."; break; }
    continue
  fi

  new_runs=$(grep -o 'srr_emitted=[0-9]*' "$log" | cut -d= -f2 | awk '{s+=$1} END {print s+0}')
  refreshed=$(grep -o '"srr_refreshed": [0-9]*' "$log" | awk '{s+=$2} END {print s+0}')
  capped=$(grep -c '^\[WARN\].*max-per-day' "$log" || true)
  compact_note=""
  if [ $COMPACT -eq 1 ]; then
    if python3 -m scripts.umdb_harvester compact 2>&1 | tee -a "$log" | grep '^\[SUMMARY\] compact'; then
      compact_note=", catalog compacted"
    else
      echo "[$year] compaction FAILED; see $log" >&2
      failed+=("$year-compact")
    fi
  fi
  echo "$(date '+%Y-%m-%d %H:%M:%S') runs_written=$new_runs refreshed=$refreshed minutes=$mins capped_days=$capped" > "$marker"
  echo "[$year] done in ${mins} min: $new_runs runs written ($refreshed were refreshes of existing runs), $capped capped day searches$compact_note"
  completed=$((completed + 1))

  if [ $COMMIT -eq 1 ]; then
    git add data docs
    if git diff --cached --quiet; then
      echo "[$year] nothing to commit"
    else
      git commit -q -m "UMDB deep backfill $year ($new_runs runs written, $refreshed refreshed)" && echo "[$year] committed"
    fi
  fi
done

echo
echo "Summary: $completed completed, $skipped skipped, ${#failed[@]} failed${failed[*]:+ (${failed[*]})}"
[ ${#failed[@]} -eq 0 ]
