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
#   --max-per-day N    cap UIDs per profile per day (default 0 = no cap, page through all;
#                      with no cap each search spans a month, with a cap a day)
#   --profile NAME     restrict to a query profile (repeatable; default: the seven
#                      environment profiles; `umdb queries` lists them all)
#   --oldest-first     process years in ascending order (default: newest first)
#   --ai               also run AI curation on new runs (needs OPENAI_API_KEY)
#   --commit           git-commit data/ and docs/ after each completed year
#   --redo             ignore completion markers and re-run every year
#   --yes              do not ask before overwriting uncommitted data
#   -v, --verbose      show every NCBI request, cache hit and experiment
#   -q, --quiet        summaries and warnings only
#   --dry-run          show what would run
#
# Settings (NCBI_API_KEY, NCBI_EMAIL, OPENAI_API_KEY) are read from .env by the
# harvester. Logs and progress markers go to logs/backfill/ (git-ignored).
#
# Expect a long run: each year searches 365 days x every profile, then fetches
# run tables and BioSample/BioProject metadata for every new experiment.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- Output: colour in a terminal, plain text in log files -------------------
export PYTHONUNBUFFERED=1            # stream harvester progress through tee
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ] && [ "${UMDB_COLOR:-}" != "never" ]; then
  export UMDB_COLOR=always
  C_HEAD=$'\033[1;34m'; C_OK=$'\033[1;32m'; C_WARN=$'\033[33m'; C_ERR=$'\033[1;31m'
  C_NUM=$'\033[1;36m'; C_DIM=$'\033[2m'; C_RST=$'\033[0m'
else
  C_HEAD=""; C_OK=""; C_WARN=""; C_ERR=""; C_NUM=""; C_DIM=""; C_RST=""
fi
say()  { printf '%s %s\n' "${C_DIM}[$(date +%H:%M:%S)]${C_RST}" "$*"; }
head_() { printf '\n%s %s\n' "${C_DIM}[$(date +%H:%M:%S)]${C_RST}" "${C_HEAD}== $*${C_RST}"; }
warn_() { printf '%s %s\n' "${C_DIM}[$(date +%H:%M:%S)]${C_RST}" "${C_WARN}[WARN]${C_RST} $*" >&2; }
err_()  { printf '%s %s\n' "${C_DIM}[$(date +%H:%M:%S)]${C_RST}" "${C_ERR}[ERROR]${C_RST} $*" >&2; }
strip_ansi() { sed -E $'s/\x1b\\[[0-9;]*m//g'; }
dur() { local s=$1; if [ "$s" -ge 3600 ]; then printf '%dh%02dm' $((s/3600)) $((s%3600/60)); elif [ "$s" -ge 60 ]; then printf '%dm%02ds' $((s/60)) $((s%60)); else printf '%ds' "$s"; fi; }
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
    -v|--verbose) export UMDB_VERBOSITY=2; shift ;;
    -q|--quiet) export UMDB_VERBOSITY=0; shift ;;
    --profile) PROFILE_ARGS+=(--query-profile "$2"); shift 2 ;;
    --oldest-first) ORDER="asc"; shift ;;
    --ai) AI_ARGS=(--ai-curate); shift ;;
    --commit) COMMIT=1; shift ;;
    --redo) REDO=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,41p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
[ $has_key -eq 0 ] && warn_ "no NCBI_API_KEY in your shell or .env; the harvest will be about 3x slower."

STATE_DIR="logs/backfill"
mkdir -p "$STATE_DIR"
MARKER_SUFFIX="done"
[ $REFRESH -eq 1 ] && MARKER_SUFFIX="refresh.done"

# Refresh/compaction overwrite catalog data. Make sure it can be recovered.
if [ $DRY_RUN -eq 0 ] && { [ $REFRESH -eq 1 ] || [ $COMPACT -eq 1 ]; }; then
  if [ -n "$(git status --porcelain --untracked-files=no -- data 2>/dev/null)" ] && [ $ASSUME_YES -eq 0 ]; then
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

mode="new records only"
[ $REFRESH -eq 1 ] && mode="${C_WARN}REFRESH: re-fetch and overwrite existing records${C_RST}"
head_ "UMDB deep backfill: ${#years[@]} year(s), ${years[0]} → ${years[${#years[@]}-1]}"
say "Mode:        $mode"
say "Per-day cap: $([ "$MAX_PER_DAY" -eq 0 ] && echo "${C_NUM}none (every hit)${C_RST}" || echo "${C_NUM}$MAX_PER_DAY${C_RST}")"
say "Profiles:    ${PROFILE_ARGS[*]:-the seven default environment profiles}"
say "Compaction:  $([ $COMPACT -eq 1 ] && echo "after each year (superseded records deleted)" || echo off)"
say "AI curation: ${AI_ARGS[*]:-off}   Commit per year: $([ $COMMIT -eq 1 ] && echo yes || echo no)"
say "Logs:        $STATE_DIR/<year>.log   Markers: $STATE_DIR/<year>.$MARKER_SUFFIX"

completed=0; skipped=0; failed=(); total_runs=0; t_all=$(date +%s); idx=0; consecutive_fail=0
for year in "${years[@]}"; do
  idx=$((idx + 1))
  marker="$STATE_DIR/$year.$MARKER_SUFFIX"
  log="$STATE_DIR/$year.log"
  if [ -f "$marker" ] && [ $REDO -eq 0 ]; then
    say "${C_DIM}[$year] already done ($(cat "$marker")); skipping. Use --redo to re-run.${C_RST}"
    skipped=$((skipped + 1)); continue
  fi

  cmd=(python3 -m scripts.umdb_harvester backfill-year --year "$year"
       --max-per-day "$MAX_PER_DAY" --fetch-biosample --fetch-bioproject)
  [ ${#PROFILE_ARGS[@]} -gt 0 ] && cmd+=("${PROFILE_ARGS[@]}")
  [ ${#AI_ARGS[@]} -gt 0 ] && cmd+=("${AI_ARGS[@]}")
  [ $REFRESH -eq 1 ] && cmd+=(--refresh)

  if [ $DRY_RUN -eq 1 ]; then
    say "[$year] would run: ${KEEP_AWAKE[*]:-} ${cmd[*]}"
    [ $COMPACT -eq 1 ] && say "[$year] then: python3 -m scripts.umdb_harvester compact"
    continue
  fi

  head_ "Year $year ($idx of ${#years[@]})  log: $log"
  start=$(date +%s)
  : > "$log"
  if [ ${#KEEP_AWAKE[@]} -gt 0 ]; then
    "${KEEP_AWAKE[@]}" "${cmd[@]}" 2>&1 | tee >(strip_ansi >> "$log")
  else
    "${cmd[@]}" 2>&1 | tee >(strip_ansi >> "$log")
  fi
  status=${PIPESTATUS[0]}
  sleep 0.2   # let the log writer flush
  secs=$(( $(date +%s) - start ))
  mins=$(( secs / 60 ))

  if [ "$status" -ne 0 ]; then
    err_ "[$year] FAILED (exit $status) after $(dur $secs); see $log. Re-run the script to retry it."
    failed+=("$year")
    consecutive_fail=$((consecutive_fail + 1))
    [ "$status" -eq 130 ] && { warn_ "Interrupted; stopping. Re-run the same command to resume."; break; }
    # Problems that affect every year (TLS, network, credentials) should stop
    # the run instead of failing each remaining year in turn.
    if grep -q "TLS certificate verification failed" "$log"; then
      err_ "Stopping: NCBI's TLS certificate could not be verified. See the fixes in $log, then re-run."
      break
    fi
    if [ $consecutive_fail -ge 2 ]; then
      err_ "Stopping after $consecutive_fail consecutive failed years; fix the cause (see $log) and re-run to resume."
      break
    fi
    continue
  fi

  new_runs=$(grep -o 'srr_emitted=[0-9]*' "$log" | cut -d= -f2 | awk '{s+=$1} END {print s+0}')
  refreshed=$(grep -o '"srr_refreshed": [0-9]*' "$log" | awk '{s+=$2} END {print s+0}')
  capped=$(grep -c '\[WARN\].*max-per-day' "$log" || true)
  compact_note=""
  if [ $COMPACT -eq 1 ]; then
    if python3 -m scripts.umdb_harvester compact 2>&1 | tee >(strip_ansi >> "$log") | grep --line-buffered -v '^\[SUMMARY\]'; [ "${PIPESTATUS[0]}" -eq 0 ]; then
      compact_note=", catalog compacted"
    else
      err_ "[$year] compaction FAILED; see $log"
      failed+=("$year-compact")
    fi
  fi
  echo "$(date '+%Y-%m-%d %H:%M:%S') runs_written=$new_runs refreshed=$refreshed minutes=$mins capped_days=$capped" > "$marker"
  consecutive_fail=0
  total_runs=$((total_runs + new_runs))
  remaining=$(( ${#years[@]} - idx ))
  avg=$(( ($(date +%s) - t_all) / (completed + 1) ))
  say "${C_OK}[$year] done in $(dur $secs)${C_RST}: ${C_NUM}$new_runs${C_RST} runs written (${refreshed} refreshes of existing runs)$( [ "$capped" -gt 0 ] && echo ", ${C_WARN}$capped capped day searches${C_RST}")$compact_note"
  [ $remaining -gt 0 ] && say "${C_DIM}Overall: ${total_runs} runs so far; $remaining year(s) left, ETA ~$(dur $((avg * remaining)))${C_RST}"
  completed=$((completed + 1))

  if [ $COMMIT -eq 1 ]; then
    git add docs     
    if git diff --cached --quiet; then
      say "[$year] nothing to commit"
    else
      git commit -q -m "UMDB deep backfill $year ($new_runs runs written, $refreshed refreshed)" && say "${C_OK}[$year] committed${C_RST}"
    fi
  fi
done

head_ "Deep backfill finished in $(dur $(( $(date +%s) - t_all )))"
say "${C_OK}$completed completed${C_RST}, $skipped skipped, $([ ${#failed[@]} -gt 0 ] && echo "${C_ERR}${#failed[@]} failed (${failed[*]})${C_RST}" || echo "0 failed"); ${C_NUM}$total_runs${C_RST} runs written"
say "${C_DIM}The catalog in data/ is not in git: upload it with 'umdb catalog push' (add --snapshot for a dated copy).${C_RST}"
[ ${#failed[@]} -eq 0 ]
