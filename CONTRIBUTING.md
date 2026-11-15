# Contributing to UMDB

Thanks for helping improve the Urban Microbiome Database. Contributions of every size are welcome: reporting a missing study, correcting a record, improving the classifiers, or working on the website.

- **Not a programmer?** Open an issue. The [issue templates](.github/ISSUE_TEMPLATE/) cover missing studies, metadata corrections and bugs.
- **Changing code?** Read this file, then [MAINTAINING.md](MAINTAINING.md) for how the pipeline runs in production.
- **Changing how records are annotated?** Read [CURATION.md](CURATION.md) first. Annotation rules are scientific decisions and need a rationale and tests.

## Ways to contribute

| You want to… | Do this |
| --- | --- |
| Report a study UMDB is missing | Open a **Missing study** issue with the BioProject accession |
| Report a wrong country, city, assay or urban label | Open a **Metadata correction** issue with the run or BioSample accession and the evidence |
| Report a website or pipeline bug | Open a **Bug report** issue |
| Improve discovery, classification or exports | Open a pull request (see below) |

## Development setup

The harvester uses only the Python standard library (3.11 in CI; 3.9+ works locally). There is nothing to install.

```bash
git clone https://github.com/aglucaci/UMDB.git
cd UMDB
cp .env.example .env   # then add NCBI_API_KEY and NCBI_EMAIL
```

`.env` is git-ignored and loaded automatically when the harvester runs from the repository root. Values already exported in your shell override it.

**Run every command from the repository root.** The harvester resolves `data/` and `docs/` relative to the current directory.

### Run the tests

```bash
umdb test -v        # or: python3 -m unittest discover tests -v
```

Tests are offline: NCBI calls are mocked. Please keep it that way, so tests never need credentials or network access.

### The `umdb` command

`bin/umdb` wraps everything below and works from any directory. Install it once with `bin/umdb install` (symlinks into `~/.local/bin`), then run `umdb help`.

### Run the harvester

```bash
umdb crawl --help
umdb crawl --sort date --page-size 100 --stop-after-new-srr 10 --fetch-bioproject --fetch-biosample
# equivalent without the wrapper:
python3 -m scripts.umdb_harvester crawl --sort date --page-size 100 --stop-after-new-srr 10 --fetch-bioproject --fetch-biosample
```

Use `python3 -m scripts.umdb_harvester`, not `python3 -m scripts.umdb_harvester.cli`: `cli.py` has no entry point, so that form exits without doing anything.

For a deep, year-by-year harvest use `umdb backfill` (`scripts/deep_backfill.sh`) (see [MAINTAINING.md](MAINTAINING.md#backfill-years-deepest-search)).

Every harvester command finishes by rebuilding `docs/db/`. To rebuild exports only:

```bash
umdb export
```

### Preview the website

```bash
umdb serve          # or: cd docs && python3 -m http.server 8000
```

Open http://localhost:8000. A server is required: the pages `fetch()` JSON, which browsers block from `file://`.

### Throw away a local harvest

Local runs modify tracked files in `data/` and `docs/`. To discard them:

```bash
git restore data docs
```

## Pull requests

1. Branch from `main` (`git switch -c fix/geo-parsing`).
2. Keep code changes and data changes in **separate commits**, ideally separate PRs. A reviewer cannot read a 100 MB JSON diff; they can read a 50-line classifier change.
3. Add or update tests for any change to parsing, classification, discovery queries or exports.
4. Run `python3 -m unittest discover tests` before pushing.
5. Fill in the pull request template, including the effect on catalog counts when annotation rules change (see below).
6. Add a line under `Unreleased` in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

### Measuring the effect of a rule change

Changes to `assay.py`, `biosample.py`, `eligibility.py` (bump `RULES_VERSION`) or query profiles can shift published numbers by thousands of runs. Before opening the PR, compare old and new annotations across the local catalog and paste the before/after counts into the PR description. `exports.refresh_derived_annotations()` re-derives annotations offline from cached metadata. The [CHANGELOG](CHANGELOG.md) entry for the 2026-11 geography fix shows the expected level of detail.

## Code guidelines

- **Standard library only** for the harvester, unless there is a strong reason. A new dependency also needs a `requirements.txt` (the workflows already install one if present).
- **Never discard original metadata.** Derived values (geo, assay, AI review) are stored next to the raw values, never in place of them. See [CURATION.md](CURATION.md).
- **Do not guess.** When a parser cannot decide, leave the field empty and record why (for example `city_status: "ambiguous"`). An empty field is better than a confident wrong one.
- **Be polite to NCBI.** Batch requests, keep the backoff in `ncbi.http_get`, and set `NCBI_EMAIL` and `NCBI_API_KEY`. See the [E-utilities usage guidelines](https://www.ncbi.nlm.nih.gov/books/NBK25497/).
- **Version discovery queries.** Any edit to `QUERY_PROFILES` in `config.py` must bump `QUERY_PROFILES_VERSION` and note the reason in the comment above it.
- **Keep output files under 50 MB.** `MAX_OUTPUT_BYTES` keeps part files below GitHub's 100 MB hard limit and its 50 MB warning.
- Match the existing style: type hints, small functions, `from __future__ import annotations`.

## Data guidelines

- **Do not hand-edit** `docs/db/*.json` or `data/srr_catalog_*.jsonl`. Fix the code or the cache and regenerate. Hand edits are overwritten on the next rebuild and leave no provenance.
- Corrections to individual records belong in the curation layer, not in the raw catalog. Until a manual-override file exists, open a **Metadata correction** issue.
- Do not commit `.env`, API keys or anything under `data/debug/`.

## Project layout

| Path | What it is |
| --- | --- |
| `scripts/umdb_harvester/` | Harvester package (discovery, enrichment, classification, export) |
| `tests/` | Offline unit tests |
| `data/` | Working state: run catalog (JSONL), seen-ID lists, NCBI response caches |
| `docs/` | Published GitHub Pages site and the `docs/db/` database release |
| `.github/workflows/` | Scheduled crawl and on-demand AI curation |
| `logo/`, `docs/press/` | Brand assets and press kit |

Module-level detail is in the [README](README.md#what-lives-in-the-harvester-package); field-level detail is in [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## License of contributions

UMDB is licensed under [CC BY-NC-SA 4.0](LICENSE). By opening a pull request you agree that your contribution is released under the same license.

## Security

Never put secrets in issues, pull requests, commits or workflow logs. See [SECURITY.md](SECURITY.md) for how to report a vulnerability or a leaked key.
