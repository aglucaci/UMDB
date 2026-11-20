# UMDB (Urban Microbiome Database)

**UMDB** harvests, curates and releases public urban microbiome sequencing metadata. It finds urban environmental sequencing studies in the INSDC archives (NCBI SRA, BioSample, BioProject), screens every BioSample for urban evidence, validates its location against GeoNames, and publishes the result as versioned, citable data releases.

UMDB is one of three repositories:

| Repository | Role |
| --- | --- |
| **UMDB** (this one) | Harvesting and curation: the catalog (`data/`, `umdb.sqlite`), curated collections, and the versioned **data releases** |
| [STRIGA](https://github.com/aglucaci/STRIGA) | The analysis workflow (Nextflow): catalog-level tests, a balanced shotgun cohort, read profiling and ecological hypothesis tests on a UMDB catalog |
| [UMA](https://github.com/aglucaci/UMA) | The data browser at **https://aglucaci.github.io/UMA/**: Explorer, Atlas, Analytics and STRIGA findings, Collections and Workspace over the latest release |

```text
NCBI SRA / BioSample / BioProject ──► UMDB harvest + curation ──► data/umdb.sqlite
                                                                       │
                                     STRIGA (+ World Bank, GeoNames,   │ --catalog data/
                                     OpenFlights, …) ◄─────────────────┘
                                          │ results/ (striga_manifest.json)
                                          ▼
                    umdb release --version V --analysis results/  ──►  UMDB release V
                    (Parquet, TSV, SQLite, collections, GeoNames         (GitHub Release,
                     reference, STRIGA results, checksums)                Zenodo DOI)
                                                                              │
                                                       uma build ◄────────────┘ ──► aglucaci.github.io/UMA
```

A release is the unit everything else refers to: STRIGA records the SHA-256 of the database it analysed and `umdb release --analysis` refuses results from a different one, and every UMA page states which release it shows. The old address, aglucaci.github.io/UMDB, redirects to UMA.

> **How does UMDB decide a study is urban?** A broad keyword search of NCBI SRA decides what enters the catalog. A strict, sample-level **eligibility screen** then labels every BioSample `confirmed_urban`, `probable_urban`, `uncertain`, `non_urban` or `non_environmental`, with the evidence behind the label. A city name alone never confirms a sample. Nothing is deleted: filter by tier in UMA or on `urban_status`. See **[INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md)** for the exact queries, rules and limitations.

**Licensing.** UMDB's curation, annotations, collections and derived tables are CC BY-NC-SA 4.0. Sequence data and submitter metadata remain governed by the INSDC policies of their source archives (NCBI, ENA, DDBJ). Releases record both (`manifest.json` → `licenses`).

## Documentation

| Document | For |
| --- | --- |
| [INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md) | **How studies are selected as urban**: exact queries, eligibility tiers and rules, limitations, filtering recipes |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Setting up, running the harvester and tests, opening pull requests |
| [MAINTAINING.md](MAINTAINING.md) | Workflows, secrets, routine operations, **making a release**, troubleshooting |
| [ARCHITECTURE.md](ARCHITECTURE.md) | How data flows: JSONL source of truth → SQLite → releases; the release contract STRIGA and UMA rely on; schema and example queries |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | What is pulled from NCBI (requests, volume, formats) and every field in a release |
| [CURATION.md](CURATION.md) | Inclusion criteria, annotation rules, AI review, known limitations |
| [ROADMAP.md](ROADMAP.md) | Products across the three repositories, what is left before v1.0.0 |
| [CHANGELOG.md](CHANGELOG.md) | What changed, including effects on catalog counts |
| [manuscript/abstract.md](manuscript/abstract.md) | The resource paper draft (the research programme and catalog findings are in STRIGA's `docs/`) |
| [SECURITY.md](SECURITY.md) | Reporting vulnerabilities, handling API keys |
| [CITATION.cff](CITATION.cff) | How to cite UMDB |
| [LICENSE](LICENSE) | CC BY-NC-SA 4.0 |

## Quick Start

```bash
bin/umdb install                          # puts `umdb` on your PATH (~/.local/bin)
cp .env.example .env                      # add NCBI_API_KEY and NCBI_EMAIL
umdb test                                 # offline tests
umdb catalog pull && umdb db build        # restore the harvested catalog and build data/umdb.sqlite
umdb crawl --sort date --page-size 100 --stop-after-new-srr 10 --fetch-bioproject --fetch-biosample
```

| Command | Does |
| --- | --- |
| `umdb crawl` / `daily` / `backfill-year` / `curate-ai` | Harvester commands (also `umdb run <command>`); add `--help` for options. Each finishes by rebuilding `data/umdb.sqlite` |
| `umdb deepest [START END]` | Deepest possible search, year by year from 2007: every profile, every hit, re-fetches existing records and overwrites superseded data |
| `umdb backfill [START END]` | Resumable year-by-year search for new records only (`--dry-run`, `--commit`, `--profile`, `--refresh`, …) |
| `umdb compact` | Rewrite `data/` keeping only the newest record per run |
| `umdb scrub` | Remove `api_key=` / `email=` from stored NCBI URLs in `data/`, `reports/` and `logs/` |
| `umdb db build` / `info` / `shell` / `query "SQL"` | Build and query the SQLite database `data/umdb.sqlite` (normalized, indexed, full-text search) |
| `umdb release --version V [--analysis DIR]` | Write release V: Parquet tables, TSV, compressed SQLite, collections, GeoNames reference, documentation, STRIGA results from `DIR`, schema, manifest, checksums, Zenodo metadata and one `umdb-V.tar.gz` bundle (`pip install -r requirements-release.txt`) |
| `umdb cohort list` / `collection SLUG` / `export cohort.json` | Research packages for curated collections or for cohorts saved in the UMA Workspace (`--max-per-cell`, `--max-per-project`, `--target`, `--seed` to balance) |
| `umdb catalog pull` / `push` | Restore or upload the harvested catalog (`data/`), which lives in GitHub Release assets, not git |
| `umdb repair` | Re-fetch BioProjects cached under the wrong accession |
| `umdb geo fetch` / `umdb geo check "USA: Portland, ME"` | Download the GeoNames gazetteer; show how one location string is validated |
| `umdb status` | Database build, counts, eligibility tiers, settings check |
| `umdb queries` | Print the exact discovery queries |
| `umdb test` | Run the test suite |

`umdb` works from any directory. In an interactive terminal it prints a UMDB banner on stderr (never into pipes or files); set `UMDB_NO_BANNER=1` to hide it.

**Output.** By default every command reports what it is doing: settings and catalog state at the start, then hits per profile for each day or page, experiments ingested with progress and ETA, runs written, database steps with eligibility tiers, and a closing summary with NCBI request statistics. Output is colour-coded in a terminal: green for runs written, yellow for warnings, red for errors, cyan for counts, and per-tier colours. Add `-v` / `--verbose` to see every NCBI request (API key redacted), cache hit and experiment, or `-q` / `--quiet` for summaries only. `NO_COLOR=1` gives plain output. Log files written by `umdb backfill` / `umdb deepest` are always plain text. Without installing it, use `bin/umdb` or `python3 -m scripts.umdb_harvester <command>` from the repository root. Harvesting needs only the Python standard library; releases need pyarrow.

## Making a release

```bash
umdb catalog pull && umdb db build                                   # the catalog to release
nextflow run aglucaci/STRIGA -r v0.2.0 -profile conda --catalog data --fetch_worldbank --outdir results
umdb release --version 1.0.0 --analysis results                      # checks STRIGA analysed this database
gh release create v1.0.0 dist/releases/umdb-1.0.0.tar.gz --title "UMDB 1.0.0"
```

Then deposit the bundle in Zenodo, add the DOI to the release's `manifest.json` and `CITATION.cff`, and rebuild UMA from it (its "Build site from a UMDB release" workflow, or `uma fetch 1.0.0 && uma build 1.0.0`). Details: [MAINTAINING.md](MAINTAINING.md#releases).

## Data Sources

UMDB (Urban Microbiome Database) uses public repository metadata. The main sources currently represented in the code and outputs are:

- **NCBI SRA** for run-level sequencing metadata
- **NCBI BioProject** for project-level metadata
- **NCBI BioSample** for sample and location context
- **NCBI E-utilities** for public metadata retrieval and cross-linking

All records remain subject to the quality and completeness of the original public submissions.

## Discovery Strategy

Discovery uses seven urban-environment query profiles, one per environment UMDB recognises: municipal wastewater, air and dust, built environment, the `urban metagenome` label, transit, urban soil and greenspace, and urban water and drainage. Each relies on NCBI's environment organism labels (for example `wastewater metagenome`) and on urban phrases ("public transit", "urban soil"), never on catch-all words like "surface" or "air" alone, and none excludes whole habitats. SARS-CoV-2 targeted amplicon surveillance is available as an opt-in profile. `umdb queries` prints the exact queries.

Matching a query decides inclusion; a rule-based eligibility screen then labels each run's urban evidence. See [INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md) for the full criteria and their known gaps.

Even with broader discovery, UMDB should still be treated as a high-recall public index rather than a guaranteed complete census of every urban microbiome study ever performed. Repository metadata are inconsistent, and some studies are only discoverable through follow-up curation.

## AI-Assisted Curation

UMDB can now add an AI-assisted curation layer on top of the original public metadata.

This curation layer is designed to:

- judge whether the available metadata are sufficient for city-level interpretation
- decide whether the sample appears to come from a truly urban or city-associated context
- inspect BioSample attributes, titles, descriptions, and run metadata together
- propose corrected country, city, or assay annotations when the evidence supports it
- preserve the original annotations while publishing AI-reviewed final values alongside them

AI curation is stored separately in cache and merged into the published dataset at export time as an `ai_curation` object on each record. The original repository metadata remain unchanged.

### AI Curation Fields

When present, `ai_curation` can include fields such as:

- `metadata_sufficiency`
- `metadata_sufficient`
- `urban_origin`
- `confidence`
- `environment_type`
- `ai_fixed`
- `original_country`
- `original_city`
- `original_assay_class`
- `final_country`
- `final_city`
- `final_assay_class`
- `reasoning_summary`
- `evidence`

UMA can then distinguish between original-only records and AI-fixed records.

## Repository Layout

```text
UMDB/
├── data/                       (git-ignored; restore with `umdb catalog pull`)
│   ├── cache/                  bioproject.json, bioproject_uid.json, biosample.json, ai_curation.json
│   ├── reference/geonames/     the gazetteer locations are validated against (`umdb geo fetch`)
│   ├── seen_sra_uids.txt, seen_srr_runs.txt
│   ├── srr_catalog_<YEAR>*.jsonl   the source of truth
│   └── umdb.sqlite             the query database (`umdb db build`)
├── dist/releases/<V>/          versioned releases and umdb-<V>.tar.gz (git-ignored; published as GitHub Releases)
├── reports/                    latest_srr.json and debug/latest_report.json, committed by the scheduled workflows
├── docs/                       redirect from the old website address to UMA
├── scripts/
│   ├── umdb_harvester/         the harvester, database, curation, cohorts and release code
│   └── deep_backfill.sh
├── bin/umdb                    the command line
├── manuscript/abstract.md      resource-paper draft
├── tests/
├── logo/
├── .env.example
├── ARCHITECTURE.md, INCLUSION_CRITERIA.md, CONTRIBUTING.md, MAINTAINING.md, DATA_DICTIONARY.md, CURATION.md, ROADMAP.md
├── CHANGELOG.md, SECURITY.md, CITATION.cff, LICENSE
└── README.md
```

## What Lives In The Harvester Package

`scripts/umdb_harvester/` is the operational core of UMDB:

- `cli.py` defines the command line; `ingest.py` coordinates harvesting and record assembly; `ncbi.py` holds the NCBI request logic.
- `bioproject.py` and `biosample.py` handle project and sample enrichment; `assay.py` derives assay classes; `geo.py` validates locations against GeoNames; `eligibility.py` is the urban eligibility screen; `ai_curation.py` the optional AI review.
- `exports.py` reads and maintains the JSONL catalog; `catalog.py` stores it in GitHub Release assets; `database.py` builds `data/umdb.sqlite`.
- `cohorts.py` defines the curated collections and research packages; `release.py` writes the versioned releases.
- `config.py` holds paths, query profiles and settings; `utils.py` common helpers.

## What Lives In `data/`

`data/` is the working data store for harvesting and enrichment: `seen_*.txt` track identifiers already processed, `srr_catalog_*.jsonl` store the run-level records, and `cache/` stores BioProject, BioSample and AI-review lookups. It is the stateful layer: where UMDB remembers what it has seen. It is not in git: `umdb catalog pull` restores it from the `catalog-latest` GitHub release and `umdb catalog push` uploads it (see [MAINTAINING.md](MAINTAINING.md#the-catalog-lives-in-release-assets)).

What is fetched from NCBI, data volumes, formats and every field are documented in [DATA_DICTIONARY.md](DATA_DICTIONARY.md#what-umdb-pulls-from-ncbi).

## Design Principles

- auditable files over opaque backend state: the JSONL catalog is the source of truth, everything else is rebuilt from it
- versioned releases as the unit of reuse: analyses (STRIGA) and the website (UMA) refer to a release, never to a moving catalog
- public metadata reuse over proprietary infrastructure
- low-cost maintenance: GitHub Actions, Release assets and Pages

## Local AI Curation

To use AI-assisted metadata curation locally, add `OPENAI_API_KEY` (and optionally `OPENAI_MODEL`) to your `.env` alongside the NCBI settings. See [.env.example](.env.example). Variables exported in your shell override the file.

You can curate newly harvested records during a deep crawl:

```bash
python3 -m scripts.umdb_harvester crawl \
  --fetch-biosample \
  --fetch-bioproject \
  --page-size 500 \
  --sort date \
  --max-total 0 \
  --stop-after-new-srr 0 \
  --ai-curate
```

You can also run AI curation against existing harvested records:

```bash
python3 -m scripts.umdb_harvester curate-ai \
  --model gpt-4o-mini \
  --max-records 500
```

To overwrite previously cached AI reviews:

```bash
python3 -m scripts.umdb_harvester curate-ai \
  --model gpt-4o-mini \
  --overwrite \
  --max-records 500
```

To target a single year:

```bash
python3 -m scripts.umdb_harvester curate-ai \
  --year 2024 \
  --max-records 500
```

## GitHub Actions AI Curation

The repository also includes a dedicated full-dataset AI curation workflow:

- Workflow file: `.github/workflows/umdb_full_ai_curation.yml`
- Workflow name in GitHub Actions: `UMDB Full AI Curation`
- Required secret: `OPENAI_API_KEY`
- Optional but recommended secrets for NCBI-linked context: `NCBI_API_KEY`, `NCBI_EMAIL`

To run it in GitHub:

1. Open the repository Actions tab.
2. Select `UMDB Full AI Curation`.
3. Click `Run workflow`.
4. Leave `max_records` as `0` to process the entire dataset.
5. Leave `year` blank to process all years, or set a single year for a targeted pass.
6. Turn on `overwrite` only when you want to replace cached AI reviews.

This workflow runs the harvester CLI, uploads the updated catalog to the `catalog-latest` release assets, rebuilds `data/umdb.sqlite` and commits the run reports in `reports/`.

## Limitations

UMDB is useful, but it should be interpreted carefully.

- Geographic metadata may be incomplete, ambiguous, or inconsistently formatted.
- Assay labels are practical summary categories, not formal reannotation of every study.
- Inclusion reflects what is publicly available and discoverable through the current harvesting logic.
- Presence in UMDB does not imply endorsement, quality ranking, or uniform study design.

These limitations are expected for public metadata integration projects and should be stated clearly in any manuscript built around the resource.

## Cost And Infrastructure Model

The project is intentionally lightweight.

| Component | Provider | Cost |
| --- | --- | --- |
| Compute | Local runs or GitHub-based workflows | Low to free |
| Storage | GitHub Release assets (catalog, releases) | Free |
| Hosting | GitHub Pages (UMA) | Free |
| Metadata access | NCBI public utilities | Free within usage guidance |

## License

UMDB (code, documentation, curated annotations and published data files) is licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). You may share and adapt it for non-commercial purposes with attribution, and must distribute adaptations under the same license. See [LICENSE](LICENSE). Underlying NCBI metadata remain subject to NCBI's data use policies. To cite UMDB, see [CITATION.cff](CITATION.cff).

## Disclaimer

UMDB is provided for research and informational purposes only. It does not constitute medical advice, clinical guidance, or public health policy.

## Author

**Alexander G. Lucaci, PhD**  
Computational Evolutionary Biology  
Urban Metagenomics  
Genomic Surveillance
