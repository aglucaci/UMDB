# UMDB (Urban Microbiome Database)

**UMDB (Urban Microbiome Database)** is a GitHub-hosted bioinformatics resource for discovering, organizing, and publishing urban environmental sequencing metadata from public repositories.

The repository has two jobs:

1. Harvest and normalize public metadata from NCBI-linked resources.
2. Publish those records as a static website and downloadable database under `docs/`.

The current public site is available at:

https://aglucaci.github.io/UMDB/

> **How does UMDB decide a study is urban?** A broad keyword search of NCBI SRA decides what enters the catalog. A strict, sample-level **eligibility screen** then labels every run `confirmed_urban`, `probable_urban`, `uncertain`, `non_urban` or `non_environmental`, with the evidence behind the label. A city name alone never confirms a sample. Nothing is deleted: filter by tier in the explorer or on `eligibility.status`. See **[INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md)** for the exact queries, rules and limitations.

> **From catalog to science.** [`pipeline/`](pipeline/README.md) is a Nextflow pipeline (UMDB-SCIENCE) that tests pre-specified hypotheses on a frozen snapshot of the catalog. It covers the research enterprise now, from the database alone: coverage inequality, development, COVID-19, habitat bias, metadata completeness and technology diffusion. It also selects a balanced shotgun cohort and reprocesses it uniformly to test ecological principles: habitat vs geography, functional convergence, scaling laws, prediction in unsampled cities, mobility, connectivity, discovery, seasonality and resistomes. Results so far: [manuscript/catalog_findings](manuscript/catalog_findings/README.md). Programme: [manuscript/research_program.md](manuscript/research_program.md).

## The platform (v1.0)

UMDB is built to take a researcher from a scientific question to an analysis-ready cohort without writing code, and to let a computational biologist do the same programmatically.

| Product | Where | What you can do |
| --- | --- | --- |
| **Catalog** | [Explorer](https://aglucaci.github.io/UMDB/explorer.html) and a page for every [study](https://aglucaci.github.io/UMDB/study.html?acc=PRJNA271013), BioSample, run, city, country and habitat | Filter by place, habitat, assay, dates and quality; see submitted next to normalized metadata, curation evidence and quality flags |
| **Atlas** | [global-map.html](https://aglucaci.github.io/UMDB/global-map.html) | Sample and study density, habitats, samples per capita with surveillance gaps, location confidence; export the visible area |
| **Analytics** | [analytics.html](https://aglucaci.github.io/UMDB/analytics.html) | Coverage, growth, study concentration, release delay, library sizes, completeness; download any chart as CSV, SVG or PNG |
| **Workspace** | [workspace.html](https://aglucaci.github.io/UMDB/workspace.html) | Save cohorts, combine them (union, intersection, difference), compare them for imbalance, balance them by city and study with a seed, and download a research package |
| **Collections** | [collections.html](https://aglucaci.github.io/UMDB/collections.html) | Curated cohorts (global wastewater, transit, urban air, greenspace, virome, resistome) with inclusion criteria and run lists |
| **Profiles** | v2.0 | Taxonomic and functional profiles from the [UMDB-SCIENCE pipeline](pipeline/README.md) |

A research package contains `cohort.json` (definition, seed and database build), `metadata.tsv`, `runs.tsv` (also the pipeline's `--cohort` input), `accessions.txt`, `download_fastq.sh`, an nf-core/taxprofiler `samplesheet.csv`, `cohort_summary.json`, starter `analysis.py` and `analysis.R`, and `CITATION.bib`. The browser and `umdb cohort export` produce the same samples. Status, design decisions and what is left before v1.0.0: [ROADMAP.md](ROADMAP.md).

**Licensing.** UMDB's curation, annotations, collections and derived tables are CC BY-NC-SA 4.0. Sequence data and submitter metadata remain governed by the INSDC policies of their source archives (NCBI, ENA, DDBJ).

## Documentation

| Document | For |
| --- | --- |
| [INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md) | **How studies are selected as urban**: exact queries, eligibility tiers and rules, limitations, filtering recipes |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Setting up, running the harvester and tests, opening pull requests |
| [MAINTAINING.md](MAINTAINING.md) | Workflows, secrets, routine operations, troubleshooting, releases |
| [ARCHITECTURE.md](ARCHITECTURE.md) | How data flows: JSONL source of truth → SQLite query database → Parquet releases and website JSON; schema and example queries |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | What is pulled from NCBI (requests, volume, formats) and every field in the published files |
| [CURATION.md](CURATION.md) | Inclusion criteria, annotation rules, AI review, known limitations |
| [pipeline/README.md](pipeline/README.md) | **Research pipeline (Nextflow)**: hypotheses, methods, decision rules, parameters, how to add a test |
| [manuscript/research_program.md](manuscript/research_program.md) | The research programme built on UMDB: hypotheses, papers, flagship analysis, external data |
| [ROADMAP.md](ROADMAP.md) | The v1.0 platform: products, routes, design decisions, performance targets, what is left |
| [CHANGELOG.md](CHANGELOG.md) | What changed, including effects on catalog counts |
| [SECURITY.md](SECURITY.md) | Reporting vulnerabilities, handling API keys |
| [CITATION.cff](CITATION.cff) | How to cite UMDB |
| [LICENSE](LICENSE) | CC BY-NC-SA 4.0 |

## Quick Start

```bash
bin/umdb install                          # puts `umdb` on your PATH (~/.local/bin)
cp .env.example .env                      # add NCBI_API_KEY and NCBI_EMAIL
umdb test                                 # offline tests
umdb crawl --sort date --page-size 100 --stop-after-new-srr 10 --fetch-bioproject --fetch-biosample
umdb serve                                # preview at http://localhost:8000
```

| Command | Does |
| --- | --- |
| `umdb crawl` / `daily` / `backfill-year` / `curate-ai` | Harvester commands (also `umdb run <command>`); add `--help` for options |
| `umdb deepest [START END]` | Deepest possible search, year by year from 2007: every profile, every hit, re-fetches existing records and overwrites superseded data |
| `umdb backfill [START END]` | Resumable year-by-year search for new records only (`--dry-run`, `--commit`, `--profile`, `--refresh`, …) |
| `umdb compact` | Rewrite `data/` keeping only the newest record per run |
| `umdb scrub` | Remove `api_key=` / `email=` from stored NCBI URLs in `data/`, `docs/` and `logs/` |
| `umdb db build` / `info` / `shell` / `query "SQL"` | Build and query the SQLite database `data/umdb.sqlite` (normalized, indexed, full-text search) |
| `umdb release [--version V]` | Write a versioned release: Parquet tables, TSV, compressed SQLite, indexes, collections, documentation, schema, manifest, checksums and Zenodo metadata (`pip install -r requirements-release.txt`) |
| `umdb cohort list` / `collection SLUG` / `export cohort.json` | Research packages for curated collections or for cohorts saved in the website Workspace (`--max-per-cell`, `--max-per-project`, `--target`, `--seed` to balance) |
| `umdb export` | Rebuild `data/umdb.sqlite` and the website data in `docs/db/` offline (re-applies geo, assay and eligibility rules) |
| `umdb catalog pull` / `push` | Restore or upload the harvested catalog (`data/`), which lives in GitHub Release assets, not git |
| `umdb repair` | Re-fetch BioProjects cached under the wrong accession |
| `umdb geo fetch` / `umdb geo check "USA: Portland, ME"` | Download the GeoNames gazetteer; show how one location string is validated |
| `umdb serve [port]` | Preview the website |
| `umdb status` | Release size, eligibility tiers, settings check |
| `umdb queries` | Print the exact discovery queries |
| `umdb science [opts]` / `umdb science test` | Run the Nextflow research pipeline on the catalog, or on its synthetic test fixture (needs Nextflow) |
| `umdb test` | Run the test suite |

`umdb` works from any directory. In an interactive terminal it prints a UMDB banner on stderr (never into pipes or files); set `UMDB_NO_BANNER=1` to hide it.

**Output.** By default every command reports what it is doing: settings and catalog state at the start, then hits per profile for each day or page, experiments ingested with progress and ETA, runs written, export steps with eligibility tiers, and a closing summary with NCBI request statistics. Output is colour-coded in a terminal: green for runs written, yellow for warnings, red for errors, cyan for counts, and per-tier colours. Add `-v` / `--verbose` to see every NCBI request (API key redacted), cache hit and experiment, or `-q` / `--quiet` for summaries only. `NO_COLOR=1` gives plain output. Log files written by `umdb backfill` / `umdb deepest` are always plain text. Without installing it, use `bin/umdb` or `python3 -m scripts.umdb_harvester <command>` from the repository root. No packages need installing (Python standard library only).

## What This Repository Is

UMDB (Urban Microbiome Database) is designed as scientific infrastructure for urban metagenomics and related environmental sequencing studies. It focuses on public sequencing records associated with urban, built-environment, wastewater, air, surface, and similar city-linked contexts.

Rather than operating as a traditional server-backed application, the repository stores harvested records directly in versioned files and publishes a static web interface through GitHub Pages. This keeps the project easy to inspect, easy to archive, and inexpensive to maintain.

## What The Website Provides

The published site includes:

- a home page that resolves BioProject, BioSample and run accessions straight to their pages
- the Explorer: studies, BioSamples and runs filtered by validated place, habitat, assay, collection and release year, eligibility tier and location quality, with every filter in the page address
- record pages for studies, BioSamples, runs, cities, countries and habitats, showing submitted and normalized metadata side by side with evidence, quality flags, provenance and downloads
- the Atlas: country coverage, sampling sites, habitats, study density, samples per capita with surveillance gaps, and location confidence
- Analytics with downloadable charts (CSV, SVG, PNG)
- the Workspace and Collections (see above)
- reviewer-facing pages: `About`, `Methodology`, `Data Access`

## How The Repository Works

At a high level, UMDB (Urban Microbiome Database) follows this flow:

1. Discover candidate public records from NCBI-linked resources.
2. Retrieve run-level metadata for SRR entries.
3. Join associated BioProject and BioSample context when available.
4. Derive practical annotations such as assay class and geographic labels.
5. Store the resulting records in local append-only or chunked artifacts.
6. Export static JSON files that power the website.
7. Publish the `docs/` directory through GitHub Pages.

This means the repository itself is both the processing workspace and the published database release.

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

The website can then distinguish between original-only records and AI-fixed records.

## Repository Layout

The repository is organized around three main areas: harvesting code, intermediate data, and published web artifacts.

```text
UMDB/
├── data/                       (git-ignored; restore with `umdb catalog pull`)
│   ├── cache/
│   │   ├── bioproject.json
│   │   ├── bioproject_uid.json
│   │   └── biosample.json
│   ├── seen_sra_uids.txt
│   ├── seen_srr_runs.txt
│   ├── srr_catalog_<YEAR>*.jsonl
│   └── umdb.sqlite
├── docs/
│   ├── index.html, explorer.html, global-map.html (Atlas), analytics.html
│   ├── study.html, sample.html, run.html, city.html, country.html, habitat.html
│   ├── collections.html, workspace.html, 404.html (short routes)
│   ├── about.html, methodology.html, data.html
│   ├── latest_srr.json
│   ├── assets/
│   │   ├── db.js            query engine shared by every page
│   │   ├── ui.js            labels, charts, workspace storage
│   │   ├── entity.js, collections.js, workspace.js
│   │   ├── cohort-templates.json
│   │   └── site.css
│   └── db/
│       ├── summary.json
│       ├── cube.json
│       ├── projects.json
│       ├── manifest.json, analytics.json, population.json, collections.json
│       ├── collections/<slug>.tsv.gz
│       ├── ix/<NN>.json      accession -> project lookup
│       └── p/<BioProject>.json
├── logo/
├── manuscript/                 resource-paper draft, research programme, catalog findings
├── pipeline/                   UMDB-SCIENCE Nextflow pipeline (see pipeline/README.md)
│   ├── main.nf, nextflow.config
│   ├── modules/local/          catalog.nf (snapshot, covariates, cohort, tests), reads.nf (SRA → profiles)
│   ├── subworkflows/local/     profile_reads.nf
│   ├── bin/                    one script per hypothesis (umdb_c*.py, umdb_h*.py) + shared statistics
│   ├── assets/hypotheses.csv   the hypothesis registry
│   ├── envs/analysis.yml
│   └── tests/                  synthetic fixture with planted effects; unit tests
├── scripts/
│   └── umdb_harvester/
│       ├── ai_curation.py
│       ├── cli.py
│       ├── ingest.py
│       ├── exports.py
│       ├── ncbi.py
│       ├── bioproject.py
│       ├── biosample.py
│       ├── assay.py
│       ├── utils.py
│       └── config.py
├── tests/
├── .env.example
├── ARCHITECTURE.md, INCLUSION_CRITERIA.md, CONTRIBUTING.md, MAINTAINING.md, DATA_DICTIONARY.md, CURATION.md
├── CHANGELOG.md, SECURITY.md, CITATION.cff, LICENSE
└── README.md
```

## What Lives In The Harvester Package

The Python package under `scripts/umdb_harvester/` is the operational core of UMDB. It powers the current UMDB pipeline and published site.

- `cli.py` defines command-line entry behavior.
- `ingest.py` coordinates harvesting and record assembly.
- `ncbi.py` contains public NCBI request logic.
- `bioproject.py` and `biosample.py` handle project and sample enrichment.
- `assay.py` derives higher-level assay labels from run metadata.
- `exports.py` writes the static JSON artifacts used by the website.
- `config.py` centralizes path and output settings.
- `utils.py` provides common helpers for JSON, timestamps, and iteration.

Together, these modules take public metadata and turn it into the catalog in `data/`, the SQLite database, the website data under `docs/db/` and the Parquet releases.

## What Lives In `data/`

The `data/` directory is the working data store for harvesting and enrichment.

- `seen_sra_uids.txt` and `seen_srr_runs.txt` track identifiers that have already been processed.
- `srr_catalog_*.jsonl` files store accumulated run-level records.
- `cache/` stores locally reused metadata lookups such as BioProject and BioSample responses.

This directory is important because it represents the stateful layer of the pipeline. It is where UMDB remembers what it has seen and where it stages enriched metadata before export. It is not in git: `umdb catalog pull` restores it from the `catalog-latest` GitHub release and `umdb catalog push` uploads it (see [MAINTAINING.md](MAINTAINING.md#the-catalog-lives-in-release-assets)).

## What Lives In `docs/`

The `docs/` directory is the public release of the project.

- HTML pages provide the user-facing web interface.
- `docs/db/` contains machine-readable database artifacts.
- `latest_srr.json` provides a compact recent-record feed.
- `assets/` contains shared JavaScript and CSS used across the site.

Anything under `docs/` should be treated as published output, not just internal project files.

## Database Model

The website loads a compact index instead of every run record:

- `docs/db/summary.json`: headline counts and top lists.
- `docs/db/cube.json`: runs aggregated by BioProject, release year, country, city, assay, eligibility tier, urban context and rounded coordinates. Every filter, chart and the map work from it.
- `docs/db/projects.json`: one row per BioProject.
- `docs/db/p/<BioProject>.json`: that project's BioSamples and runs, fetched when it is opened or exported.

Together the first three are about 0.4 MB compressed. They are built from `data/umdb.sqlite` by `umdb export` (and after every harvest). Field reference: [DATA_DICTIONARY.md](DATA_DICTIONARY.md#published-files-docs); design: [ARCHITECTURE.md](ARCHITECTURE.md#website-data). Full tables are published as versioned Parquet releases.

What is fetched from NCBI, data volumes, formats and every field are documented in [DATA_DICTIONARY.md](DATA_DICTIONARY.md#what-umdb-pulls-from-ncbi).

## Why The Site Aggregates By BioProject

Many users want to discover studies, not just individual runs. UMDB therefore groups records by BioProject in the main explorer while preserving the underlying SRR records for inspection.

This gives the site two layers:

- a study-facing summary layer for browsing
- a run-facing provenance layer for detail and reuse

That structure is useful for both casual exploration and manuscript review.

## Publication And Reviewer-Facing Pages

The site now includes pages that help present the repository as a maintained scientific database:

- `about.html` explains the scope and intended use of the resource
- `methodology.html` describes derivation logic, provenance, and caveats
- `data.html` explains downloads and schema expectations
- `analytics.html` summarizes global counts and distributions
- `global-map.html` visualizes country-level coverage

These pages are meant to reduce ambiguity for collaborators, manuscript reviewers, and future users of the database.

## Design Principles

UMDB follows a few simple design principles:

- static publication over server complexity
- auditable files over opaque backend state
- public metadata reuse over proprietary infrastructure
- browser access plus machine-readable exports
- low-cost maintenance with transparent artifacts

## Typical Update Pattern

Although the exact command flow may evolve, a normal update cycle looks like this:

1. Harvest new or target records into `data/`.
2. Update caches and enrichments.
3. Rebuild exported JSON release artifacts in `docs/db/`.
4. Refresh site pages that consume the latest summaries.
5. Commit the updated data and published files.

In other words, a repository update is also a database release.

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

This workflow runs the harvester CLI, rebuilds the exported database artifacts, and commits updated `data/` and `docs/` outputs back to the repository.

## Output Formats

UMDB publishes several output styles because different users need different levels of structure:

- **JSONL** for accumulation-oriented internal catalogs in `data/`
- **JSON** for the public website and machine-readable exports in `docs/`
- **HTML** for the user-facing static interface
- **shell scripts and accession lists** generated from browser searches for raw FASTQ retrieval

At the moment, the public website is centered on JSON-driven delivery rather than a live backend database.

## Downloading Raw Datasets

UMDB is primarily a metadata and discovery resource, but the web explorer now supports bundling matched search results into download-ready dataset cohorts.

From the main explorer, a user can:

- export a JSON bundle of the currently matched runs
- export a plain-text list of SRR accessions
- export a shell script that uses `prefetch` and `fasterq-dump` from SRA Toolkit

This makes it possible to go from a filtered UMDB search to raw FASTQ retrieval without manually collecting accessions one by one.

## Research Pipeline

UMDB's next step is to move from cataloging urban sequencing data to finding the ecological, evolutionary and biogeographical rules that govern microbial life in cities. The working hypothesis is that urbanization acts as a global ecological and evolutionary filter. The [UMDB-SCIENCE Nextflow pipeline](pipeline/README.md) is the infrastructure for testing it:

```bash
umdb science test                                  # synthetic fixture with planted effects (~2 min)
umdb science -profile conda --fetch_worldbank      # catalog hypotheses + shotgun cohort audit
umdb science -profile slurm,conda --run_reads --sylph_db … --resfinder_db …   # reprocess the cohort
```

- **Track A · catalog** (minutes, from `data/umdb.sqlite`): C0 data-quality audit; C1 coverage vs population and blind-spot cities; C2 development; C3 COVID-19 interrupted time series; C4 habitat representation; C5 metadata completeness; C6 centre concentration; C7 technology diffusion; C8 blinded expert benchmark of the eligibility screen.
- **Track B · reads**: a habitat-, city- and study-balanced shotgun cohort is streamed from SRA, quality-controlled and depth-standardized, then profiled for species (sylph/GTDB or MetaPhlAn 4), functions (HUMAnN → KEGG orthologs), resistance genes (KMA/ResFinder) and viral contigs (MEGAHIT → geNomad).
- **Track C · ecology**: H1 habitat vs geography; H2 functional convergence; H3 scaling laws; H4 prediction in held-out cities; H5 air-travel connectivity; H6 cross-habitat connectivity; H9 undiscovered diversity; H10 hemisphere-aligned seasonality; H11 resistome ecology. Evolution (MAGs + HyPhy), viral persistence, homogenization, resilience and climate redistribution are on the roadmap.

Each test writes its decision rule in its docstring, uses one frozen catalog snapshot, and reports `supported`, `not_supported`, `inconclusive` or `descriptive` with caveats. External covariates (GeoNames, World Bank, OpenFlights, optionally Köppen–Geiger) are versioned with checksums. On the 2026-11-16 catalog, Track A found coverage strongly concentrated relative to population (concentration index 0.82) and tied to GDP per capita, wastewater dominating urban samples (71%), and assay-dependent metadata gaps. It also showed that the balanced shotgun cohort is 9,221 samples from 774 cities ([details](manuscript/catalog_findings/README.md)).

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
| Storage | Git repository artifacts | Low to free |
| Hosting | GitHub Pages | Free |
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
