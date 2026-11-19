# UMDB research programme: towards predictive urban microbial ecology

> **Status: planning document.** It describes what the UMDB-SCIENCE pipeline (`pipeline/`) is built to test and
> how the results map onto papers. Journal targets are aspirations that depend on the strength of the results.
> Catalog-level results so far: [`catalog_findings/`](catalog_findings/README.md).

## Central hypothesis

**Urbanization acts as a global ecological and evolutionary filter, producing predictable patterns of microbial composition, functional adaptation, dispersal and persistence across geographically distinct cities.**

This treats cities as interconnected ecosystems with potentially general properties, not only as places where microorganisms are collected. UMDB is suited to testing it because it integrates independently generated studies, several sequencing technologies, many habitats and longitudinal sampling, all screened at the sample level for urban origin.

UMDB catalogs sequencing metadata, not abundance profiles. Hypotheses about the research enterprise (where, how and by whom cities are sequenced) can be tested on the catalog now (Track A). Hypotheses about microbes need a uniformly reprocessed shotgun cohort (Tracks B and C).

## Hypotheses and where they are tested

| ID | Hypothesis | Programme | Pipeline status |
| --- | --- | --- | --- |
| C1 | Urban sequencing coverage is geographically unequal relative to population | UMDB resource | implemented; **supported** |
| C2 | Surveillance capacity tracks national economic development | UMDB resource | implemented; **supported** |
| C3 | COVID-19 structurally changed urban sequencing | UMDB resource | implemented; not supported under the pre-set rule, sensitive to truncation |
| C4 | Some urban habitats are systematically under-sampled | UMDB resource | implemented; **supported** |
| C5 | Metadata completeness varies by assay, region and period | UMDB resource | implemented; **supported** |
| C6 | Few centres produce most data | UMDB resource | implemented; concentrated but below threshold |
| C7 | Sequencing technologies diffuse unevenly | UMDB resource | implemented; **supported** for NovaSeq |
| C8 | The eligibility screen (and AI review) is accurate against expert labels | UMDB resource | implemented; awaiting labels |
| H1 | Habitat explains more community variation than geography (universal ecological structure) | UMDB-ECO | implemented; needs profiles |
| H2 | Functional convergence despite taxonomic divergence | UMDB-ECO | implemented; needs HUMAnN profiles |
| H3 | Diversity scales with population as D = D₀N^β | UMDB-SCALE | implemented; needs profiles |
| H4 | Communities are predictable in held-out cities | UMDB-AI | random-forest baseline implemented; foundation model planned |
| H5 | Human mobility shapes biogeography beyond distance and climate | UMDB-ECO | implemented (air routes); rail, commuting and trade planned |
| H6 | A city's habitats are connected non-randomly | UMDB-ECO | implemented (species level); strain tracking planned |
| H7 | Urban microbes show convergent molecular evolution | UMDB-EVO | roadmap: MAGs + HyPhy |
| H8 | Viral persistence is predictable | UMDB-VIR | geNomad contigs implemented; vOTU occupancy models planned |
| H9 | Most urban diversity is undiscovered, especially where sampling is sparse | UMDB-ECO | implemented (reference species); MAG/vOTU novelty planned |
| H10 | Seasonality follows local, hemisphere-aligned seasons | UMDB-ECO | implemented |
| H11 | Resistome ecology differs across connected habitats | UMDB-AMR | implemented (KMA/ResFinder) |
| H12 | Urbanization homogenizes communities | UMDB-ECO | roadmap: non-urban comparator cohort |
| H13 | Communities are resilient to disturbance | UMDB-ECO | roadmap: event-aligned series |
| H14 | Climate change redistributes urban microbes | UMDB-ECO | roadmap: ERA5-linked range models |

Each test's method and decision rule are in `pipeline/README.md` and in the docstring of its script.

## How this differs from existing work

| Established | UMDB's next question | Test |
| --- | --- | --- |
| A core urban microbiome with city-specific signatures (Danko et al., *Cell* 2021, MetaSUB) | Are there rules that hold across urban habitats, not only within transit systems? | H1, H2 |
| Homogenization of urban greenspace microbiomes (Delgado-Baquerizo et al., *Sci. Adv.* 2021) | Does convergence generalize across habitats and kingdoms? | H2, H12 |
| Global sewage resistomes and their socio-economic correlates (Hendriksen et al., *Nat. Commun.* 2019; Munk et al., *Nat. Commun.* 2022) | How does AMR ecology differ across connected urban habitats and over time? | H11, H6 |
| Global urban and peri-urban RNA viromes (*Nat. Commun.* 2026) | Can viral persistence and distribution be predicted? | H8, H4 |

## Papers

| Paper | Content | Pipeline output | Target (aspirational) |
| --- | --- | --- | --- |
| UMDB resource paper | The catalog, eligibility screen, validation, C0–C8 | Track A; C8 labels | *Nucleic Acids Research* (Database issue) |
| *The global blind spots of urban genomic surveillance* | C1, C2, C4, C6, C7 with population, development and city covariates | Track A | *Nature Cities* / *Lancet Planetary Health* |
| UMDB-ECO: *Universal principles of the global urban microbiome* | H1, H2, H3, H5, H6, H9, H4 (flagship; below) | Tracks B + C | *Nature Ecology & Evolution* |
| UMDB-SCALE | H3 at multiple taxonomic and functional levels with GHSL urban extents | Track C + GHSL | *Nature Cities* |
| UMDB-EVO | H7 | MAG branch (planned) | *Molecular Biology and Evolution* |
| UMDB-VIR | H8 | geNomad + occupancy (planned) | *Nature Communications* |
| UMDB-AMR | H11 longitudinally and across habitats | Track C | *Microbiome* |
| UMDB-AI | H4 foundation model with held-out-city benchmark | Track C | *Nature Methods* |

## Flagship paper: *Universal principles of the global urban microbiome*

Central claim to test: urban microbial ecosystems have conserved ecological and functional organizing principles that transcend geography and allow prediction in cities that have not been sampled.

| Figure | Finding to investigate | Pipeline output |
| --- | --- | --- |
| 1 | Global atlas of harmonized urban communities, with the sampling blind spots | cohort map; C1 |
| 2 | Hierarchical determinants of community structure: habitat vs city vs study | H1 |
| 3 | Diversity scaling laws | H3 |
| 4 | Functional convergence across cities | H2 |
| 5 | Connectivity: across habitats within cities, and between cities through mobility | H6, H5 |
| 6 | Prediction in unsampled cities | H4 |

**Cohort.** The feasibility audit (`catalog_findings/`) gives 9,221 balanced shotgun samples from 774 cities, 63 countries and 919 BioProjects, below the 10,000–20,000 first proposed. We prioritize balance across habitats, cities and independent studies over sample count. Non-wastewater habitats are the limiting factor, and the paper should say so.

**Safeguards.** One frozen catalog snapshot and cohort; strict eligibility tiers; depth standardization; study-level random effects or different-study comparisons; cluster bootstraps over cities; held-out cities and studies; decision rules fixed before analysis; and an external validation set of studies released after the snapshot date.

## External data

| Source | Variables | Status |
| --- | --- | --- |
| GeoNames | place and country population, city list for blind spots | integrated |
| World Bank WDI | GDP per capita, urban share, sanitation, R&D spending | integrated (fetched or pinned) |
| OpenFlights | air routes between cities | integrated (optional input) |
| Köppen–Geiger | climate class | integrated if a raster is supplied; latitude-band proxy otherwise |
| WorldPop, GHSL | population density, functional urban area, built-up extent | planned (H3) |
| ERA5 | temperature, precipitation, humidity at sample date and place | planned (H10, H14) |
| OpenAQ, OpenStreetMap, MODIS | air pollution, land use and infrastructure, vegetation | planned (H4) |
| WHO/UNICEF JMP | sub-national water and sanitation | planned (H11) |

Every annotation is versioned in `covariates_manifest.json` (source, retrieval date, SHA-256) and should match the spatial and temporal resolution of the samples.

## Limitations to fix before large-scale inference

* About half of all BioSamples, and 43% of urban ones, have no resolved place (C0). Recovering places from coordinates and project descriptions would enlarge every cohort.
* 180 samples have impossible collection dates. The pipeline excludes them; they should be reported to submitters or flagged in the catalog.
* The eligibility screen has not been benchmarked against expert labels (C8 provides the sheet).
* AI annotations are not yet populated (`ai_annotations` is empty), so AI-assisted curation cannot be evaluated yet.
* Reference-based profiling cannot see novel lineages; H9's completeness is an upper bound until the MAG and vOTU branches exist.
