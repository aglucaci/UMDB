# Catalog-level findings (Track A of the UMDB-SCIENCE pipeline)

> **Status: preliminary results for the resource paper.** Produced by `nextflow run pipeline --fetch_worldbank`
> on `data/umdb.sqlite` built 2026-11-16T14:00:32Z (schema 1.1.0, eligibility rules 2026.11.6, query profiles
> 2026.11.2, database SHA-256 `1129b8c312d8f9b8…`), World Bank WDI fetched 2026-11-19 (see
> `covariates_manifest.json`). Every number below comes from `hypotheses/<ID>/summary.json` of that run. Decision rules were fixed
> in each script before the run (see `pipeline/README.md`). Refresh them before submission if the catalog changes.

Unit: urban BioSamples (eligibility tier confirmed or probable; 225,380 samples) unless stated.

| ID | Hypothesis | Verdict under the pre-set rule | Key result |
| --- | --- | --- | --- |
| C0 | Data-quality audit | descriptive | 43.2% of urban samples lack a resolved place; 21.0% lack a collection date; 180 have impossible dates (111 in the future, 47 after release, 22 before 1950); 3,515 have coordinates contradicting their place. 106,803 urban samples have a place and a plausible full date. |
| C1 | Coverage is unequal relative to population | **supported** | Concentration index 0.82 (95% CI 0.62–0.91) across 162 countries of ≥1 M people; 49 of them have no urban sample. Five countries hold 73% of located urban samples. Africa holds 0.06× and South America 0.15× its population share; North America 5.6×, Europe 3.0×. 224 of 573 cities of ≥1 M people (457 M residents) have no urban sample within 50 km; 136 are in Asia and 48 in Africa. |
| C2 | Coverage tracks economic development | **supported** | Samples per person rise 1.27% per 1% GDP per capita (95% CI 0.86–1.69; negative binomial, 159 countries, adjusted for urban share); BioProjects 1.31% (1.06–1.57). Holds with R&D spending in the model (131 countries). |
| C3 | COVID-19 caused a structural break | not supported (sensitive) | With an immediate break at 2020-03 and data to 2024-12, neither the level (+30%, p = 0.46) nor the trend (p = 0.87) changed significantly. The wastewater share of dated urban samples rose from 67% to 84% (χ², p < 10⁻¹⁰⁰). The verdict depends on right truncation: ending at 2023-06 gives a −28% level drop (lockdown, p = 0.03) and a +55%/yr trend increase (p = 6 × 10⁻⁸); a lagged break at 2021-01 gives +139% (p = 0.007). New urban BioProjects slowed after 2020 (trend −37%/yr, p = 2 × 10⁻⁵). |
| C4 | Habitats are unevenly represented | **supported** | Pielou evenness 0.51. Wastewater holds 71% of urban samples, air and dust 14%, built environment 8%; transit (9 BioProjects, 1,988 samples) and surfaces (30 BioProjects) are least studied. Habitat mix of studies differs by continent (χ² = 277, p = 6 × 10⁻³⁹). |
| C5 | Metadata completeness varies by assay | **supported** | Assay terms are jointly significant for every outcome (Wald p ≤ 6 × 10⁻⁷; logistic models with BioProject-clustered SEs, 60,000 samples). Metatranscriptomes have a resolved place 12% of the time and coordinates 6%, against 63% and 58% for shotgun metagenomes; viral RNA runs almost never report coordinates (1%) or MIxS env_broad_scale (0.3%). |
| C6 | Few centres produce most data (top 1% > 50%) | not supported | The top 1% of 1,491 submitting centres (15) hold 43% and the top 10 hold 38% of urban samples (Gini 0.84). Highly concentrated, but below the pre-set 50% threshold. The largest submitter is a commercial wastewater surveillance company (13% of urban samples). |
| C7 | Technologies diffuse unevenly and lag tracks wealth | **supported** (NovaSeq only) | Across 66 countries with ≥50 urban samples, median adoption lag after global introduction is 4 years for Illumina NovaSeq (ρ with log GDP = −0.30, p = 0.03) and 3 years for long-read platforms (ρ = −0.21, p = 0.16); 18 countries have not yet published long-read urban data. |
| C8 | Eligibility-screen accuracy | awaiting labels | A stratified, blinded labeling sheet of 500 BioSamples (100 per tier) is written for two independent raters; precision, recall and κ follow from `--labels`. |

## Shotgun cohort feasibility (input to Track B)

16,083 urban BioSamples are eligible for the flagship analysis: shotgun metagenomes (WGS), Illumina, ≥1 Gbp, validated place, plausible date. Balancing habitats, cities and studies (≤ 50 per city-habitat cell, ≤ 300 per BioProject) gives **9,221 BioSamples** from 774 cities in 63 countries and 919 BioProjects. That is 98 Tbp raw, or 13.6 Tbp after capping each sample at 5 M read pairs. This is below the 10,000–20,000 originally proposed. The binding constraint is habitat balance: wastewater supplies 6,382 of the 9,221 samples, transit 154 and surfaces 202 (`cohort_feasibility.tsv`). Adding metatranscriptomes would mostly add wastewater; adding 16S would add habitats but needs a separate amplicon branch.

## What these results mean for the programme

* **The blind-spot paper is ready to write.** C1, C2 and C4 together show that public urban microbiome data describe the wealthy, northern, wastewater-sampled city. Working title: *The global blind spots of urban genomic surveillance*.
* **The flagship ecology analysis must model the imbalance, not ignore it.** Wastewater dominance (C4), study clustering (C6) and assay-dependent missingness (C5) are why the ecology tests use different-study pairs, city-level bootstraps, held-out cities and balanced cohorts.
* **C3 needs a better model, not a different threshold.** The pandemic signal is a lockdown dip followed by a wastewater-surveillance ramp-up with release lag. A two-break or distributed-lag model, fitted on release-lag–corrected counts, is the next version. The primary verdict stays as reported.
* **C8 is the most urgent gap** for the resource paper: the recall figures in the abstract are measured against an earlier catalog, not expert labels.

## Files

* `hypotheses_status.tsv`: the status table from this run
* `figures/`: one figure per test (`C1_coverage.png`, `C2_gdp_vs_samples.png`, `C3_its.png`, `C4_habitat_continent_residuals.png`, `C5_completeness.png`, `C6_centres.png`, `C7_adoption.png`)
* `cohort_feasibility.tsv` / `.json`: eligible vs selected samples by habitat and continent; selection criteria
* `covariates_manifest.json`: external data sources, retrieval dates and checksums
