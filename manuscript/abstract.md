# UMDB manuscript: title and abstract (draft)

> **Status: draft.** Counts are final for the completed deep backfill:
> `data/umdb.sqlite` built 2026-11-16T07:23:40Z, query profiles 2026.11.2,
> eligibility rules 2026.11.6. Refresh them before submission if the catalog
> has grown (see the end of this file).

## Title

**UMDB: a continuously updated, rule-screened catalog of public urban microbiome sequencing data**

Alternatives:

- *Finding the city in the Sequence Read Archive: a curated, sample-level index of urban environmental metagenomes*
- *UMDB: harmonizing and screening public sequencing metadata to map the urban microbiome*

## Abstract

**Background.** Public repositories hold hundreds of thousands of sequencing
datasets from wastewater, air, the built environment and other urban settings.
But they have no field identifying a sample as urban, and submitter metadata
are free text with inconsistent spellings. This makes urban microbiome studies
hard to find, compare and combine.

**Methods.** UMDB searches NCBI's Sequence Read Archive (SRA) with seven query
profiles (municipal wastewater, air and dust, built environment, urban
metagenomes, transit, urban soil and green space, and urban water and
drainage). It fetches run, BioSample and BioProject metadata for every match,
validates every sample location against ISO 3166 and the GeoNames gazetteer (checking reported coordinates where present), standardizes assay types, and applies a transparent, rule-based
eligibility screen to each BioSample. The screen checks that a sample is
environmental, then looks for evidence of urban origin, and assigns one of five
tiers (confirmed urban, probable urban, uncertain, non-urban,
non-environmental), recording the evidence behind each decision. In
validation, the queries still retrieved 98% of confirmed-urban and 95% of
probable-urban runs from an earlier catalog, while excluding 99.5% of
non-environmental runs. The pipeline is versioned and can be fully rebuilt from
its public inputs.

**Results.** The current release indexes 477,166 runs from 406,177
BioSamples in 11,366 BioProjects, released between 2008 and 2026, from
148 countries. 103,028 BioSamples are confirmed urban and 122,352
probable urban. Municipal wastewater dominates (157,187 urban samples),
followed by air and dust (30,917) and the built environment (18,773).
206,862 BioSamples are located to a validated city or town (122,728 verified, 84,134 probable)
and 176,347 report coordinates. Amplicon sequencing makes up the
largest share of experiments, followed by shotgun metagenomes and
metatranscriptomes.

**Use.** To show what the catalog supports, a companion Nextflow pipeline
(UMDB-SCIENCE) tests pre-specified hypotheses on a frozen snapshot. Public urban
sequencing is highly concentrated relative to population (concentration index
0.82, 95% CI 0.62–0.91): Africa contributes 0.06 times its population share,
and 224 of 573 cities of more than a million people have no urban sample within
50 km. Samples per person rise 1.27% for each 1% of GDP per capita (95% CI
0.86–1.69; 159 countries). Metadata completeness depends strongly on assay type.
The same pipeline selects a balanced cohort of 9,221 shotgun metagenomes from
774 cities for uniform reprocessing.

**Availability.** UMDB is distributed as a web explorer with filters by place,
assay, year and eligibility tier; a SQLite database; and versioned Parquet and
TSV releases with checksums. It is freely available under CC BY-NC-SA 4.0 at
https://aglucaci.github.io/UMDB/.

## Notes for revision

- **Research results.** The "Use" paragraph comes from Track A of `pipeline/`
  (`manuscript/catalog_findings/README.md`, same database build). Full
  programme: `manuscript/research_program.md`. Rerun with
  `nextflow run pipeline --fetch_worldbank` after any catalog refresh and update
  the numbers together.
- **Expert benchmark now has a sheet.** Hypothesis C8 writes a stratified,
  blinded labeling sheet of 500 BioSamples (`hypotheses/C8/labeling_sheet.tsv`);
  two raters plus `--labels` give catalog-weighted precision and recall for the
  eligibility screen. This would replace the recall caveat below.
- **Data-quality numbers for the Limitations section** (C0): 43.2% of urban
  BioSamples have no resolved place, 21.0% no collection date, and 180 samples have
  impossible collection dates (111 in the future, 47 after the data were released,
  22 before 1950).

- **Recall caveat.** The 98% / 95% figures measure retrieval against an earlier
  UMDB catalog, not an independent gold standard. Plan an external benchmark,
  such as a hand-labelled random sample of SRA records.
- **AI review is not mentioned** because no AI annotations exist in this
  catalog yet (`ai_annotations` is empty). `CITATION.cff` currently claims AI
  review; soften it or run the review before submission.
- **Limitations to state in the paper.** Discovery is keyword-based, so studies
  with sparse metadata are missed; the eligibility screen is rule-based and not
  yet benchmarked against expert labels; location validation depends on
  submitter text (places under 500 inhabitants, city districts and facility
  names are not resolved), and 3,515 samples have coordinates that contradict
  their stated place.
- **Location numbers** come from `sample_locations` (status verified/probable for
  places; country_status verified/probable for countries). Cite GeoNames.

## Refreshing the numbers

```bash
umdb db info
umdb db query "SELECT COUNT(*) FROM runs"
umdb db query "SELECT COUNT(*) FROM samples"
umdb db query "SELECT COUNT(*) FROM projects"
```

Tier, context, country and assay breakdowns come from the same tables
(`samples.eligibility_tier`, `samples.urban_context`, `experiments.assay`);
check column names with `umdb db shell` and `.schema`.
