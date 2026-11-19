// Catalog-level steps: freeze the database, attach covariates, choose the cohort,
// run one hypothesis test, and assemble the report. All use envs/analysis.yml.

process SNAPSHOT {
    label 'analysis'
    label 'process_medium'
    publishDir "${params.outdir}/snapshot", mode: params.publish_dir_mode

    input:
    path db

    output:
    path "samples.tsv.gz", emit: samples
    path "runs.tsv.gz",    emit: runs
    path "snapshot.json",  emit: json
    path "build_date.txt", emit: build_date

    script:
    """
    umdb_snapshot.py --db ${db} --outdir . ${params.hash_db ? '' : '--no-hash'}
    """
}

process COVARIATES {
    label 'analysis'
    label 'process_low'
    publishDir "${params.outdir}/covariates", mode: params.publish_dir_mode

    input:
    path samples
    path geonames, stageAs: 'geonames/*'
    path worldbank
    path openflights, stageAs: 'openflights/*'

    output:
    path "country_covariates.tsv",   emit: country
    path "city_covariates.tsv",      emit: city
    path "cities.tsv",               emit: cities
    path "city_routes.tsv",          emit: routes
    path "covariates_manifest.json", emit: manifest
    path "worldbank_wdi.tsv",        emit: worldbank, optional: true

    script:
    def wb  = worldbank.name.startsWith('NO_FILE') ? (params.fetch_worldbank ? '--fetch-worldbank' : '') : "--worldbank ${worldbank}"
    def of  = params.openflights_dir ? '--openflights-dir openflights' : ''
    def kg  = params.koppen_raster ? "--koppen-raster ${params.koppen_raster}" : ''
    """
    umdb_covariates.py --samples ${samples} --geonames-dir geonames ${wb} ${of} ${kg} \\
        --wb-years ${params.worldbank_years} --min-city-pop ${params.min_city_pop} --outdir .
    [ -f city_routes.tsv ] || : > city_routes.tsv
    """
}

process SELECT_COHORT {
    label 'analysis'
    label 'process_low'
    publishDir "${params.outdir}/cohort", mode: params.publish_dir_mode

    input:
    path samples
    path runs

    output:
    path "cohort_samples.tsv", emit: samples
    path "cohort_runs.tsv",    emit: runs
    path "feasibility.tsv",    emit: table
    path "feasibility.json",   emit: json

    script:
    """
    umdb_select_cohort.py --samples ${samples} --runs ${runs} --outdir . \\
        --assays ${params.cohort_assays} --target ${params.cohort_target} \\
        --min-bases ${params.cohort_min_bases} --max-per-cell ${params.cohort_max_per_cell} \\
        --max-per-project ${params.cohort_max_per_project} \\
        --subsample-bases ${params.subsample_reads * 300} --seed ${params.seed}
    """
}

// One hypothesis test. Every script takes the same arguments and writes <id>/summary.json.
process HYPOTHESIS {
    tag "${id}"
    label 'analysis'
    label 'process_medium'
    publishDir "${params.outdir}/hypotheses", mode: params.publish_dir_mode

    input:
    tuple val(id), val(script_name)
    path snapshot     // samples.tsv.gz, runs.tsv.gz
    path covariates   // country_covariates.tsv, city_covariates.tsv, cities.tsv, city_routes.tsv
    path species
    path functions
    path arg
    path labels

    output:
    path "${id}",              emit: dir
    path "${id}/summary.json", emit: summary

    script:
    """
    ${script_name} --id ${id} --outdir ${id} \\
        --samples samples.tsv.gz --runs runs.tsv.gz \\
        --country-covariates country_covariates.tsv --city-covariates city_covariates.tsv \\
        --cities cities.tsv --routes city_routes.tsv \\
        --species ${species} --functions ${functions} --arg ${arg} --labels ${labels} \\
        --permutations ${params.permutations} --max-samples ${params.max_samples} --seed ${params.seed}
    """
}

process REPORT {
    label 'analysis'
    label 'process_low'
    publishDir "${params.outdir}", mode: params.publish_dir_mode

    input:
    path registry
    path snapshot_json
    path cohort_json
    path summaries, stageAs: 'summaries/*/summary.json'

    output:
    path "report.md",              emit: md
    path "report.json",            emit: json
    path "hypotheses_status.tsv",  emit: tsv

    script:
    def cohort = cohort_json.name.startsWith('NO_FILE') ? '' : "--cohort ${cohort_json}"
    """
    umdb_report.py --registry ${registry} --snapshot ${snapshot_json} ${cohort} --summaries summaries --outdir .
    """
}
