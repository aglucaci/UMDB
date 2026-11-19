#!/usr/bin/env nextflow
/*
 * UMDB-SCIENCE: hypothesis tests on the Urban Microbiome Database.
 *
 *   Track A  catalog    tests that need only metadata (C0–C8), from data/umdb.sqlite
 *   Track B  reads      selects a balanced shotgun cohort; fetches, QCs, depth-standardises
 *                       and profiles it (species, KO functions, ARGs, viral contigs)
 *   Track C  ecology    tests on the profiles (H1–H6, H9–H11)
 *
 * Every test is a script in bin/ registered in assets/hypotheses.csv; see README.md.
 *
 *   nextflow run pipeline -profile test,conda                     # synthetic fixture, ~2 min
 *   nextflow run pipeline -profile conda --fetch_worldbank        # catalog tests + cohort audit
 *   nextflow run pipeline -profile slurm,conda --run_reads --sylph_db gtdb-r220.syldb ...
 */
nextflow.enable.dsl = 2

include { SNAPSHOT; COVARIATES; SELECT_COHORT; HYPOTHESIS; REPORT } from './modules/local/catalog'
include { PROFILE_READS } from './subworkflows/local/profile_reads'

def noFile(name) { file("${projectDir}/assets/NO_FILE_${name}") }
def optFile(p, name) { p ? file(p, checkIfExists: true) : noFile(name) }

workflow {
    log.info """
    U M D B - S C I E N C E
    database      : ${params.db}
    hypotheses    : ${params.hypotheses}
    reads track   : ${params.run_reads ? params.taxonomy_tool : 'off'}
    profiles      : ${params.species ?: (params.run_reads ? 'from reads' : 'none (ecology tests skipped)')}
    outdir        : ${params.outdir}
    """.stripIndent()

    // ---- Track A: snapshot, covariates, cohort
    SNAPSHOT(file(params.db, checkIfExists: true))
    COVARIATES(
        SNAPSHOT.out.samples,
        files("${params.geonames_dir}/{countryInfo.txt,cities500.zip}", checkIfExists: true),
        optFile(params.worldbank, 'worldbank'),
        params.openflights_dir ? files("${params.openflights_dir}/{airports,routes}.dat", checkIfExists: true)
                               : noFile('openflights')
    )
    snapshot   = SNAPSHOT.out.samples.mix(SNAPSHOT.out.runs).collect()
    covariates = COVARIATES.out.country.mix(COVARIATES.out.city, COVARIATES.out.cities, COVARIATES.out.routes).collect()

    cohort_json = Channel.value(noFile('cohort'))
    cohort_runs = params.cohort ? Channel.value(file(params.cohort, checkIfExists: true)) : null
    if (params.select_cohort && !params.cohort) {
        SELECT_COHORT(SNAPSHOT.out.samples, SNAPSHOT.out.runs)
        cohort_runs = SELECT_COHORT.out.runs
        cohort_json = SELECT_COHORT.out.json
    }

    // ---- Track B: profiles from reads, or matrices supplied with --species/--functions/--arg
    species   = Channel.value(optFile(params.species, 'species'))
    functions = Channel.value(optFile(params.functions, 'functions'))
    arg       = Channel.value(optFile(params.arg, 'arg'))
    if (params.run_reads) {
        if (cohort_runs == null) error "--run_reads needs a cohort: keep --select_cohort or pass --cohort"
        PROFILE_READS(cohort_runs)
        species = PROFILE_READS.out.species
        if (params.functional) functions = PROFILE_READS.out.functions
        if (params.amr)        arg       = PROFILE_READS.out.arg
    }

    // ---- which hypotheses can run with the inputs we have
    have = [
        species  : params.species || params.run_reads,
        functions: params.functions || (params.run_reads && params.functional),
        arg      : params.arg || (params.run_reads && params.amr),
        routes   : params.openflights_dir as boolean,
        worldbank: (params.worldbank || params.fetch_worldbank) as boolean,
    ]
    wanted = params.hypotheses == 'all' ? null : params.hypotheses.tokenize(',')*.trim()
    registry = file("${projectDir}/assets/hypotheses.csv")
    tests = Channel.fromPath(registry)
        .splitCsv(header: true)
        .filter { r -> r.track in ['catalog', 'ecology'] && r.script }
        .filter { r -> !wanted || r.id in wanted }
        .filter { r -> params.run_catalog || r.track != 'catalog' }
        .filter { r -> r.requires.tokenize(';').every { have[it] } }
        .map { r -> tuple(r.id, r.script) }

    HYPOTHESIS(tests, snapshot, covariates, species, functions, arg, optFile(params.labels, 'labels'))

    REPORT(registry, SNAPSHOT.out.json, cohort_json, HYPOTHESIS.out.summary.collect().ifEmpty([]))
}
