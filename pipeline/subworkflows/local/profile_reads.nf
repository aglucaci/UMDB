// Fetch, QC, depth-standardise and profile the reads of the selected shotgun cohort,
// then merge per-sample outputs into feature x BioSample matrices.

include { SRA_FASTQ; FASTP; SUBSAMPLE; SYLPH_PROFILE; METAPHLAN; HUMANN; KMA_RESFINDER;
          MEGAHIT; GENOMAD } from '../../modules/local/reads'
include { MERGE_PROFILES as MERGE_SPECIES; MERGE_PROFILES as MERGE_FUNCTIONS;
          MERGE_PROFILES as MERGE_ARG } from '../../modules/local/reads'

workflow PROFILE_READS {
    take:
    cohort_runs   // path: cohort_runs.tsv (run, biosample, ...)

    main:
    runs = cohort_runs
        .splitCsv(header: true, sep: '\t')
        .map { row -> tuple(row.biosample, row.run) }
        .take(params.max_cohort_samples > 0 ? params.max_cohort_samples : -1)

    SRA_FASTQ(runs)
    per_sample = SRA_FASTQ.out.reads.groupTuple().map { bs, files -> tuple(bs, files.flatten()) }
    FASTP(per_sample)
    SUBSAMPLE(FASTP.out.reads)
    reads = SUBSAMPLE.out.reads

    // ---- taxonomy
    if (params.taxonomy_tool == 'sylph') {
        SYLPH_PROFILE(reads, file(params.sylph_db, checkIfExists: true))
        tax = SYLPH_PROFILE.out.profile
    } else {
        METAPHLAN(reads, file(params.metaphlan_db, checkIfExists: true))
        tax = METAPHLAN.out.profile
    }
    species = MERGE_SPECIES(params.taxonomy_tool, tax.map { it[1] }.collect()).matrix

    // ---- function (HUMAnN; reuses the MetaPhlAn profile when available)
    functions = Channel.value(file("${projectDir}/assets/NO_FILE_functions"))
    if (params.functional) {
        no_tax = file("${projectDir}/assets/NO_FILE_species")
        hin = params.taxonomy_tool == 'metaphlan' ? reads.join(tax) : reads.map { bs, r -> tuple(bs, r, no_tax) }
        HUMANN(hin, file(params.humann_nucleotide_db, checkIfExists: true),
               file(params.humann_protein_db, checkIfExists: true))
        functions = MERGE_FUNCTIONS('humann', HUMANN.out.profile.map { it[1] }.collect()).matrix
    }

    // ---- antimicrobial resistance genes
    arg = Channel.value(file("${projectDir}/assets/NO_FILE_arg"))
    if (params.amr) {
        KMA_RESFINDER(reads, file(params.resfinder_db, checkIfExists: true))
        arg = MERGE_ARG('kma', KMA_RESFINDER.out.profile.map { it[1] }.collect()).matrix
    }

    // ---- viral contigs (inputs for H8, viral persistence; occupancy model on the roadmap)
    if (params.viral) {
        MEGAHIT(reads)
        GENOMAD(MEGAHIT.out.contigs, file(params.genomad_db, checkIfExists: true))
    }

    emit:
    species
    functions
    arg
    depth = SUBSAMPLE.out.depth
    qc    = FASTP.out.json
}
