// Read-level steps for the shotgun cohort. Tool versions are pinned in the conda
// directives; with -profile docker, Wave builds containers from the same specs.

process SRA_FASTQ {
    tag "${run}"
    label 'process_low'
    label 'network'
    conda 'bioconda::sra-tools=3.1.1 conda-forge::pigz'
    maxForks params.max_downloads

    input:
    tuple val(biosample), val(run)

    output:
    tuple val(biosample), path("${run}*.fastq.gz"), emit: reads

    script:
    // fastq-dump -X streams only the first N spots, so a depth cap never downloads a whole run.
    // N = 0 downloads everything with fasterq-dump.
    if (params.max_spots > 0)
        """
        fastq-dump --split-3 --skip-technical --gzip -X ${params.max_spots} ${run}
        """
    else
        """
        fasterq-dump --split-3 --skip-technical --threads ${task.cpus} ${run}
        pigz -p ${task.cpus} ${run}*.fastq
        """
}

process FASTP {
    tag "${biosample}"
    label 'process_medium'
    conda 'bioconda::fastp=0.23.4'
    publishDir "${params.outdir}/reads/qc", mode: params.publish_dir_mode, pattern: '*.json'

    input:
    tuple val(biosample), path(fastqs, stageAs: 'in/*')

    output:
    tuple val(biosample), path("${biosample}_R*.fastq.gz"), emit: reads
    path "${biosample}.fastp.json",                          emit: json

    script:
    """
    # one BioSample may have several runs: concatenate mates, fall back to single-end
    r1=\$(ls in/*_1.fastq.gz 2>/dev/null || true)
    if [ -n "\$r1" ]; then
        cat in/*_1.fastq.gz > raw_1.fq.gz
        cat in/*_2.fastq.gz > raw_2.fq.gz
        fastp -i raw_1.fq.gz -I raw_2.fq.gz -o ${biosample}_R1.fastq.gz -O ${biosample}_R2.fastq.gz \\
            --detect_adapter_for_pe --length_required 50 --thread ${task.cpus} \\
            --json ${biosample}.fastp.json --html /dev/null
    else
        cat in/*.fastq.gz > raw.fq.gz
        fastp -i raw.fq.gz -o ${biosample}_R1.fastq.gz --length_required 50 --thread ${task.cpus} \\
            --json ${biosample}.fastp.json --html /dev/null
    fi
    """
}

// Standardise sequencing depth: exactly --subsample_reads read pairs per sample (same
// seed for both mates keeps pairs). Samples with fewer reads are dropped unless
// --drop_below_depth false, because depth drives detected richness.
process SUBSAMPLE {
    tag "${biosample}"
    label 'process_low'
    conda 'bioconda::seqtk=1.4'

    input:
    tuple val(biosample), path(reads)

    output:
    tuple val(biosample), path("${biosample}.sub_R*.fastq.gz"), emit: reads, optional: true
    tuple val(biosample), path("${biosample}.depth.txt"),       emit: depth

    script:
    def r1 = reads instanceof List ? reads[0] : reads
    """
    n=\$(( \$(zcat ${r1} | wc -l) / 4 ))
    echo -e "${biosample}\\t\$n" > ${biosample}.depth.txt
    if [ "\$n" -lt ${params.subsample_reads} ] && [ "${params.drop_below_depth}" = "true" ]; then
        exit 0
    fi
    i=1
    for f in ${reads}; do
        seqtk sample -s ${params.seed} \$f ${params.subsample_reads} | gzip -c > ${biosample}.sub_R\${i}.fastq.gz
        i=\$((i+1))
    done
    """
}

process SYLPH_PROFILE {
    tag "${biosample}"
    label 'process_medium'
    conda 'bioconda::sylph=0.6.1 bioconda::sylph-tax=1.2.0'
    publishDir "${params.outdir}/profiles/sylph", mode: params.publish_dir_mode

    input:
    tuple val(biosample), path(reads)
    path db

    output:
    tuple val(biosample), path("${biosample}.sylphmpa"), emit: profile

    script:
    def sk = reads instanceof List && reads.size() == 2 ? "-1 ${reads[0]} -2 ${reads[1]}" : "-r ${reads}"
    """
    sylph sketch ${sk} -d sketch -t ${task.cpus}
    sylph profile ${db} sketch/*.sylsp -t ${task.cpus} > ${biosample}.sylph.tsv
    # GTDB taxonomy for each genome hit (sylph-tax; metadata dir set by --sylph_tax_dir)
    export SYLPH_TAXONOMY_CONFIG=${params.sylph_tax_dir}
    sylph-tax taxprof ${biosample}.sylph.tsv -t ${params.sylph_taxonomy} -o tax_
    mv tax_*.sylphmpa ${biosample}.sylphmpa
    """
}

process METAPHLAN {
    tag "${biosample}"
    label 'process_high'
    conda 'bioconda::metaphlan=4.1.1'
    publishDir "${params.outdir}/profiles/metaphlan", mode: params.publish_dir_mode

    input:
    tuple val(biosample), path(reads)
    path db

    output:
    tuple val(biosample), path("${biosample}.metaphlan.txt"), emit: profile

    script:
    def inp = reads instanceof List ? reads.join(',') : reads
    """
    metaphlan ${inp} --input_type fastq --bowtie2db ${db} --index ${params.metaphlan_index} \\
        --nproc ${task.cpus} --bowtie2out ${biosample}.bt2.bz2 -o ${biosample}.metaphlan.txt
    """
}

process HUMANN {
    tag "${biosample}"
    label 'process_high'
    conda 'bioconda::humann=3.9'
    publishDir "${params.outdir}/profiles/humann", mode: params.publish_dir_mode

    input:
    tuple val(biosample), path(reads), path(taxprofile)
    path nucleotide_db
    path protein_db

    output:
    tuple val(biosample), path("${biosample}.ko_cpm.tsv"), emit: profile

    script:
    def tax = taxprofile.name.startsWith('NO_FILE') ? '' : "--taxonomic-profile ${taxprofile}"
    """
    cat ${reads} > ${biosample}.fastq.gz
    humann --input ${biosample}.fastq.gz --output out --threads ${task.cpus} ${tax} \\
        --nucleotide-database ${nucleotide_db} --protein-database ${protein_db} --remove-temp-output
    humann_regroup_table -i out/${biosample}_genefamilies.tsv -g uniref90_ko -o ko.tsv
    humann_renorm_table -i ko.tsv -u cpm -o ${biosample}.ko_cpm.tsv
    """
}

process KMA_RESFINDER {
    tag "${biosample}"
    label 'process_medium'
    conda 'bioconda::kma=1.4.15'
    publishDir "${params.outdir}/profiles/resfinder", mode: params.publish_dir_mode

    input:
    tuple val(biosample), path(reads)
    path db, stageAs: 'resfinder_db/*'

    output:
    tuple val(biosample), path("${biosample}.res"), emit: profile

    script:
    // Settings follow the global sewage resistome studies (Hendriksen 2019, Munk 2022)
    def inp = reads instanceof List && reads.size() == 2 ? "-ipe ${reads[0]} ${reads[1]}" : "-i ${reads}"
    def prefix = params.resfinder_prefix
    """
    kma ${inp} -o ${biosample} -t_db resfinder_db/${prefix} -mem_mode -ef -1t1 -nf -t ${task.cpus}
    """
}

process MEGAHIT {
    tag "${biosample}"
    label 'process_high'
    conda 'bioconda::megahit=1.2.9'

    input:
    tuple val(biosample), path(reads)

    output:
    tuple val(biosample), path("${biosample}.contigs.fa.gz"), emit: contigs

    script:
    def inp = reads instanceof List && reads.size() == 2 ? "-1 ${reads[0]} -2 ${reads[1]}" : "-r ${reads}"
    """
    megahit ${inp} -t ${task.cpus} --min-contig-len 1000 -o asm
    gzip -c asm/final.contigs.fa > ${biosample}.contigs.fa.gz
    """
}

process GENOMAD {
    tag "${biosample}"
    label 'process_high'
    conda 'bioconda::genomad=1.8.0'
    publishDir "${params.outdir}/viral/genomad", mode: params.publish_dir_mode

    input:
    tuple val(biosample), path(contigs)
    path db

    output:
    tuple val(biosample), path("${biosample}_virus_summary.tsv"), emit: summary
    tuple val(biosample), path("${biosample}_virus.fna.gz"),      emit: fasta

    script:
    """
    genomad end-to-end --cleanup --threads ${task.cpus} ${contigs} out ${db}
    cp out/*_summary/*_virus_summary.tsv ${biosample}_virus_summary.tsv
    gzip -c out/*_summary/*_virus.fna > ${biosample}_virus.fna.gz
    """
}

process MERGE_PROFILES {
    tag "${kind}"
    label 'analysis'
    label 'process_low'
    publishDir "${params.outdir}/profiles", mode: params.publish_dir_mode

    input:
    val kind
    path files, stageAs: 'in/*'

    output:
    path "${kind}_matrix.tsv", emit: matrix

    script:
    """
    umdb_merge_profiles.py --kind ${kind} --out ${kind}_matrix.tsv in/*
    """
}
