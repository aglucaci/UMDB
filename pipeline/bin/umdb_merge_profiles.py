#!/usr/bin/env python3
"""Merge per-sample profiler outputs into one feature x sample matrix (TSV).

Kinds:
  metaphlan   MetaPhlAn 4 profiles (clade, taxid, relative_abundance, ...): species rows (s__, no t__)
  sylph       sylph-tax ``taxprof`` MPA-style profiles (clade, relative_abundance, ...): species rows
  humann      HUMAnN tables regrouped to KO/pathways and renormalised (CPM): unstratified rows only
  kma         KMA .res files against ResFinder: genes with identity >= --min-id and coverage >= --min-cov,
              abundance = mean depth; alleles collapsed to gene (text before the first '_')

The sample name is the file name up to the first '.', which the pipeline sets to the BioSample.
"""
import argparse
import os
import sys

import pandas as pd


def sample_name(path):
    return os.path.basename(path).split(".")[0]


def read_mpa(path, col):
    out = {}
    for ln in open(path):
        if ln.startswith("#") or not ln.strip():
            continue
        f = ln.rstrip("\n").split("\t")
        clade = f[0]
        last = clade.split("|")[-1]
        if last.startswith("s__") and "|t__" not in clade and len(f) > col:
            try:
                out[last] = out.get(last, 0.0) + float(f[col])
            except ValueError:
                continue
    return pd.Series(out, dtype=float)


def read_humann(path):
    df = pd.read_csv(path, sep="\t", comment=None, header=0, index_col=0)
    df = df[~df.index.astype(str).str.contains(r"\|")]
    df = df[~df.index.astype(str).str.startswith(("UNMAPPED", "UNGROUPED", "UNINTEGRATED"))]
    return df.iloc[:, 0].astype(float)


def read_kma(path, min_id, min_cov):
    df = pd.read_csv(path, sep="\t")
    df.columns = [c.lstrip("#").strip() for c in df.columns]
    df = df[(df["Template_Identity"] >= min_id) & (df["Template_Coverage"] >= min_cov)]
    gene = df["Template"].astype(str).str.split("_").str[0]
    return df.groupby(gene)["Depth"].sum().astype(float)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kind", required=True, choices=["metaphlan", "sylph", "humann", "kma"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-id", type=float, default=90.0)
    ap.add_argument("--min-cov", type=float, default=60.0)
    ap.add_argument("files", nargs="+")
    a = ap.parse_args()
    cols = {}
    for f in a.files:
        if a.kind == "metaphlan":
            v = read_mpa(f, 2)
        elif a.kind == "sylph":
            v = read_mpa(f, 1)
        elif a.kind == "humann":
            v = read_humann(f)
        else:
            v = read_kma(f, a.min_id, a.min_cov)
        cols[sample_name(f)] = v
    m = pd.DataFrame(cols).fillna(0.0)
    m.index.name = "feature"
    m = m.loc[m.sum(axis=1) > 0].sort_index()
    m.to_csv(a.out, sep="\t")
    print(f"{a.kind}: {m.shape[0]} features x {m.shape[1]} samples", file=sys.stderr)


if __name__ == "__main__":
    main()
