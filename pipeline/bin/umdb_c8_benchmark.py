#!/usr/bin/env python3
"""C8 · Independent benchmark of the urban eligibility screen (and of AI review, when present).

Without --labels: draws a stratified random sample of BioSamples (--per-tier from each
eligibility tier, seeded) and writes ``labeling_sheet.tsv`` with links to NCBI and empty
columns for two independent expert labels. This is the gold standard the manuscript's
recall figures lack.

With --labels (the completed sheet: biosample, expert_urban in {yes, no, unsure},
expert_environmental in {yes, no}, optionally expert_context): reports, for "urban" =
confirmed + probable, precision, recall, F1 and Cohen's kappa, both in the sample and
re-weighted by the tier sampling fractions to estimate catalog-wide values, plus
per-tier agreement and inter-rater kappa when a second rater column is present.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import URBAN_TIERS, base_parser, parse_args, read_tsv, write_summary, write_table  # noqa: E402

TITLE = "Eligibility-screen benchmark"
QUESTION = "How accurate is the rule-based urban eligibility screen against expert labels?"
TIERS = ["confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental"]


def kappa(a, b):
    a, b = pd.Series(a).astype(str), pd.Series(b).astype(str)
    cats = sorted(set(a) | set(b))
    po = (a.values == b.values).mean()
    pe = sum((a == c).mean() * (b == c).mean() for c in cats)
    return float((po - pe) / (1 - pe)) if pe < 1 else float("nan")


def main():
    p = base_parser(__doc__)
    p.add_argument("--per-tier", type=int, default=100)
    a = parse_args(p)
    s = read_tsv(a.samples)
    sizes = s["urban_status"].value_counts()
    rng_seed = a.seed
    sheet = pd.concat([g.sample(min(a.per_tier, len(g)), random_state=rng_seed)
                       for _, g in s[s["urban_status"].isin(TIERS)].groupby("urban_status")])
    sheet = sheet[["biosample", "project", "urban_status", "urban_context", "collection_date_raw",
                   "country_name", "place_name", "primary_assay"]].copy()
    sheet["ncbi_url"] = "https://www.ncbi.nlm.nih.gov/biosample/" + sheet["biosample"]
    for col in ("expert_environmental", "expert_urban", "expert_context", "rater", "notes"):
        sheet[col] = ""
    sheet = sheet.sample(frac=1, random_state=rng_seed)  # blind raters to tier order
    files = [write_table(sheet.drop(columns=["urban_status", "urban_context"]), a.outdir, "labeling_sheet.tsv"),
             write_table(sheet[["biosample", "urban_status", "urban_context"]], a.outdir,
                         "labeling_key.tsv")]
    weights = (sizes / sheet["urban_status"].value_counts()).rename("weight")

    labels = read_tsv(a.labels)
    if labels is None:
        write_summary(a.outdir, a.id, TITLE, QUESTION, "inconclusive",
                      {"sheet_rows": len(sheet), "per_tier": a.per_tier, "tier_sizes": sizes.to_dict()},
                      interpretation=(f"Labeling sheet of {len(sheet)} BioSamples written. Label them "
                                      "(two raters, blind to the tier in labeling_key.tsv) and rerun with --labels."),
                      files=files)
        return
    lab = labels.merge(s[["biosample", "urban_status"]], on="biosample", how="inner")
    lab = lab[lab["expert_urban"].isin(["yes", "no"])]
    lab["pred"] = lab["urban_status"].isin(URBAN_TIERS)
    lab["truth"] = lab["expert_urban"].eq("yes")
    lab["w"] = lab["urban_status"].map(weights).fillna(1.0)

    def metrics(df, w=None):
        w = np.ones(len(df)) if w is None else df[w].to_numpy()
        tp = w[(df["pred"] & df["truth"]).to_numpy()].sum()
        fp = w[(df["pred"] & ~df["truth"]).to_numpy()].sum()
        fn = w[(~df["pred"] & df["truth"]).to_numpy()].sum()
        prec = tp / (tp + fp) if tp + fp else float("nan")
        rec = tp / (tp + fn) if tp + fn else float("nan")
        return {"precision": prec, "recall": rec, "f1": 2 * prec * rec / (prec + rec) if prec + rec else float("nan")}

    res = {"labelled": len(lab), "sample": metrics(lab), "catalog_weighted": metrics(lab, "w"),
           "kappa_screen_vs_expert": kappa(lab["pred"], lab["truth"]),
           "per_tier_expert_urban_rate": lab.groupby("urban_status")["truth"].mean().to_dict()}
    if "expert_urban_2" in labels.columns:
        two = labels[labels["expert_urban"].isin(["yes", "no"]) & labels["expert_urban_2"].isin(["yes", "no"])]
        res["inter_rater_kappa"] = kappa(two["expert_urban"], two["expert_urban_2"])
    files.append(write_table(lab, a.outdir, "benchmark_scored.tsv"))
    m = res["catalog_weighted"]
    write_summary(a.outdir, a.id, TITLE, QUESTION, "descriptive", res,
                  interpretation=(f"Catalog-weighted precision {m['precision']:.2f}, recall {m['recall']:.2f} "
                                  f"(n={len(lab)} labelled)."),
                  caveats=["Recall is measured only among discovered records; studies the keyword search never "
                           "found need a separate, query-independent sample of SRA."],
                  files=files)


if __name__ == "__main__":
    main()
