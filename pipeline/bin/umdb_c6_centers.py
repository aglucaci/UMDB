#!/usr/bin/env python3
"""C6 · Do relatively few research centres produce most urban sequencing data?

Uses the SRA submitting centre (``center_name``, normalised for case and punctuation)
of each urban BioSample. Reports the share held by the top 10 and top 1% of centres,
Gini and Herfindahl–Hirschman indices for samples and for BioProjects, the trend of
concentration by release year, and how many countries each centre samples.

Decision rule: "supported" when the top 1% of centres hold > 50% of urban samples.
"""
import os
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import base_parser, gini, hhi, load_samples, parse_args, plt, write_summary, write_table  # noqa: E402

TITLE = "Concentration of urban sequencing in few centres"
QUESTION = "Do relatively few research centres produce most public urban sequencing data?"


def norm(name):
    if not isinstance(name, str):
        return None
    n = re.sub(r"[^A-Z0-9 ]+", " ", name.upper())
    n = re.sub(r"\b(THE|OF|AND)\b", " ", n)
    return re.sub(r"\s+", " ", n).strip() or None


def main():
    a = parse_args(base_parser(__doc__))
    s = load_samples(a.samples)
    s["centre"] = s["center"].map(norm)
    s = s.dropna(subset=["centre"])
    c = pd.DataFrame({"samples": s.groupby("centre").size(),
                      "bioprojects": s.groupby("centre")["project"].nunique(),
                      "countries_sampled": s.groupby("centre")["country_iso2"].nunique(),
                      "habitats": s.groupby("centre")["urban_context"].nunique()}).sort_values("samples",
                                                                                                ascending=False)
    k1 = max(1, int(np.ceil(0.01 * len(c))))
    top1 = c["samples"].iloc[:k1].sum() / c["samples"].sum()
    top10 = c["samples"].iloc[:10].sum() / c["samples"].sum()
    by_year = []
    min_year = min(200, max(20, len(s) // 20))
    for y, g in s.groupby("first_release_year"):
        if len(g) < min_year:
            continue
        cnt = g["centre"].value_counts()
        by_year.append({"year": int(y), "samples": len(g), "centres": len(cnt), "hhi": hhi(cnt),
                        "gini": gini(cnt), "top10_share": cnt.iloc[:10].sum() / cnt.sum()})
    by_year = pd.DataFrame(by_year, columns=["year", "samples", "centres", "hhi", "gini", "top10_share"])
    from scipy.stats import spearmanr
    trend = spearmanr(by_year["year"], by_year["hhi"]) if len(by_year) > 3 else None

    files = [write_table(c.reset_index().rename(columns={"index": "centre"}), a.outdir, "centres.tsv"),
             write_table(by_year, a.outdir, "concentration_by_year.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(1, 2, figsize=(8.5, 3.2))
        cum = np.cumsum(c["samples"].to_numpy()) / c["samples"].sum()
        ax[0].plot(np.arange(1, len(cum) + 1) / len(cum), cum, color="#2a6f97")
        ax[0].set(xscale="log", xlabel="Fraction of centres (ranked)", ylabel="Cumulative share of samples")
        ax[1].plot(by_year["year"], by_year["top10_share"], marker="o", ms=3, color="#2a6f97")
        ax[1].set(xlabel="Release year", ylabel="Top-10 centre share", ylim=(0, 1))
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "centres.png"))
        files.append("centres.png")
    res = {"centres": len(c), "top1pct_centres": k1, "top1pct_share": top1, "top10_share": top10,
           "gini_samples": gini(c["samples"]), "gini_bioprojects": gini(c["bioprojects"]),
           "hhi_samples": hhi(c["samples"]), "top10": c.head(10)["samples"].to_dict(),
           "hhi_trend_spearman": None if trend is None else {"rho": trend.statistic, "p": trend.pvalue}}
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if top1 > 0.5 else "not_supported", res,
                  interpretation=(f"The top 1% of {len(c):,} submitting centres ({k1}) hold {100 * top1:.0f}% of urban "
                                  f"samples; the top 10 hold {100 * top10:.0f}%."),
                  caveats=["center_name is the submitter, not the collecting or funding institution; "
                           "national archives and sequencing cores submit on behalf of many groups.",
                           "Spelling variants of one institution are merged only by case and punctuation."],
                  files=files)


if __name__ == "__main__":
    main()
