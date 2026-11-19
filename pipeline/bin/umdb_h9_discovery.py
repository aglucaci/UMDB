#!/usr/bin/env python3
"""H9 · Is urban microbial diversity still largely undiscovered, and where is discovery cheapest?

From species incidence (presence in a sample): sample-based accumulation curves
(incidence rarefaction), Chao2 richness, completeness (S_obs / Chao2), Chao–Jost sample
coverage, and the expected number of new species in the next sample (Q1 / n), for the
whole cohort and per habitat and continent.

Under-sampled regions are those with fewer urban samples per person in the catalog
(C1); at equal effort (rarefied to the same number of samples) they are predicted to
yield more species per sample and have lower completeness.

Decision rule: "supported" when global completeness < 0.9 and, at equal effort, the
mean undiscovered fraction is higher in under-sampled continents than in
well-sampled ones.

Reference-based profiles only see catalogued species; the same script takes vOTU or
MAG-cluster incidence matrices (--species) for true novelty once those are built.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (CONTINENTS, base_parser, chao2, incidence_rarefaction, load_ecology, parse_args, plt,  # noqa: E402
                         read_tsv, skip, write_summary, write_table)

TITLE = "Undiscovered urban microbial diversity"
QUESTION = "Are discovery curves unsaturated, with the largest gains in under-sampled regions?"


def main():
    p = base_parser(__doc__)
    p.add_argument("--min-units", type=int, default=10)
    a = parse_args(p)
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles.")
    inc = (X.loc[meta.index] > 0).astype(np.int8)
    meta["continent_name"] = meta["continent"].map(lambda k: CONTINENTS.get(k, k))
    # catalog-wide sampling intensity per continent (samples per million people)
    cov = read_tsv(a.country_covariates)
    allsamp = read_tsv(a.samples)
    allsamp = allsamp[allsamp["urban_status"].isin(["confirmed_urban", "probable_urban"])]
    intensity = None
    if cov is not None:
        pop = cov.groupby("continent")["population"].sum()
        n = allsamp.groupby("continent").size()
        intensity = (n / pop * 1e6).rename(lambda k: CONTINENTS.get(k, k))

    strata = [("all", "all", meta.index)]
    for col in ("urban_context", "continent_name"):
        for v, g in meta.groupby(col):
            strata.append((col, v, g.index))
    k_eq = min(len(idx) for _, _, idx in strata if len(idx) >= a.min_units) if len(meta) >= a.min_units else 0
    rows, curves = [], []
    for kind, val, idx in strata:
        if len(idx) < a.min_units:
            continue
        m = inc.loc[idx].to_numpy()
        c = chao2(m)
        s_eq = incidence_rarefaction(m, k_eq)
        sub_inc = m
        rows.append({"stratum": kind, "value": val, **c, "k_equal_effort": k_eq, "S_at_equal_effort": s_eq,
                     "undiscovered_fraction": 1 - c["completeness"],
                     "samples_per_million_catalog": None if intensity is None or kind != "continent_name"
                     else intensity.get(val)})
        for k in np.unique(np.linspace(1, len(idx), 25).astype(int)):
            curves.append({"stratum": kind, "value": val, "samples": int(k),
                           "expected_species": incidence_rarefaction(sub_inc, int(k))})
    tab = pd.DataFrame(rows)
    glob = tab[tab["stratum"] == "all"].iloc[0]
    cont = tab[tab["stratum"] == "continent_name"].dropna(subset=["samples_per_million_catalog"])
    comparison = None
    if len(cont) >= 2:
        med = cont["samples_per_million_catalog"].median()
        under = cont[cont["samples_per_million_catalog"] < med]
        well = cont[cont["samples_per_million_catalog"] >= med]
        if len(under) and len(well):
            comparison = {"under_sampled": list(under["value"]), "well_sampled": list(well["value"]),
                          "undiscovered_under": float(under["undiscovered_fraction"].mean()),
                          "undiscovered_well": float(well["undiscovered_fraction"].mean()),
                          "S_equal_effort_under": float(under["S_at_equal_effort"].mean()),
                          "S_equal_effort_well": float(well["S_at_equal_effort"].mean())}
    files = [write_table(tab, a.outdir, "discovery_by_stratum.tsv"),
             write_table(pd.DataFrame(curves), a.outdir, "accumulation_curves.tsv")]
    pyplot = plt()
    if pyplot:
        cv = pd.DataFrame(curves)
        fig, ax = pyplot.subplots(1, 2, figsize=(9, 3.4), sharey=True)
        for i, col in enumerate(("urban_context", "continent_name")):
            for v, g in cv[cv["stratum"] == col].groupby("value"):
                ax[i].plot(g["samples"], g["expected_species"], label=str(v).replace("_", " "))
            ax[i].set(xlabel="Samples", title=col.replace("_name", "").replace("urban_context", "habitat"))
            ax[i].legend(frameon=False, fontsize=6)
        ax[0].set_ylabel("Expected species")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "accumulation.png"))
        files.append("accumulation.png")
    ok = glob["completeness"] < 0.9 and comparison is not None and \
        comparison["undiscovered_under"] > comparison["undiscovered_well"]
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else (
        "inconclusive" if comparison is None else "not_supported"),
                  {"global": glob.to_dict(), "continent_comparison": comparison, "k_equal_effort": k_eq},
                  interpretation=(f"Observed {int(glob['s_obs'])} of an estimated {glob['chao2']:.0f} species "
                                  f"(completeness {glob['completeness']:.2f}); the next sample is expected to add "
                                  f"{glob['new_species_next_unit']:.2f} new species."),
                  caveats=["Reference-based detection: novel lineages are invisible until MAG/vOTU clustering is "
                           "added, so completeness here is an upper bound.",
                           "Chao2 is a lower-bound estimator of total richness."],
                  files=files)


if __name__ == "__main__":
    main()
