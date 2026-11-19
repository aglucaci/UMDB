#!/usr/bin/env python3
"""H2 · Do urban microbiomes converge functionally despite taxonomic divergence?

For samples with both species and function (KO) profiles, computes Bray–Curtis
dissimilarity on each and, using pairs from different BioProjects only:

  relative dispersion R = mean d(same habitat, different city) / mean d(different city)

R < 1 means samples of a habitat resemble each other across cities more than samples
from different cities in general. Functional convergence predicts R_function < R_species
(habitats are more alike across cities in what they can do than in who is there).
Reports R per habitat and overall, the overall difference with a 95% cluster bootstrap
over cities, and the Mantel correlation between taxonomic and functional distances
(functional redundancy weakens it).

Decision rule: "supported" when R_species - R_function > 0 with the bootstrap
interval above 0.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, bray_curtis, city_bootstrap_weights, load_ecology, mantel, pair_frame,  # noqa: E402
                         parse_args, plt, read_matrix, skip, stratified_subsample, weighted_pair_mean,
                         write_summary, write_table)

TITLE = "Functional convergence across cities"
QUESTION = "Are habitats more alike across cities in function than in taxonomy?"


def rel_disp(pairs, w=None, habitat=None):
    m = ~pairs["same_project"].to_numpy() & ~pairs["same_city"].to_numpy()
    sh = m & pairs["same_habitat"].to_numpy()
    if habitat is not None:
        sh = sh & (pairs["habitat_i"].to_numpy() == habitat)
    return weighted_pair_mean(pairs, sh, w) / weighted_pair_mean(pairs, m, w)


def main():
    a = parse_args(base_parser(__doc__))
    X, meta = load_ecology(a)
    F = read_matrix(a.functions)
    if X is None or F is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "Needs both species and function profiles.")
    common = meta.index.intersection(F.index)
    if len(common) < 20:
        skip(a.outdir, a.id, TITLE, QUESTION, "Fewer than 20 samples have both profiles.")
    meta = stratified_subsample(meta.loc[common], ["urban_context", "place_id"], a.max_samples, a.seed)
    Dt = bray_curtis(X.loc[meta.index].to_numpy())
    Df = bray_curtis(F.loc[meta.index].to_numpy())
    pt, pf = pair_frame(meta, Dt), pair_frame(meta, Df)
    r_t, r_f = rel_disp(pt), rel_disp(pf)
    diff = r_t - r_f
    boots = []
    for w in city_bootstrap_weights(meta["place_id"].unique(), 500, a.seed):
        boots.append(rel_disp(pt, w) - rel_disp(pf, w))
    boots = np.array([b for b in boots if np.isfinite(b)])
    lo, hi = np.percentile(boots, [2.5, 97.5]) if boots.size else (np.nan, np.nan)
    per = pd.DataFrame([{"habitat": h, "R_species": rel_disp(pt, habitat=h), "R_function": rel_disp(pf, habitat=h)}
                        for h in sorted(meta["urban_context"].unique())])
    per["convergence"] = per["R_species"] - per["R_function"]
    mt = mantel(Dt, Df, permutations=min(a.permutations, 499), seed=a.seed)
    files = [write_table(per, a.outdir, "relative_dispersion_by_habitat.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(5, 3.4))
        x = np.arange(len(per))
        ax.bar(x - 0.2, per["R_species"], 0.4, label="species", color="#9a9a9a")
        ax.bar(x + 0.2, per["R_function"], 0.4, label="function", color="#2a6f97")
        ax.axhline(1, ls="--", lw=0.8, color="#666")
        ax.set_xticks(x, [h.replace("_", "\n") for h in per["habitat"]], fontsize=7)
        ax.set(ylabel="Relative dispersion across cities")
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "convergence.png"))
        files.append("convergence.png")
    status = "supported" if np.isfinite(lo) and lo > 0 else ("not_supported" if np.isfinite(hi) and hi < 0
                                                              else "inconclusive")
    write_summary(a.outdir, a.id, TITLE, QUESTION, status,
                  {"samples": len(meta), "R_species": r_t, "R_function": r_f, "convergence": diff,
                   "convergence_ci95": [lo, hi], "mantel_species_vs_function": mt,
                   "by_habitat": per.set_index("habitat").to_dict(orient="index")},
                  interpretation=(f"Relative dispersion across cities: species {r_t:.3f}, function {r_f:.3f} "
                                  f"(difference {diff:+.3f}, 95% CI {lo:.3f} to {hi:.3f})."),
                  caveats=["Function profiles from read mapping (HUMAnN/UniRef→KO) only cover characterised genes.",
                           "KO profiles are less sparse than species profiles, which lowers Bray–Curtis "
                           "dispersion by itself; R is a ratio within each data type to reduce this, and a "
                           "phylogenetically controlled comparison is planned."],
                  files=files)


if __name__ == "__main__":
    main()
