#!/usr/bin/env python3
"""H11 · How does antimicrobial-resistance ecology differ across connected urban habitats?

From the ARG matrix (KMA against ResFinder, depth per gene):

* Resistome composition by habitat: PERMANOVA on Bray–Curtis with permutations
  restricted within cities (habitat effect holding city fixed).
* Resistome–microbiome coupling: Mantel correlation between resistome and species
  dissimilarities (Hendriksen et al. 2019 found composition tracks bacterial taxonomy).
* Total ARG load: log10 total depth ~ habitat + log GDP per capita + sanitation (country),
  with standard errors clustered by country (the sewage resistome–development
  association of Hendriksen et al., tested across habitats).

Decision rule: "supported" when habitat explains resistome composition within cities
(PERMANOVA p < 0.05).
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, bray_curtis, load_ecology, mantel, parse_args, permanova, plt, read_matrix,  # noqa: E402
                         skip, stratified_subsample, write_summary, write_table)

TITLE = "Urban antimicrobial resistance ecology"
QUESTION = "Do resistomes differ by habitat within cities, and track taxonomy and development?"


def main():
    a = parse_args(base_parser(__doc__))
    A, meta = load_ecology(a, matrix="arg")
    if A is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No ARG profiles.")
    meta = stratified_subsample(meta, ["urban_context", "place_id"], a.max_samples, a.seed)
    A = A.loc[meta.index]
    keep = A.sum(axis=1) > 0
    A, meta = A[keep], meta[keep]
    Da = bray_curtis(A.to_numpy())
    perm = permanova(Da, meta["urban_context"], a.permutations, strata=meta["place_id"], seed=a.seed)
    S = read_matrix(a.species)
    coupling = None
    if S is not None and meta.index.isin(S.index).all():
        coupling = mantel(Da, bray_curtis(S.loc[meta.index].to_numpy()), min(a.permutations, 499), a.seed)
    load = meta.assign(log_total=np.log10(A.sum(axis=1)))
    model = None
    try:
        import statsmodels.formula.api as smf
        terms = ["C(urban_context)"] if load["urban_context"].nunique() > 1 else []
        for c, t in (("gdp_per_capita_usd2015", "np.log10(gdp_per_capita_usd2015)"),
                     ("sanitation_safely_managed_pct", "sanitation_safely_managed_pct")):
            if c in load and load[c].notna().mean() > 0.8 and load["country_iso2"].nunique() >= 4:
                terms.append(t)
        d = load.dropna(subset=[c for c in ("gdp_per_capita_usd2015", "sanitation_safely_managed_pct")
                                if c in load and any(c in t for t in terms)])
        r = smf.ols("log_total ~ " + (" + ".join(terms) or "1"), data=d).fit(
            cov_type="cluster", cov_kwds={"groups": pd.factorize(d["country_iso2"])[0]})
        ci = r.conf_int()
        model = {t: {"coef": r.params[t], "ci_low": ci.loc[t, 0], "ci_high": ci.loc[t, 1], "p": r.pvalues[t]}
                 for t in r.params.index if t != "Intercept"}
    except Exception as exc:
        model = {"error": str(exc)}
    by_hab = load.groupby("urban_context")["log_total"].describe()
    top = A.groupby(meta["urban_context"]).mean().T
    files = [write_table(by_hab.reset_index(), a.outdir, "arg_load_by_habitat.tsv"),
             write_table(top.reset_index().rename(columns={"index": "gene"}), a.outdir, "mean_arg_by_habitat.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(5, 3.2))
        habs = sorted(load["urban_context"].unique())
        ax.boxplot([load.loc[load["urban_context"] == h, "log_total"] for h in habs], showfliers=False)
        ax.set_xticks(range(1, len(habs) + 1), [h.replace("_", "\n") for h in habs], fontsize=7)
        ax.set(ylabel="log10 total ARG depth")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "resistome.png"))
        files.append("resistome.png")
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if perm["p"] < 0.05 else "not_supported",
                  {"samples": len(meta), "genes": A.shape[1], "permanova_habitat_within_city": perm,
                   "mantel_resistome_vs_species": coupling, "load_model": model},
                  interpretation=(f"Habitat explains {100 * perm['R2']:.1f}% of resistome variation within cities "
                                  f"(p={perm['p']:.3g})" + (f"; resistome–species Mantel r = {coupling['r']:.2f}"
                                                            if coupling else "") + "."),
                  caveats=["Depth from depth-capped reads; gene length is accounted for by KMA's depth but not "
                           "16S-normalised (as in Munk et al. 2022).",
                           "Country-level GDP and sanitation cannot separate city- or catchment-level drivers."],
                  files=files)


if __name__ == "__main__":
    main()
