#!/usr/bin/env python3
"""H3 · Do urban microbial diversity measures scale with city population (D = D0 N^beta)?

Two complementary estimates, both effort-standardised:

* City level (gamma diversity). For each (city, habitat) with >= --k samples, expected
  species richness in k samples by incidence rarefaction. Fit
      log S_k = alpha_habitat + beta log N
  by OLS; 95% CI by cluster bootstrap over cities. Alternative models (no population
  term; quadratic in log N) are compared by AIC.
* Sample level (alpha diversity). Observed species per sample (profiles come from
  depth-capped reads) ~ log N + habitat with a random intercept per BioProject.

N is the GeoNames population of the validated place (not the metropolitan area).

Decision rule: "supported" when the city-level beta 95% interval excludes 0.
beta < 1 (sublinear) is the expected form; the estimate, not the decision, says which.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, incidence_rarefaction, load_ecology, parse_args, plt, skip,  # noqa: E402
                         write_summary, write_table)

TITLE = "Scaling of urban microbial diversity with city size"
QUESTION = "Does effort-standardised microbial richness scale as a power law of city population?"


def ols(df, formula):
    import statsmodels.formula.api as smf
    return smf.ols(formula, data=df).fit()


def main():
    p = base_parser(__doc__)
    p.add_argument("--k", type=int, default=5)
    a = parse_args(p)
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles.")
    meta = meta[meta["place_population"].notna() & (meta["place_population"] > 0)]
    inc = (X.loc[meta.index] > 0).astype(np.int8)
    rows = []
    for (city, hab), g in meta.groupby(["place_id", "urban_context"]):
        if len(g) < a.k:
            continue
        rows.append({"place_id": city, "habitat": hab, "place_name": g["place_name"].iloc[0],
                     "population": float(g["place_population"].iloc[0]), "n_samples": len(g),
                     "S_k": incidence_rarefaction(inc.loc[g.index].to_numpy(), a.k)})
    cities = pd.DataFrame(rows)
    if cities["place_id"].nunique() < 5:
        skip(a.outdir, a.id, TITLE, QUESTION, f"Fewer than 5 cities with >= {a.k} samples in a habitat.")
    cities["logS"] = np.log(cities["S_k"])
    cities["logN"] = np.log(cities["population"])
    hab_term = " + C(habitat)" if cities["habitat"].nunique() > 1 else ""
    m_pow = ols(cities, "logS ~ logN" + hab_term)
    m_null = ols(cities, "logS ~ 1" + hab_term)
    m_quad = ols(cities, "logS ~ logN + I(logN**2)" + hab_term)
    beta = float(m_pow.params["logN"])
    rng = np.random.default_rng(a.seed)
    ids = cities["place_id"].unique()
    boots = []
    for _ in range(1000):
        pick = rng.choice(ids, size=len(ids), replace=True)
        b = pd.concat([cities[cities["place_id"] == c] for c in pick], ignore_index=True)
        if b["logN"].nunique() > 2:
            try:
                boots.append(ols(b, "logS ~ logN" + hab_term).params["logN"])
            except Exception:
                pass
    lo, hi = np.percentile(boots, [2.5, 97.5])

    # sample-level alpha diversity with a study random intercept
    alpha = meta.assign(richness=inc.sum(axis=1), logN=np.log(meta["place_population"]))
    mixed = None
    try:
        import statsmodels.formula.api as smf
        f = "np.log(richness) ~ logN" + (" + C(urban_context)" if alpha["urban_context"].nunique() > 1 else "")
        r = smf.mixedlm(f, alpha[alpha["richness"] > 0], groups=alpha.loc[alpha["richness"] > 0, "project"]).fit()
        ci = r.conf_int().loc["logN"]
        mixed = {"beta": r.params["logN"], "ci_low": ci[0], "ci_high": ci[1], "p": r.pvalues["logN"],
                 "samples": int(r.nobs), "studies": alpha["project"].nunique()}
    except Exception as exc:  # singular fits on tiny inputs
        mixed = {"error": str(exc)}

    files = [write_table(cities, a.outdir, "city_richness.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(4.6, 3.6))
        for h, g in cities.groupby("habitat"):
            ax.scatter(g["population"], g["S_k"], s=14, label=h.replace("_", " "))
        ax.set(xscale="log", yscale="log", xlabel="City population (GeoNames)",
               ylabel=f"Expected species in {a.k} samples", title=f"β = {beta:.3f} [{lo:.3f}, {hi:.3f}]")
        ax.legend(frameon=False, fontsize=6)
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "scaling.png"))
        files.append("scaling.png")
    status = "supported" if (lo > 0 or hi < 0) else "not_supported"
    write_summary(a.outdir, a.id, TITLE, QUESTION, status,
                  {"cities": int(cities["place_id"].nunique()), "city_habitat_cells": len(cities), "k": a.k,
                   "beta_city": beta, "beta_city_ci95": [lo, hi],
                   "aic": {"null": m_null.aic, "power_law": m_pow.aic, "quadratic_logN": m_quad.aic},
                   "beta_sample_mixed": mixed},
                  interpretation=(f"Richness in {a.k} samples scales as N^{beta:.3f} (95% CI {lo:.3f}–{hi:.3f}) across "
                                  f"{cities['place_id'].nunique()} cities; ΔAIC (null − power) = "
                                  f"{m_null.aic - m_pow.aic:.1f}."),
                  caveats=["Place population is administrative, not functional urban area (GHSL integration planned).",
                           "Wastewater catchment size, not city size, sets the population a sample integrates.",
                           "Reference-based richness counts only catalogued species."],
                  files=files)


if __name__ == "__main__":
    main()
