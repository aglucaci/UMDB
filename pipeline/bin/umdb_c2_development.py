#!/usr/bin/env python3
"""C2 · Is public urban sequencing capacity associated with national economic development?

Negative-binomial regression, one row per country (population >= 1 M):
    urban_samples ~ log(GDP per capita) + urban population % + log(R&D % GDP)
                    + offset(log population)
with heteroskedasticity-robust SEs. The GDP coefficient is an elasticity: the % change
in samples per capita for a 1% change in GDP per capita. Also reports the same model
counting BioProjects (independent studies) rather than samples, which is less
dominated by a few large surveillance programmes.

Decision rule: "supported" when the GDP elasticity is > 0 with p < 0.05 in both models.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import base_parser, load_samples, parse_args, plt, read_tsv, skip, write_summary, write_table  # noqa: E402

TITLE = "Genomic surveillance capacity and economic development"
QUESTION = "Do richer countries contribute more public urban sequencing per person?"


def fit(df, y, covars):
    import statsmodels.api as sm
    X = sm.add_constant(df[covars])
    off = np.log(df["population"])

    def model(al):
        return sm.GLM(df[y], X, family=sm.families.NegativeBinomial(alpha=al), offset=off)

    # dispersion alpha by profile likelihood over a log grid, then refit with robust SEs
    grid = np.exp(np.linspace(np.log(0.05), np.log(20), 40))
    best = grid[int(np.argmax([model(al).fit().llf for al in grid]))]
    r = model(best).fit(cov_type="HC1")
    out = pd.DataFrame({"coef": r.params, "se": r.bse, "p": r.pvalues,
                        "ci_low": r.conf_int()[0], "ci_high": r.conf_int()[1]})
    out["alpha"] = best
    out["n"] = int(r.nobs)
    return out


def main():
    p = base_parser(__doc__)
    p.add_argument("--min-country-pop", type=float, default=1e6)
    a = parse_args(p)
    cov = read_tsv(a.country_covariates)
    if cov is None or "gdp_per_capita_usd2015" not in cov.columns:
        skip(a.outdir, a.id, TITLE, QUESTION, "No World Bank covariates (run with --fetch_worldbank or --worldbank).")
    s = load_samples(a.samples)
    s = s[s["country_status"].isin(["verified", "probable"])]
    c = cov.set_index("iso2")
    c = c.join(s.groupby("country_iso2").size().rename("urban_samples"))
    c = c.join(s.groupby("country_iso2")["project"].nunique().rename("urban_projects"))
    c = c.fillna({"urban_samples": 0, "urban_projects": 0})
    c = c[(c["population"] >= a.min_country_pop)].dropna(subset=["gdp_per_capita_usd2015"])
    c["log_gdp_pc"] = np.log(c["gdp_per_capita_usd2015"])
    covars = ["log_gdp_pc"]
    if c["urban_population_pct"].notna().mean() > 0.9:
        c = c.dropna(subset=["urban_population_pct"])
        c["urban_pop_pct_z"] = (c["urban_population_pct"] - c["urban_population_pct"].mean()) / \
            c["urban_population_pct"].std()
        covars.append("urban_pop_pct_z")
    tabs = []
    for y in ("urban_samples", "urban_projects"):
        t = fit(c, y, covars)
        t.insert(0, "outcome", y)
        tabs.append(t.reset_index().rename(columns={"index": "term"}))
    coefs = pd.concat(tabs, ignore_index=True)

    # R&D sensitivity model (fewer countries report it)
    rd = c.dropna(subset=["rd_expenditure_pct_gdp"])
    rd = rd[rd["rd_expenditure_pct_gdp"] > 0].copy()
    rd_res = None
    if len(rd) >= 30:
        rd["log_rd_pct_gdp"] = np.log(rd["rd_expenditure_pct_gdp"])
        t = fit(rd, "urban_samples", covars + ["log_rd_pct_gdp"])
        t.insert(0, "outcome", "urban_samples (+R&D)")
        rd_res = t.reset_index().rename(columns={"index": "term"})
        coefs = pd.concat([coefs, rd_res], ignore_index=True)

    files = [write_table(coefs, a.outdir, "nb_regression.tsv"),
             write_table(c.reset_index()[["iso2", "name", "continent", "population", "gdp_per_capita_usd2015",
                                          "urban_samples", "urban_projects"]], a.outdir, "country_table.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(4.5, 3.6))
        rate = (c["urban_samples"] + 0.5) / c["population"] * 1e6
        ax.scatter(c["gdp_per_capita_usd2015"], rate, s=10, color="#2a6f97", alpha=0.7)
        ax.set(xscale="log", yscale="log", xlabel="GDP per capita (constant 2015 US$)",
               ylabel="Urban samples per million people (+0.5)")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "gdp_vs_samples.png"))
        files.append("gdp_vs_samples.png")

    g = coefs[coefs["term"] == "log_gdp_pc"].set_index("outcome")
    ok = all(g.loc[y, "coef"] > 0 and g.loc[y, "p"] < 0.05 for y in ("urban_samples", "urban_projects"))
    res = {"countries": len(c), "covariates": covars,
           "gdp_elasticity_samples": g.loc["urban_samples", ["coef", "ci_low", "ci_high", "p"]].to_dict(),
           "gdp_elasticity_projects": g.loc["urban_projects", ["coef", "ci_low", "ci_high", "p"]].to_dict()}
    if rd_res is not None:
        res["rd_model_countries"] = int(rd_res["n"].iloc[0])
        res["rd_elasticity"] = rd_res.set_index("term").loc["log_rd_pct_gdp", ["coef", "p"]].to_dict()
    e = res["gdp_elasticity_samples"]
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported", res,
                  interpretation=(f"A 1% higher GDP per capita goes with {e['coef']:.2f}% more urban samples per "
                                  f"person (95% CI {e['ci_low']:.2f}–{e['ci_high']:.2f}, p={e['p']:.2g}), "
                                  f"across {len(c)} countries."),
                  caveats=["Ecological (country-level) association; not causal.",
                           "Samples are attributed to the country sampled, not the country of the sequencing centre.",
                           "World Bank values are the most recent non-missing year in the fetched window."],
                  files=files)


if __name__ == "__main__":
    main()
