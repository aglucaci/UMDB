#!/usr/bin/env python3
"""C5 · Does metadata completeness vary by assay, region and time?

For each completeness outcome (resolved place, coordinates, full collection date,
MIxS env_broad_scale), a population-averaged logistic model with standard errors
clustered by BioProject (GEE, independence working correlation; submitters fill
metadata per study, so samples in a study are not independent):
    outcome ~ assay + release period (+ continent, for outcomes that do not define it)
Reports odds ratios with 95% CIs and a joint Wald test for the assay terms. Samples
are randomly subsampled to --max-rows for speed.

Decision rule: "supported" when the assay terms are jointly significant (p < 0.05)
for every outcome.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import chi2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import CONTINENTS, base_parser, load_samples, parse_args, plt, write_summary, write_table  # noqa: E402

TITLE = "Metadata completeness"
QUESTION = "Does metadata completeness vary systematically by assay, region and release period?"
OUTCOMES = {"has_place": False, "has_coordinates": False, "has_collection_date": True,
            "has_env_broad_scale": True}  # value: include continent as a predictor


def main():
    p = base_parser(__doc__)
    p.add_argument("--max-rows", type=int, default=60000)
    a = parse_args(p)
    import statsmodels.api as sm
    import statsmodels.formula.api as smf

    s = load_samples(a.samples).dropna(subset=["project", "primary_assay", "first_release_year"])
    if len(s) > a.max_rows:
        s = s.sample(a.max_rows, random_state=a.seed)
    top = s["primary_assay"].value_counts()
    keep = top[top >= 200].index
    s["assay"] = np.where(s["primary_assay"].isin(keep), s["primary_assay"], "other")
    s["period"] = pd.cut(s["first_release_year"], [0, 2015, 2019, 2022, 3000],
                         labels=["<=2015", "2016-19", "2020-22", "2023+"]).astype(str)
    s["continent"] = s["continent"].map(lambda k: CONTINENTS.get(k, "unresolved") if isinstance(k, str)
                                        else "unresolved")
    ref_assay = "WGS" if "WGS" in set(s["assay"]) else s["assay"].mode()[0]
    rows, wald, rates = [], {}, []
    for out, with_cont in OUTCOMES.items():
        s["y"] = s[out].astype(bool).astype(int)
        f = f"y ~ C(assay, Treatment('{ref_assay}')) + C(period, Treatment('2016-19'))"
        if with_cont:
            f += " + C(continent, Treatment('Europe'))"
        # GEE with independence working correlation = logistic GLM with BioProject-clustered SEs.
        # (Exchangeable GEE diverges here: some studies hold >10,000 identical-metadata samples.)
        groups = pd.factorize(s["project"])[0]
        r = smf.glm(f, data=s, family=sm.families.Binomial()).fit(cov_type="cluster",
                                                                   cov_kwds={"groups": groups})
        ci = r.conf_int()
        for term in r.params.index:
            if term == "Intercept":
                continue
            rows.append({"outcome": out, "term": term, "odds_ratio": np.exp(r.params[term]),
                         "ci_low": np.exp(ci.loc[term, 0]), "ci_high": np.exp(ci.loc[term, 1]),
                         "p": r.pvalues[term]})
        names = [t for t in r.params.index if t.startswith("C(assay")]
        R = np.zeros((len(names), len(r.params)))
        for i, t in enumerate(names):
            R[i, list(r.params.index).index(t)] = 1
        b = R @ r.params.to_numpy()
        V = R @ r.cov_params().to_numpy() @ R.T
        stat = float(b @ np.linalg.pinv(V) @ b)
        wald[out] = {"chi2": stat, "df": len(names), "p": float(chi2.sf(stat, len(names)))}
        g = s.groupby("assay")["y"].mean().rename(out)
        rates.append(g)
    ors = pd.DataFrame(rows)
    rate_tab = pd.concat(rates, axis=1).mul(100).round(1)
    files = [write_table(ors, a.outdir, "odds_ratios.tsv"),
             write_table(rate_tab.reset_index(), a.outdir, "completeness_by_assay.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(6, 3.4))
        im = ax.imshow(rate_tab.to_numpy(), cmap="viridis", vmin=0, vmax=100, aspect="auto")
        ax.set_xticks(range(rate_tab.shape[1]), [c.replace("has_", "") for c in rate_tab.columns], rotation=30,
                      ha="right")
        ax.set_yticks(range(rate_tab.shape[0]), rate_tab.index)
        fig.colorbar(im, ax=ax, label="% complete")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "completeness.png"))
        files.append("completeness.png")
    ok = all(v["p"] < 0.05 for v in wald.values())
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"rows_modelled": len(s), "reference_assay": ref_assay, "assay_wald_tests": wald,
                   "completeness_pct_by_assay": rate_tab.to_dict()},
                  interpretation="Joint Wald tests of the assay terms: " +
                                 ", ".join(f"{k} (p={v['p']:.2g})" for k, v in wald.items()),
                  caveats=["Continent is omitted for place and coordinate outcomes, which define it.",
                           "GEE gives population-averaged effects; a BioProject random-effects model would give "
                           "study-specific ones."],
                  files=files)


if __name__ == "__main__":
    main()
