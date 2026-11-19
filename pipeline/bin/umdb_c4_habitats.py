#!/usr/bin/env python3
"""C4 · Are some urban habitats systematically under-represented, and does it differ by continent?

Cross-tabulates urban BioSamples (and, separately, BioProjects, which are less
dominated by single large programmes) by habitat and continent. Reports Pielou
evenness of the habitat distribution globally and per continent, a chi-square test
of habitat x continent independence with standardised (adjusted) residuals, and the
habitats with the fewest independent studies.

Decision rule: "supported" when global evenness (samples) < 0.6 and the habitat x
continent test on BioProjects has p < 0.05.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import CONTINENTS, base_parser, load_samples, parse_args, plt, write_summary, write_table  # noqa: E402

TITLE = "Habitat representation"
QUESTION = "Are some urban habitats systematically under-sampled, and does the imbalance differ by continent?"


def pielou(counts):
    c = np.asarray(counts, dtype=float)
    c = c[c > 0]
    if c.size < 2:
        return float("nan")
    p = c / c.sum()
    return float(-(p * np.log(p)).sum() / np.log(c.size))


def adjusted_residuals(tab):
    chi, p, dof, exp = chi2_contingency(tab)
    n = tab.to_numpy().sum()
    r = tab.sum(axis=1).to_numpy()[:, None] / n
    c = tab.sum(axis=0).to_numpy()[None, :] / n
    res = (tab.to_numpy() - exp) / np.sqrt(exp * (1 - r) * (1 - c))
    return pd.DataFrame(res, index=tab.index, columns=tab.columns), chi, p


def main():
    a = parse_args(base_parser(__doc__))
    s = load_samples(a.samples)
    s = s[s["urban_context"].notna()].copy()
    s["continent"] = s["continent"].map(lambda k: CONTINENTS.get(k, "unresolved") if isinstance(k, str)
                                        else "unresolved")
    samples_tab = pd.crosstab(s["urban_context"], s["continent"])
    proj = s.drop_duplicates(["project", "urban_context", "continent"])
    proj_tab = pd.crosstab(proj["urban_context"], proj["continent"])
    resolved = proj_tab.drop(columns=["unresolved"], errors="ignore")
    resolved = resolved.loc[resolved.sum(axis=1) > 0, resolved.sum(axis=0) > 0]
    resid, chi, p = adjusted_residuals(resolved)

    habitat = pd.DataFrame({"samples": s["urban_context"].value_counts(),
                            "bioprojects": s.groupby("urban_context")["project"].nunique(),
                            "countries": s.groupby("urban_context")["country_iso2"].nunique(),
                            "places": s.groupby("urban_context")["place_id"].nunique()})
    habitat["samples_per_project"] = habitat["samples"] / habitat["bioprojects"]
    habitat = habitat.sort_values("samples", ascending=False)
    even = {c: pielou(samples_tab[c]) for c in samples_tab.columns}
    ev_global = pielou(habitat["samples"])

    files = [write_table(habitat.reset_index().rename(columns={"index": "habitat"}), a.outdir, "habitats.tsv"),
             write_table(samples_tab.reset_index(), a.outdir, "samples_habitat_by_continent.tsv"),
             write_table(proj_tab.reset_index(), a.outdir, "bioprojects_habitat_by_continent.tsv"),
             write_table(resid.round(2).reset_index(), a.outdir, "adjusted_residuals_bioprojects.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(6.5, 3.5))
        m = np.clip(resid.to_numpy(), -6, 6)
        im = ax.imshow(m, cmap="RdBu_r", vmin=-6, vmax=6, aspect="auto")
        ax.set_xticks(range(resid.shape[1]), resid.columns, rotation=40, ha="right")
        ax.set_yticks(range(resid.shape[0]), [x.replace("_", " ") for x in resid.index])
        fig.colorbar(im, ax=ax, label="Adjusted residual (BioProjects)")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "habitat_continent_residuals.png"))
        files.append("habitat_continent_residuals.png")
    ok = ev_global < 0.6 and p < 0.05
    least = habitat.sort_values("bioprojects").head(3)
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"evenness_global_samples": ev_global, "evenness_by_continent": even,
                   "chi2_bioprojects": chi, "p_bioprojects": p,
                   "habitat_share_samples": (habitat["samples"] / habitat["samples"].sum()).round(4).to_dict(),
                   "least_studied_habitats": least["bioprojects"].to_dict()},
                  interpretation=(f"Habitat evenness {ev_global:.2f}; "
                                  f"{habitat.index[0].replace('_', ' ')} holds "
                                  f"{100 * habitat['samples'].iloc[0] / habitat['samples'].sum():.0f}% of urban samples. "
                                  f"Least-studied: {', '.join(least.index)}. Habitat mix differs by continent "
                                  f"(χ²={chi:.0f}, p={p:.2g})."),
                  caveats=["Habitat is the eligibility screen's primary context; multi-habitat samples count once.",
                           "Discovery queries define which habitats can enter; habitats without query profiles "
                           "(e.g. hospitals, food markets) are absent by construction."],
                  files=files)


if __name__ == "__main__":
    main()
