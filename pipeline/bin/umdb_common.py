"""Shared helpers for the UMDB-SCIENCE analysis scripts.

Every hypothesis script takes the same arguments (see ``base_parser``) and
writes ``<outdir>/summary.json`` with a fixed shape (see ``write_summary``), so
the pipeline can run them generically and the report can collect them.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

URBAN_TIERS = ("confirmed_urban", "probable_urban")
CONTINENTS = {"AF": "Africa", "AS": "Asia", "EU": "Europe", "NA": "North America",
              "OC": "Oceania", "SA": "South America", "AN": "Antarctica"}


# --------------------------------------------------------------------------- I/O

def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--samples", required=True, help="samples.tsv.gz from umdb_snapshot.py")
    p.add_argument("--runs", help="runs.tsv.gz from umdb_snapshot.py")
    p.add_argument("--country-covariates", help="country_covariates.tsv from umdb_covariates.py")
    p.add_argument("--city-covariates", help="city_covariates.tsv from umdb_covariates.py")
    p.add_argument("--cities", help="populated places (GeoNames cities500.zip or cities.tsv)")
    p.add_argument("--species", help="species x sample relative-abundance matrix (TSV)")
    p.add_argument("--functions", help="function x sample abundance matrix (TSV)")
    p.add_argument("--arg", help="ARG x sample abundance matrix (TSV)")
    p.add_argument("--labels", help="expert labels for the eligibility benchmark (TSV)")
    p.add_argument("--outdir", required=True)
    p.add_argument("--id", required=True, help="hypothesis id, e.g. C1 or H3")
    p.add_argument("--seed", type=int, default=20261119)
    p.add_argument("--permutations", type=int, default=999)
    p.add_argument("--max-samples", type=int, default=3000,
                   help="cap for n x n distance analyses (stratified subsample)")
    p.add_argument("--build-date", help="catalog build date (YYYY-MM-DD); future dates are flagged against it")
    return p


def parse_args(p: argparse.ArgumentParser) -> argparse.Namespace:
    args, _unknown = p.parse_known_args()  # scripts share one CLI; ignore what they don't use
    os.makedirs(args.outdir, exist_ok=True)
    return args


def read_tsv(path: Optional[str], **kw) -> Optional[pd.DataFrame]:
    if not path or path in ("NO_FILE", "null") or os.path.basename(path).startswith("NO_FILE"):
        return None
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    # "NA" is Namibia and North America, not missing
    return pd.read_csv(path, sep="\t", low_memory=False, keep_default_na=False, na_values=[""], **kw)


def read_matrix(path: Optional[str]) -> Optional[pd.DataFrame]:
    """Feature x sample matrix -> sample x feature DataFrame (rows = BioSamples)."""
    df = read_tsv(path, index_col=0)
    if df is None:
        return None
    df = df.T.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    df = df.loc[:, df.sum(axis=0) > 0]
    df.index = df.index.astype(str)
    return df


def write_table(df: pd.DataFrame, outdir: str, name: str, index: bool = False) -> str:
    path = os.path.join(outdir, name)
    df.to_csv(path, sep="\t", index=index)
    return name


def _jsonable(x: Any) -> Any:
    if isinstance(x, dict):
        return {str(k): _jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_jsonable(v) for v in x]
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating, float)):
        v = float(x)
        return None if math.isnan(v) or math.isinf(v) else float(f"{v:.6g}")
    if isinstance(x, np.bool_):
        return bool(x)
    return x


def write_summary(outdir: str, hid: str, title: str, question: str, status: str,
                  results: Dict[str, Any], interpretation: str = "",
                  caveats: Sequence[str] = (), files: Sequence[str] = ()) -> None:
    """status: 'supported' | 'not_supported' | 'inconclusive' | 'descriptive' | 'skipped'."""
    doc = {
        "id": hid, "title": title, "question": question, "status": status,
        "interpretation": interpretation, "results": results,
        "caveats": list(caveats), "files": list(files),
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(_jsonable(doc), fh, indent=2, sort_keys=False)
        fh.write("\n")
    print(f"[{hid}] {status}: {interpretation}", file=sys.stderr)


def skip(outdir: str, hid: str, title: str, question: str, reason: str) -> None:
    write_summary(outdir, hid, title, question, "skipped", {}, interpretation=reason)
    sys.exit(0)


def plt():
    """matplotlib.pyplot with a headless backend, or None if matplotlib is missing."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as pyplot
        pyplot.rcParams.update({"figure.dpi": 150, "font.size": 9, "axes.spines.top": False,
                                "axes.spines.right": False})
        return pyplot
    except Exception:  # pragma: no cover - plotting is optional
        return None


# ------------------------------------------------------------------ samples

def load_samples(path: str, urban_only: bool = True) -> pd.DataFrame:
    s = read_tsv(path)
    if s is None:
        raise SystemExit(f"cannot read samples table: {path}")
    if urban_only:
        s = s[s["urban_status"].isin(URBAN_TIERS)]
    return s.copy()


def analysis_samples(samples_path: str, profiled: pd.Index) -> pd.DataFrame:
    """Metadata for profiled BioSamples that pass the ecology filters."""
    s = load_samples(samples_path, urban_only=False).set_index("biosample")
    s = s.loc[s.index.intersection(profiled)]
    s = s[s["urban_status"].isin(URBAN_TIERS)]
    s = s[s["urban_context"].notna() & (s["urban_context"] != "")]
    return s


def stratified_subsample(df: pd.DataFrame, by: Sequence[str], n_max: int, seed: int) -> pd.DataFrame:
    """Keep at most n_max rows, sampling evenly across strata (cities, habitats)."""
    if len(df) <= n_max:
        return df
    rng = np.random.default_rng(seed)
    groups = list(df.groupby(list(by)).groups.values())
    per = max(1, n_max // max(1, len(groups)))
    keep: List[Any] = []
    for idx in groups:
        idx = list(idx)
        keep.extend(rng.choice(idx, size=min(per, len(idx)), replace=False).tolist())
    if len(keep) > n_max:
        keep = rng.choice(keep, size=n_max, replace=False).tolist()
    return df.loc[keep]


# ------------------------------------------------------------------ inequality

def gini(x: Iterable[float]) -> float:
    a = np.sort(np.asarray(list(x), dtype=float))
    a = a[np.isfinite(a)]
    if a.size == 0 or a.sum() == 0:
        return float("nan")
    n = a.size
    return float((2 * np.sum(np.arange(1, n + 1) * a) / (n * a.sum())) - (n + 1) / n)


def lorenz(x: Iterable[float]) -> np.ndarray:
    a = np.sort(np.asarray(list(x), dtype=float))
    c = np.concatenate([[0.0], np.cumsum(a)])
    return c / c[-1] if c[-1] > 0 else c


def hhi(counts: Iterable[float]) -> float:
    a = np.asarray(list(counts), dtype=float)
    if a.sum() == 0:
        return float("nan")
    s = a / a.sum()
    return float(np.sum(s ** 2))


# ------------------------------------------------------------------ geography

def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def pairwise_km(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    return haversine_km(lat[:, None], lon[:, None], lat[None, :], lon[None, :])


def climate_band(lat: float) -> str:
    """Coarse latitude-band climate proxy, used when no Köppen raster is supplied."""
    if lat is None or not np.isfinite(lat):
        return "unknown"
    a = abs(lat)
    if a < 23.44:
        return "tropical"
    if a < 35:
        return "subtropical"
    if a < 60:
        return "temperate"
    return "boreal_polar"


# ------------------------------------------------------------------ community ecology

def closure(X: np.ndarray) -> np.ndarray:
    tot = X.sum(axis=1, keepdims=True)
    tot[tot == 0] = 1.0
    return X / tot


def bray_curtis(X: np.ndarray) -> np.ndarray:
    from scipy.spatial.distance import pdist, squareform
    return squareform(pdist(closure(X), metric="braycurtis"))


def jaccard(X: np.ndarray) -> np.ndarray:
    from scipy.spatial.distance import pdist, squareform
    return squareform(pdist(X > 0, metric="jaccard"))


def gower_centered(D: np.ndarray) -> np.ndarray:
    A = -0.5 * D ** 2
    n = A.shape[0]
    J = np.eye(n) - np.ones((n, n)) / n
    return J @ A @ J


def pcoa(D: np.ndarray, k: int = 3) -> np.ndarray:
    G = gower_centered(D)
    w, v = np.linalg.eigh(G)
    order = np.argsort(w)[::-1][:k]
    w, v = w[order], v[:, order]
    return v * np.sqrt(np.clip(w, 0, None))


def design(df: pd.DataFrame, factors: Sequence[str]) -> np.ndarray:
    """Dummy-coded design matrix (drop-first) for one or more categorical factors."""
    if not factors:
        return np.zeros((len(df), 0))
    d = pd.get_dummies(df[list(factors)].astype(str), drop_first=True, dtype=float)
    return d.to_numpy()


def dbrda_r2(G: np.ndarray, X: np.ndarray) -> (float, int):
    """Fraction of total (Gower-centred) inertia explained by the column space of X."""
    if X.shape[1] == 0:
        return 0.0, 0
    Xc = X - X.mean(axis=0)
    Q, R = np.linalg.qr(Xc)
    rank = int(np.sum(np.abs(np.diag(R)) > 1e-8))
    Q = Q[:, np.abs(np.diag(R)) > 1e-8]
    H = Q @ Q.T
    return float(np.trace(H @ G) / np.trace(G)), rank


def adj_r2(r2: float, n: int, p: int) -> float:
    if n - p - 1 <= 0:
        return float("nan")
    return 1 - (1 - r2) * (n - 1) / (n - p - 1)


def permanova(D: np.ndarray, groups: Sequence[Any], permutations: int = 999,
              strata: Optional[Sequence[Any]] = None, seed: int = 0) -> Dict[str, float]:
    """One-way PERMANOVA (Anderson 2001). Permutations are restricted within strata if given."""
    g = pd.Series(list(groups)).astype(str).to_numpy()
    n = len(g)
    D2 = D ** 2
    labels, inv = np.unique(g, return_inverse=True)
    a = len(labels)
    if a < 2 or a >= n:
        return {"F": float("nan"), "R2": float("nan"), "p": float("nan"), "groups": a, "n": n}
    sst = D2[np.triu_indices(n, 1)].sum() / n

    def ssw(codes):
        tot = 0.0
        for k in range(a):
            idx = np.where(codes == k)[0]
            if idx.size > 1:
                sub = D2[np.ix_(idx, idx)]
                tot += sub[np.triu_indices(idx.size, 1)].sum() / idx.size
        return tot

    w = ssw(inv)
    F = ((sst - w) / (a - 1)) / (w / (n - a)) if w > 0 else float("inf")
    rng = np.random.default_rng(seed)
    strata_arr = None if strata is None else pd.Series(list(strata)).astype(str).to_numpy()
    ge = 0
    for _ in range(permutations):
        if strata_arr is None:
            perm = rng.permutation(inv)
        else:
            perm = inv.copy()
            for s in np.unique(strata_arr):
                idx = np.where(strata_arr == s)[0]
                perm[idx] = rng.permutation(inv[idx])
        wp = ssw(perm)
        Fp = ((sst - wp) / (a - 1)) / (wp / (n - a)) if wp > 0 else float("inf")
        ge += Fp >= F
    return {"F": float(F), "R2": float((sst - w) / sst), "p": (ge + 1) / (permutations + 1),
            "groups": a, "n": n}


def mantel(D1: np.ndarray, D2: np.ndarray, permutations: int = 999, seed: int = 0,
           D3: Optional[np.ndarray] = None) -> Dict[str, float]:
    """(Partial) Mantel test with Spearman correlation on the upper triangles."""
    from scipy.stats import rankdata
    iu = np.triu_indices(D1.shape[0], 1)

    def r(a, b, c=None):
        x, y = rankdata(a[iu]), rankdata(b[iu])
        if c is None:
            return float(np.corrcoef(x, y)[0, 1])
        z = rankdata(c[iu])
        rxy, rxz, ryz = np.corrcoef(x, y)[0, 1], np.corrcoef(x, z)[0, 1], np.corrcoef(y, z)[0, 1]
        return float((rxy - rxz * ryz) / math.sqrt(max(1e-12, (1 - rxz ** 2) * (1 - ryz ** 2))))

    obs = r(D1, D2, D3)
    rng = np.random.default_rng(seed)
    n = D1.shape[0]
    ge = 0
    for _ in range(permutations):
        p = rng.permutation(n)
        ge += r(D1[np.ix_(p, p)], D2, D3) >= obs
    return {"r": obs, "p": (ge + 1) / (permutations + 1), "n": n}


def incidence_rarefaction(inc: np.ndarray, k: int) -> float:
    """Expected species richness in k sampling units (incidence matrix: units x species)."""
    from scipy.special import gammaln
    n = inc.shape[0]
    if k > n:
        return float("nan")
    q = inc.sum(axis=0)
    q = q[q > 0]

    def lc(a, b):
        return gammaln(a + 1) - gammaln(b + 1) - gammaln(a - b + 1)

    with np.errstate(invalid="ignore"):
        absent = np.where(n - q >= k, np.exp(lc(n - q, k) - lc(n, k)), 0.0)
    return float(np.sum(1 - absent))


def chao2(inc: np.ndarray) -> Dict[str, float]:
    """Bias-corrected Chao2 richness and Chao-Jost sample coverage from an incidence matrix."""
    n = inc.shape[0]
    q = inc.sum(axis=0)
    s_obs = int(np.sum(q > 0))
    q1, q2 = int(np.sum(q == 1)), int(np.sum(q == 2))
    f = (n - 1) / n if n > 0 else 0
    est = s_obs + f * q1 * (q1 - 1) / (2 * (q2 + 1))
    u = q.sum()
    cov = 1 - (q1 / u) * (((n - 1) * q1) / ((n - 1) * q1 + 2 * q2)) if u > 0 and (q1 + q2) > 0 else 1.0
    return {"n_units": n, "s_obs": s_obs, "q1": q1, "q2": q2, "chao2": float(est),
            "completeness": s_obs / est if est > 0 else float("nan"), "sample_coverage": float(cov),
            "new_species_next_unit": float(q1 / n) if n else float("nan")}


def cluster_bootstrap(values: pd.DataFrame, cluster: str, stat, n_boot: int = 500, seed: int = 0):
    """Percentile CI of stat(df) resampling whole clusters (e.g. cities) with replacement."""
    rng = np.random.default_rng(seed)
    groups = {k: v for k, v in values.groupby(cluster)}
    keys = list(groups)
    out = []
    for _ in range(n_boot):
        pick = rng.choice(len(keys), size=len(keys), replace=True)
        df = pd.concat([groups[keys[i]] for i in pick], ignore_index=True)
        try:
            out.append(stat(df))
        except Exception:
            continue
    a = np.array([x for x in out if x is not None and np.isfinite(x)])
    if a.size == 0:
        return float("nan"), float("nan")
    return float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))


# ------------------------------------------------------------------ ecology inputs

def load_ecology(args, matrix: str = "species", min_samples: int = 20):
    """(X, meta): sample x feature matrix and aligned metadata for profiled urban samples.

    Exits with a 'skipped' summary when the matrix is missing or too small.
    """
    X = read_matrix(getattr(args, matrix))
    if X is None:
        return None, None
    meta = analysis_samples(args.samples, X.index)
    meta = meta[meta["place_id"].notna()]
    meta["place_id"] = meta["place_id"].astype(int).astype(str)
    meta["project"] = meta["project"].fillna("unknown")
    X = X.loc[meta.index]
    X = X.loc[:, X.sum(axis=0) > 0]
    if len(meta) < min_samples:
        return None, None
    cc = read_tsv(getattr(args, "country_covariates", None))
    if cc is not None:
        cols = [c for c in ("iso2", "gdp_per_capita_usd2015", "sanitation_safely_managed_pct",
                            "urban_population_pct") if c in cc.columns]
        meta = meta.join(cc[cols].set_index("iso2"), on="country_iso2")
    return X, meta


def pair_frame(meta: pd.DataFrame, D: np.ndarray) -> pd.DataFrame:
    """Upper-triangle pairs with same/different habitat, city and project flags."""
    i, j = np.triu_indices(len(meta), 1)
    h = meta["urban_context"].to_numpy()
    c = meta["place_id"].to_numpy()
    p = meta["project"].to_numpy()
    return pd.DataFrame({"i": i, "j": j, "d": D[i, j], "same_habitat": h[i] == h[j],
                         "same_city": c[i] == c[j], "same_project": p[i] == p[j],
                         "city_i": c[i], "city_j": c[j], "habitat_i": h[i], "habitat_j": h[j]})


def weighted_pair_mean(pairs: pd.DataFrame, mask: np.ndarray, w_city: Optional[Dict[str, float]] = None) -> float:
    sub = pairs[mask]
    if w_city is None:
        return float(sub["d"].mean()) if len(sub) else float("nan")
    w = sub["city_i"].map(w_city).fillna(0).to_numpy() * sub["city_j"].map(w_city).fillna(0).to_numpy()
    return float(np.sum(w * sub["d"]) / np.sum(w)) if np.sum(w) > 0 else float("nan")


def city_bootstrap_weights(cities: Sequence[str], n_boot: int, seed: int):
    """Yield {city: multiplicity} for a cluster bootstrap over cities."""
    rng = np.random.default_rng(seed)
    cities = list(cities)
    for _ in range(n_boot):
        draw = rng.choice(len(cities), size=len(cities), replace=True)
        w = np.bincount(draw, minlength=len(cities))
        yield {cities[k]: float(w[k]) for k in range(len(cities))}
