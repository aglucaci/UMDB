"""Unit tests for the UMDB-SCIENCE statistics helpers. Run: python3 -m unittest discover pipeline/tests

Needs numpy, pandas and scipy (envs/analysis.yml); skipped otherwise.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
try:
    import numpy as np
    import umdb_common as uc
    from umdb_select_cohort import water_fill
except ImportError:  # pragma: no cover
    uc = None


@unittest.skipIf(uc is None, "numpy/pandas/scipy not installed")
class StatsTests(unittest.TestCase):
    def test_gini(self):
        self.assertAlmostEqual(uc.gini([1, 1, 1, 1]), 0.0)
        self.assertAlmostEqual(uc.gini([0, 0, 0, 10]), 0.75)

    def test_hhi(self):
        self.assertAlmostEqual(uc.hhi([1, 1, 1, 1]), 0.25)

    def test_haversine(self):
        # London to Paris is about 344 km
        self.assertAlmostEqual(float(uc.haversine_km(51.5074, -0.1278, 48.8566, 2.3522)), 343.5, delta=2)

    def test_rarefaction_bounds(self):
        inc = np.array([[1, 0, 1], [1, 1, 0], [1, 0, 0]])
        self.assertAlmostEqual(uc.incidence_rarefaction(inc, 3), 3.0)   # all units: observed richness
        self.assertAlmostEqual(uc.incidence_rarefaction(inc, 1), 5 / 3)  # mean richness per unit (2, 2, 1)

    def test_chao2(self):
        inc = np.array([[1, 1, 0, 0], [1, 0, 1, 0], [1, 0, 0, 1]])  # 3 uniques, 0 duplicates
        c = uc.chao2(inc)
        self.assertEqual(c["s_obs"], 4)
        self.assertAlmostEqual(c["chao2"], 4 + (2 / 3) * 3 * 2 / 2)

    def test_permanova_detects_groups(self):
        rng = np.random.default_rng(1)
        X = np.vstack([rng.dirichlet(np.r_[np.full(5, 5.0), np.full(5, 0.2)], 15),
                       rng.dirichlet(np.r_[np.full(5, 0.2), np.full(5, 5.0)], 15)])
        res = uc.permanova(uc.bray_curtis(X), ["a"] * 15 + ["b"] * 15, permutations=99, seed=1)
        self.assertLess(res["p"], 0.05)
        self.assertGreater(res["R2"], 0.5)

    def test_dbrda_full_design_explains_all(self):
        rng = np.random.default_rng(2)
        D = uc.bray_curtis(rng.dirichlet(np.ones(6), 8))
        r2, _ = uc.dbrda_r2(uc.gower_centered(D), np.eye(8)[:, 1:])
        self.assertAlmostEqual(r2, 1.0, places=6)

    def test_water_fill(self):
        self.assertEqual(water_fill({"a": 100, "b": 5, "c": 100}, 60), {"a": 28, "b": 5, "c": 27})
        self.assertEqual(sum(water_fill({"a": 3, "b": 4}, 100).values()), 7)


if __name__ == "__main__":
    unittest.main()
