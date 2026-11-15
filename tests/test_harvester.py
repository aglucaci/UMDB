"""Offline unit tests for the UMDB harvester. Run: python -m unittest discover tests"""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from urbanscope_harvester import ncbi  # noqa: E402
from urbanscope_harvester.assay import classify_assay  # noqa: E402
from urbanscope_harvester.biosample import infer_geo, parse_geo_loc_name  # noqa: E402
from urbanscope_harvester.config import QUERY_PROFILES  # noqa: E402


def row(strategy="", source="", selection="", name=""):
    return {"LibraryStrategy": strategy, "LibrarySource": source, "LibrarySelection": selection, "LibraryName": name}


class AssayTests(unittest.TestCase):
    def cls(self, *a, title="", attrs=None):
        return classify_assay(row(*a), title, {"attributes": attrs or {}})["assay_class"]

    def test_metatranscriptomics_is_not_rna_seq(self):
        self.assertEqual(self.cls("RNA-Seq", "METATRANSCRIPTOMIC", "RANDOM"), "Metatranscriptomics")
        self.assertEqual(self.cls("WGS", "METATRANSCRIPTOMIC", "RANDOM"), "Metatranscriptomics")

    def test_host_transcriptomics(self):
        self.assertEqual(self.cls("RNA-Seq", "TRANSCRIPTOMIC", "cDNA"), "RNA-seq")

    def test_viral_rna(self):
        self.assertEqual(self.cls("WGA", "VIRAL RNA", "PCR", title="SARS-CoV-2 ARTIC amplicon wastewater"), "Viral RNA")
        self.assertEqual(self.cls("AMPLICON", "VIRAL RNA", "PCR"), "Viral RNA")

    def test_marker_genes(self):
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="16S rRNA V4"), "16S")
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="fungal ITS2"), "ITS")
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="18S survey"), "18S")

    def test_english_its_is_not_marker(self):
        # "its" / "sites" previously triggered the ITS class.
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="subway sites and its surfaces"), "Amplicon")

    def test_wgs_strategy_beats_pcr_selection(self):
        self.assertEqual(self.cls("WGS", "METAGENOMIC", "PCR"), "WGS")

    def test_unknown(self):
        self.assertEqual(self.cls("OTHER", "OTHER", "other"), "Unknown")


class GeoTests(unittest.TestCase):
    def test_state_abbrev_not_city(self):
        d = parse_geo_loc_name("USA: Ames, IA")
        self.assertEqual((d["city"], d["region"], d["city_status"]), ("Ames", "Iowa", "parsed"))

    def test_state_only(self):
        for raw in ("USA: Iowa", "USA: AZ", "China: Guangdong Province", "USA: Miami Dade County"):
            d = parse_geo_loc_name(raw)
            self.assertEqual(d["city"], "", raw)
            self.assertEqual(d["city_status"], "region_only", raw)

    def test_placeholders_and_continents(self):
        for raw in ("USA: multiple cities", "Kenya: Africa", "USA: city"):
            self.assertEqual(parse_geo_loc_name(raw)["city"], "", raw)

    def test_city_named_like_state(self):
        self.assertEqual(parse_geo_loc_name("USA: New York, NY")["city"], "New York")

    def test_missing_country(self):
        self.assertEqual(parse_geo_loc_name("missing")["country"], "")

    def test_infer_geo_normalizes_and_keeps_provenance(self):
        g = infer_geo({"accession": "SAMN1", "attributes": {
            "geo_loc_name": "USA: Boston, MA", "lat_lon": "42.36 N 71.06 W", "collection_date": "2021-05-03"}}, [])
        self.assertEqual(g["country"], "United States")
        self.assertEqual(g["city"], "Boston")
        self.assertEqual((g["lat"], g["lon"]), ("42.36", "-71.06"))
        self.assertEqual(g["collection_date"], "2021-05-03")
        self.assertEqual(g["raw"], "USA: Boston, MA")
        self.assertEqual(g["resolution"], "text_parsed_unvalidated")

    def test_country_fallback_needs_word_boundary(self):
        # "uk" inside "Fukuoka" must not yield United Kingdom.
        self.assertEqual(infer_geo({}, ["Fukuoka sewer"])["country"], "")


class NcbiTests(unittest.TestCase):
    EXPXML = ("&lt;Summary&gt;&lt;Title&gt;Subway swab 12&lt;/Title&gt;&lt;/Summary&gt;"
              "&lt;Experiment acc=\"SRX1\" name=\"x\"/&gt;&lt;Study acc=\"SRP1\" name=\"NYC subway\"/&gt;")

    def _docsum_xml(self, uids):
        docs = "".join(
            f'<DocSum><Id>{u}</Id><Item Name="ExpXml" Type="String">{self.EXPXML}</Item></DocSum>' for u in uids)
        return f"<eSummaryResult>{docs}</eSummaryResult>".encode()

    def test_title_from_expxml(self):
        with mock.patch.object(ncbi, "http_get", side_effect=lambda url: self._docsum_xml(["1"])):
            out, _ = ncbi.esummary_sra(["1"])
        self.assertEqual(out["1"]["title"], "Subway swab 12")
        self.assertEqual(out["1"]["study_name"], "NYC subway")
        self.assertEqual(out["1"]["experiment_accession"], "SRX1")

    def test_all_uids_summarized_in_batches(self):
        uids = [str(i) for i in range(1, 1201)]
        calls = []

        def fake_get(url):
            import urllib.parse
            ids = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["id"][0].split(",")
            calls.append(len(ids))
            return self._docsum_xml(ids)

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            out, _ = ncbi.esummary_sra(uids)
        self.assertEqual(len(out), 1200)
        self.assertTrue(all(n <= ncbi.ESUMMARY_BATCH_SIZE for n in calls))


class QueryProfileTests(unittest.TestCase):
    def test_urban_compartments_not_excluded(self):
        for name, q in QUERY_PROFILES.items():
            not_clause = q.split("NOT", 1)[1].lower() if "NOT" in q else ""
            for term in ("soil", "sediment", "freshwater", "river", "lake", " plant "):
                self.assertNotIn(term, not_clause, f"{name} excludes {term.strip()}")


if __name__ == "__main__":
    unittest.main()
