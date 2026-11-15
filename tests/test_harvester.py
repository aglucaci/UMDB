"""Offline unit tests for the UMDB harvester. Run: python -m unittest discover tests"""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from umdb_harvester import ncbi  # noqa: E402
from umdb_harvester.assay import classify_assay  # noqa: E402
from umdb_harvester.biosample import infer_geo, parse_geo_loc_name  # noqa: E402
from umdb_harvester.config import QUERY_PROFILES  # noqa: E402
from umdb_harvester.eligibility import assess_eligibility  # noqa: E402


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


class DateSearchTests(unittest.TestCase):
    def test_day_search_uses_pdat_and_slash_dates(self):
        # SRA ignores datetype=edat and ISO dashes; both silently return zero hits.
        import urllib.parse
        seen = {}

        def fake_get(url):
            seen.update(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
            return b"<eSearchResult><IdList/></eSearchResult>"

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ncbi.esearch_day("sra", "wastewater", "2024-06-12", 10)
        self.assertEqual(seen["datetype"], ["pdat"])
        self.assertEqual(seen["mindate"], ["2024/06/12"])
        self.assertEqual(seen["maxdate"], ["2024/06/12"])

    def test_recent_search_uses_pdat(self):
        import urllib.parse
        seen = {}

        def fake_get(url):
            seen.update(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
            return b"<eSearchResult><IdList/></eSearchResult>"

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ncbi.esearch_recent("sra", "wastewater", 7, 10)
        self.assertEqual(seen["datetype"], ["pdat"])


def rec_with(organism, city="", city_status="parsed", project_title=""):
    return {
        "runinfo_row": {"ScientificName": organism},
        "geo": {"city": city, "city_status": city_status},
        "bioproject": {"title": project_title, "description": ""},
    }


def bs(**attrs):
    return {"attributes": attrs, "title": ""}


class EligibilityTests(unittest.TestCase):
    def tier(self, rec, biosample=None):
        return assess_eligibility(rec, biosample or {}, rec["bioproject"])["status"]

    # Stage 2: environmental eligibility
    def test_human_gut_is_non_environmental(self):
        r = rec_with("human gut metagenome", city="New York City")
        self.assertEqual(self.tier(r, bs(host="Homo sapiens", isolation_source="stool")), "non_environmental")

    def test_clinical_blood_in_city_is_non_environmental(self):
        r = rec_with("metagenome", city="Boston")
        self.assertEqual(self.tier(r, bs(isolation_source="human blood")), "non_environmental")

    def test_clinical_isolate_is_non_environmental(self):
        r = rec_with("Staphylococcus aureus", city="Iowa City")
        self.assertEqual(self.tier(r, bs(isolation_source="patient sputum")), "non_environmental")

    def test_lab_material_is_non_environmental(self):
        r = rec_with("bioreactor metagenome", city="Nanjing")
        self.assertEqual(self.tier(r), "non_environmental")

    def test_no_environmental_evidence_is_uncertain(self):
        self.assertEqual(self.tier(rec_with("metagenome", city="Chicago")), "uncertain")

    def test_isolate_from_wastewater_is_uncertain(self):
        r = rec_with("Escherichia coli")
        self.assertEqual(self.tier(r, bs(isolation_source="wastewater")), "uncertain")

    def test_host_field_holding_environment_is_not_a_host(self):
        r = rec_with("wastewater metagenome", city="Zurich")
        self.assertEqual(self.tier(r, bs(host="Wastewater")), "probable_urban")

    # Stage 3: urban origin
    def test_explicit_building_setting_urban(self):
        r = rec_with("metagenome")
        self.assertEqual(self.tier(r, bs(building_setting="urban", isolation_source="surface swab")), "confirmed_urban")

    def test_subway_handrail_is_confirmed(self):
        r = rec_with("metagenome", city="New York City")
        self.assertEqual(self.tier(r, bs(isolation_source="subway handrail")), "confirmed_urban")

    def test_municipal_wastewater_influent_is_confirmed(self):
        r = rec_with("wastewater metagenome")
        self.assertEqual(self.tier(r, bs(isolation_source="municipal wastewater influent")), "confirmed_urban")

    def test_city_alone_gives_probable(self):
        r = rec_with("soil metagenome", city="Chicago")
        self.assertEqual(self.tier(r, bs(isolation_source="soil")), "probable_urban")

    def test_project_context_alone_gives_probable(self):
        r = rec_with("air metagenome", project_title="Microbiome of the New York City subway")
        self.assertEqual(self.tier(r), "probable_urban")

    def test_wastewater_surveillance_project_gives_probable(self):
        r = rec_with("wastewater metagenome", project_title="Wastewater surveillance of respiratory viruses")
        self.assertEqual(self.tier(r), "probable_urban")

    def test_wastewater_without_urban_evidence_is_uncertain(self):
        self.assertEqual(self.tier(rec_with("wastewater metagenome")), "uncertain")

    def test_ambiguous_state_city_is_uncertain(self):
        r = rec_with("freshwater metagenome", city="", city_status="region_only")
        self.assertEqual(self.tier(r, bs(env_medium="freshwater")), "uncertain")

    def test_rural_site_is_non_urban(self):
        r = rec_with("soil metagenome")
        r["geo"]["raw"] = "USA: rural Pennsylvania"
        self.assertEqual(self.tier(r, bs(env_medium="soil")), "non_urban")
        self.assertEqual(self.tier(rec_with("metagenome"), bs(built_struc_set="rural", env_medium="dust")), "non_urban")

    def test_city_in_place_name_is_not_urban_wording(self):
        r = rec_with("farm metagenome")
        self.assertEqual(self.tier(r, bs(isolation_source="camel stable, China: Fukang City, Xinjiang")), "non_urban")
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(isolation_source="city park soil")), "confirmed_urban")

    def test_swine_wastewater_is_non_urban(self):
        r = rec_with("wastewater metagenome", city="Xinxing", project_title="Metagenomes of swine wastewater")
        self.assertEqual(self.tier(r), "non_urban")

    def test_space_station_is_non_urban(self):
        r = rec_with("indoor metagenome", project_title="Microbial mapping of the ISS environment")
        self.assertEqual(self.tier(r, bs(isolation_source="indoor surface")), "non_urban")

    def test_urban_soil_needs_urban_evidence(self):
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(env_medium="soil")), "uncertain")
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(env_broad_scale="urban biome", env_medium="soil")), "confirmed_urban")

    # Stage 4: annotation
    def test_result_records_structured_evidence(self):
        r = rec_with("air metagenome", city="Shanghai")
        r["geo"].update({"raw": "China: Shanghai", "raw_field": "geo_loc_name"})
        out = assess_eligibility(r, bs(isolation_source="subway platform air"), {})
        self.assertEqual(out["status"], "confirmed_urban")
        self.assertIn("transit", out["urban_context"])
        self.assertEqual(out["location_resolution"], "city")
        self.assertEqual(out["location_validation"], "plausible")
        self.assertTrue(out["environmental_sample"])
        self.assertEqual(out["decision_method"], "rules")
        fields = {(e["source"], e["field"], e["signal"]) for e in out["evidence"]}
        self.assertIn(("BioSample", "isolation_source", "urban_infrastructure"), fields)
        self.assertIn(("BioSample", "geo_loc_name", "urban_location"), fields)


class CatalogReadTests(unittest.TestCase):
    def test_part_files_are_read_once(self):
        import json, tempfile
        from umdb_harvester import exports
        from umdb_harvester.utils import iter_jsonl_glob
        with tempfile.TemporaryDirectory() as d:
            for name, runs in (("srr_catalog_2026.jsonl", ["A", "B"]),
                               ("srr_catalog_2026_part000.jsonl", ["C"]),
                               ("srr_catalog_2026_part001.jsonl", ["D"]),
                               ("srr_catalog_2025.jsonl", ["E"])):
                with open(os.path.join(d, name), "w") as fh:
                    fh.writelines(json.dumps({"srr": r}) + "\n" for r in runs)
            with mock.patch.object(exports, "DATA_DIR", d):
                bases = exports._find_year_catalog_prefixes()
            self.assertEqual([os.path.basename(b) for b in bases], ["srr_catalog_2025.jsonl", "srr_catalog_2026.jsonl"])
            runs = [r["srr"] for b in bases for r in iter_jsonl_glob(b)]
            self.assertEqual(sorted(runs), ["A", "B", "C", "D", "E"])


class RefreshTests(unittest.TestCase):
    def _write(self, d, name, recs):
        import json
        with open(os.path.join(d, name), "w") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in recs)

    def test_newest_record_wins_and_compaction_drops_superseded(self):
        import tempfile
        from umdb_harvester import exports
        from umdb_harvester.utils import iter_jsonl_glob
        old = {"srr": "SRR1", "title": "old", "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}}
        new = {"srr": "SRR1", "title": "new", "provenance": {"ingested_utc": "2026-11-15T00:00:00Z"}}
        other = {"srr": "SRR2", "provenance": {"ingested_utc": "2026-02-01T00:00:00Z"}}
        with tempfile.TemporaryDirectory() as d:
            # The refreshed record lands in an older year's file: newest timestamp must still win.
            self._write(d, "srr_catalog_2026.jsonl", [old, other])
            self._write(d, "srr_catalog_2024.jsonl", [new])
            with mock.patch.object(exports, "DATA_DIR", d):
                bases = exports._find_year_catalog_prefixes()
                winners = exports.latest_record_positions(bases)
                kept = [rec for pos, _, rec in exports._iter_catalog(bases) if winners[rec["srr"]] == pos]
                self.assertEqual(sorted((r["srr"], r.get("title")) for r in kept), [("SRR1", "new"), ("SRR2", None)])
                stats = exports.compact_catalog()
                bases = exports._find_year_catalog_prefixes()
            self.assertEqual(stats, {"records_before": 3, "records_after": 2, "superseded_removed": 1})
            after = [r for b in bases for r in iter_jsonl_glob(b)]
            self.assertEqual(sorted((r["srr"], r.get("title")) for r in after), [("SRR1", "new"), ("SRR2", None)])
            self.assertFalse([n for n in os.listdir(d) if n.startswith("umdb_compact_")])

    def test_day_search_pages_until_count(self):
        import urllib.parse
        calls = []

        def fake_get(url):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            start, size = int(q["retstart"][0]), int(q["retmax"][0])
            calls.append((start, size))
            ids = "".join(f"<Id>{i}</Id>" for i in range(start, min(start + size, 25000)))
            return f"<eSearchResult><Count>25000</Count><IdList>{ids}</IdList></eSearchResult>".encode()

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ids, _ = ncbi.esearch_day("sra", "x", "2024-01-01", 0)
            capped, _ = ncbi.esearch_day("sra", "x", "2024-01-01", 500)
        self.assertEqual(len(ids), 25000)
        self.assertEqual(len(set(ids)), 25000)
        self.assertEqual(calls[:3], [(0, 10000), (10000, 10000), (20000, 10000)])
        self.assertEqual(len(capped), 500)

    def test_refresh_reingests_seen_runs_without_duplicating_seen_lists(self):
        from umdb_harvester import ingest
        fake_rows = [{"srr": "SRR9", "sra_uid": "1"}]
        with mock.patch.object(ingest, "build_srr_records_for_sra_uid", return_value=(fake_rows, {})), \
             mock.patch.object(ingest, "append_lines") as append_lines:
            seen_sra, seen_srr = {"1"}, {"SRR9"}
            added, report = ingest.ingest_uids_to_srr(
                tag="t", uids=["1"], summaries={}, biosample_cache={}, bp_cache={}, bp_uid_cache={},
                seen_sra=seen_sra, seen_srr=seen_srr, fetch_biosample=False, fetch_bioproject=False,
                debug=False, runinfo_max_rows=0, refresh=False)
            self.assertEqual(added, [])
            added, report = ingest.ingest_uids_to_srr(
                tag="t", uids=["1"], summaries={}, biosample_cache={}, bp_cache={}, bp_uid_cache={},
                seen_sra=seen_sra, seen_srr=seen_srr, fetch_biosample=False, fetch_bioproject=False,
                debug=False, runinfo_max_rows=0, refresh=True)
            self.assertEqual([r["srr"] for r in added], ["SRR9"])
            self.assertEqual(report["counters"].get("srr_refreshed"), 1)
            append_lines.assert_not_called()


class SlimRecordTests(unittest.TestCase):
    def test_slim_drops_repeated_text_and_keeps_site_fields(self):
        from umdb_harvester.utils import slim_record
        rec = {"srr": "SRR1",
               "bioproject": {"accession": "PRJNA1", "uid": "9", "title": "Subway", "center_name": "C",
                              "description": "long abstract " * 50, "esummary_items": {"x": "y"},
                              "ncbi": {"bioproject_url": "u"}},
               "debug": {"runinfo_url": "r", "runinfo_columns": ["Run"] * 47, "biosample_url": "b"}}
        out = slim_record(rec)
        self.assertEqual(out["bioproject"], {"accession": "PRJNA1", "uid": "9", "title": "Subway", "center_name": "C"})
        self.assertEqual(out["debug"], {"runinfo_url": "r", "biosample_url": "b"})
        self.assertEqual(slim_record(out), out)          # idempotent
        self.assertIn("description", rec["bioproject"])  # input untouched

    def test_export_uses_cached_project_text_for_eligibility(self):
        from umdb_harvester.exports import refresh_derived_annotations
        rec = {"srr": "SRR1", "title": "", "runinfo_row": {"BioProject": "PRJNA1", "ScientificName": "air metagenome"},
               "bioproject": {"accession": "PRJNA1", "title": "Air study"}, "geo": {}}
        bp_cache = {"PRJNA1": {"accession": "PRJNA1", "title": "Air study",
                               "description": "Bioaerosols sampled across the New York City subway system"}}
        without = refresh_derived_annotations(rec, {}, {})
        with_cache = refresh_derived_annotations(rec, {}, bp_cache)
        self.assertEqual(without["eligibility"]["status"], "uncertain")
        self.assertEqual(with_cache["eligibility"]["status"], "probable_urban")
        self.assertNotIn("description", with_cache["bioproject"])
        self.assertTrue(all("value" not in e for e in with_cache["eligibility"]["evidence"] if e["source"] == "BioProject"))


class DatabaseTests(unittest.TestCase):
    def _catalog(self, d):
        import json
        os.makedirs(os.path.join(d, "cache"))
        recs = [
            {"srr": "SRR1", "sra_uid": "1", "title": "subway swab",
             "runinfo_row": {"Run": "SRR1", "Experiment": "SRX1", "BioSample": "SAMN1", "BioProject": "PRJNA1",
                             "ScientificName": "metagenome", "LibraryStrategy": "WGS", "LibrarySource": "METAGENOMIC",
                             "ReleaseDate": "2024-05-01 10:00:00", "spots": "1000", "bases": "150000"},
             "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}},
            {"srr": "SRR1", "sra_uid": "1", "title": "subway swab (refreshed)",
             "runinfo_row": {"Run": "SRR1", "Experiment": "SRX1", "BioSample": "SAMN1", "BioProject": "PRJNA1",
                             "ScientificName": "metagenome", "LibraryStrategy": "WGS", "LibrarySource": "METAGENOMIC",
                             "ReleaseDate": "2024-05-01 10:00:00", "spots": "2000", "bases": "300000"},
             "provenance": {"ingested_utc": "2026-11-15T00:00:00Z"}},
            {"srr": "SRR2", "sra_uid": "2", "title": "stool",
             "runinfo_row": {"Run": "SRR2", "Experiment": "SRX2", "BioSample": "SAMN2", "BioProject": "PRJNA1",
                             "ScientificName": "human gut metagenome", "ReleaseDate": "2024-05-02"},
             "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}},
        ]
        with open(os.path.join(d, "srr_catalog_2026.jsonl"), "w") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in recs)
        json.dump({"SAMN1": {"accession": "SAMN1", "attributes": {
                        "isolation_source": "subway station handrail", "geo_loc_name": "USA: New York City",
                        "collection_date": "2019-06-04"}},
                   "SAMN2": {"accession": "SAMN2", "attributes": {"isolation_source": "stool", "host": "Homo sapiens",
                                                                   "collection_date": "2019"}}},
                  open(os.path.join(d, "cache", "biosample.json"), "w"))
        json.dump({"PRJNA1": {"accession": "PRJNA1", "title": "NYC subway", "description": "Subway surfaces"}},
                  open(os.path.join(d, "cache", "bioproject.json"), "w"))

    def test_build_query_and_newest_record(self):
        import sqlite3, tempfile
        from umdb_harvester import database, exports, config
        with tempfile.TemporaryDirectory() as d:
            self._catalog(d)
            db = os.path.join(d, "umdb.sqlite")
            with mock.patch.object(exports, "DATA_DIR", d), \
                 mock.patch.object(database, "BIOSAMPLE_CACHE", os.path.join(d, "cache", "biosample.json")), \
                 mock.patch.object(database, "BIOPROJECT_CACHE", os.path.join(d, "cache", "bioproject.json")), \
                 mock.patch.object(database, "AI_CURATION_CACHE", os.path.join(d, "cache", "ai.json")):
                counts = database.build_database(db)
            self.assertEqual((counts["runs"], counts["samples"], counts["projects"]), (2, 2, 1))
            con = sqlite3.connect(db)
            self.assertEqual(con.execute("SELECT spots FROM runs WHERE accession='SRR1'").fetchone()[0], 2000)
            s1 = con.execute("SELECT urban_status, urban_context, collection_date, collection_year, city "
                             "FROM samples WHERE accession='SAMN1'").fetchone()
            self.assertEqual(s1, ("confirmed_urban", "transit", "2019-06-04", 2019, "New York City"))
            s2 = con.execute("SELECT urban_status, collection_date, collection_year FROM samples WHERE accession='SAMN2'").fetchone()
            self.assertEqual(s2, ("non_environmental", None, 2019))
            hits = con.execute("SELECT accession FROM search WHERE search MATCH 'handrail'").fetchall()
            self.assertEqual(hits, [("SAMN1",)])
            self.assertEqual(con.execute("SELECT count(*) FROM umdb_runs").fetchone()[0], 2)
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
            # With 2 rows the planner rightly scans; check the indexes exist instead.
            indexes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
            self.assertTrue({"idx_samples_cohort", "idx_samples_urban", "idx_runs_sample"} <= indexes)
            con.close()

    def test_collection_date_parsing(self):
        from umdb_harvester.database import parse_collection_date
        self.assertEqual(parse_collection_date("2019-06-04"), ("2019-06-04", 2019))
        self.assertEqual(parse_collection_date("2019-06"), (None, 2019))
        self.assertEqual(parse_collection_date("2019/06/04"), ("2019-06-04", 2019))
        self.assertEqual(parse_collection_date("missing"), (None, None))
        self.assertEqual(parse_collection_date("Jun-2019"), (None, None))


class QueryProfileTests(unittest.TestCase):
    def test_urban_compartments_not_excluded(self):
        for name, q in QUERY_PROFILES.items():
            not_clause = q.split(" NOT ", 1)[1].lower() if " NOT " in q else ""
            for term in ("soil", "sediment", "freshwater", "river", "lake", " plant ", "hospital"):
                self.assertNotIn(term, not_clause, f"{name} excludes {term.strip()}")

    def test_defaults_are_the_seven_environment_profiles(self):
        from umdb_harvester.config import DEFAULT_QUERY_PROFILE_NAMES
        self.assertEqual(len(DEFAULT_QUERY_PROFILE_NAMES), 7)
        self.assertNotIn("wastewater_sars_cov_2", DEFAULT_QUERY_PROFILE_NAMES)
        self.assertTrue(set(DEFAULT_QUERY_PROFILE_NAMES) <= set(QUERY_PROFILES))

    def test_no_terms_ncbi_mishandles(self):
        import re
        for name, q in QUERY_PROFILES.items():
            # NCBI drops "ITS" as a stopword and truncates short wildcards to 600 variants.
            self.assertIsNone(re.search(r"\bITS\b(?![12])", q), f"{name} relies on bare ITS")
            self.assertNotIn("metagenom*", q, f"{name} uses a truncated wildcard")

    def test_no_bare_catch_all_terms(self):
        import re
        for name, q in QUERY_PROFILES.items():
            unquoted = re.sub(r'"[^"]*"', " ", q)   # quoted phrases are fine
            for term in ("surface", "swab", "station", "air", "bus", "train"):
                self.assertIsNone(re.search(rf"\b{term}\b", unquoted, re.I), f"{name} uses bare '{term}'")


if __name__ == "__main__":
    unittest.main()
