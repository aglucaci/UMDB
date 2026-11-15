from __future__ import annotations
import urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple

from .config import EUTILS, NCBI_API_KEY, TOOL_NAME, NCBI_EMAIL, BIOPROJECT_RE
from .utils import _sleep_backoff

def http_get(url: str, retries: int = 6) -> bytes:
    headers = {"User-Agent": f"{TOOL_NAME}/1.0 ({NCBI_EMAIL or 'no-email'})"}
    req = urllib.request.Request(url, headers=headers)
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except Exception:
            _sleep_backoff(i)
    raise RuntimeError(f"HTTP failed: {url}")

def parse_xml(data: bytes) -> ET.Element:
    return ET.fromstring(data)

def _eutils_params(extra: Dict[str, str]) -> Dict[str, str]:
    p = dict(extra)
    if NCBI_API_KEY:
        p["api_key"] = NCBI_API_KEY
    if TOOL_NAME:
        p["tool"] = TOOL_NAME
    if NCBI_EMAIL:
        p["email"] = NCBI_EMAIL
    return p

def esummary(db: str, ids: List[str]) -> Tuple[ET.Element, str]:
    if not ids:
        return ET.Element("EMPTY"), ""
    params = _eutils_params({"db": db, "id": ",".join(ids), "retmode": "xml"})
    url = EUTILS + "esummary.fcgi?" + urllib.parse.urlencode(params)
    return parse_xml(http_get(url)), url

def esearch_any(db: str, term: str, retmax: int = 10) -> Tuple[List[str], str]:
    params = _eutils_params({"db": db, "term": term, "retmode": "xml", "retmax": str(retmax)})
    url = EUTILS + "esearch.fcgi?" + urllib.parse.urlencode(params)
    root = parse_xml(http_get(url))
    ids = [x.text for x in root.findall(".//Id") if x.text]
    return ids, url

def esearch_day(db: str, term: str, day: str, retmax: int, datetype: str = "edat") -> Tuple[List[str], str]:
    params = _eutils_params({
        "db": db, "term": term, "retmode": "xml",
        "mindate": day, "maxdate": day, "datetype": datetype,
        "retmax": str(retmax),
    })
    url = EUTILS + "esearch.fcgi?" + urllib.parse.urlencode(params)
    root = parse_xml(http_get(url))
    ids = [x.text for x in root.findall(".//Id") if x.text]
    return ids, url

def esearch_recent(db: str, term: str, reldate_days: int, retmax: int, datetype: str = "edat") -> Tuple[List[str], str]:
    params = _eutils_params({
        "db": db, "term": term, "retmode": "xml",
        "reldate": str(reldate_days), "datetype": datetype,
        "retmax": str(retmax), "sort": "date",
    })
    url = EUTILS + "esearch.fcgi?" + urllib.parse.urlencode(params)
    root = parse_xml(http_get(url))
    ids = [x.text for x in root.findall(".//Id") if x.text]
    return ids, url

def esearch_history(db: str, term: str, retstart: int, retmax: int, sort: str = ""):
    params = _eutils_params({
        "db": db, "term": term, "retmode": "xml",
        "retstart": str(retstart), "retmax": str(retmax),
        "usehistory": "n",
    })
    if sort:
        params["sort"] = sort
    url = EUTILS + "esearch.fcgi?" + urllib.parse.urlencode(params)
    root = parse_xml(http_get(url))
    ids = [x.text for x in root.findall(".//Id") if x.text]
    count_total = int((root.findtext(".//Count") or "0").strip() or "0")
    return ids, count_total, url

def efetch_runinfo_text(uid: str) -> Tuple[str, str]:
    params = _eutils_params({"db": "sra", "id": uid, "rettype": "runinfo", "retmode": "text"})
    url = EUTILS + "efetch.fcgi?" + urllib.parse.urlencode(params)
    text = http_get(url).decode(errors="replace")
    return text, url

ESUMMARY_BATCH_SIZE = 200


def parse_sra_expxml(expxml: str) -> Dict[str, str]:
    """Pull title/study/experiment fields out of the escaped ExpXml blob.

    SRA esummary has no top-level Title item; the experiment title lives in
    ExpXml as an XML fragment with several root elements.
    """
    if not expxml or not isinstance(expxml, str):
        return {}
    try:
        root = ET.fromstring(f"<root>{expxml}</root>")
    except ET.ParseError:
        return {}
    study = root.find("Study")
    exp = root.find("Experiment")
    org = root.find("Organism")
    return {
        "title": (root.findtext("Summary/Title") or "").strip(),
        "study_accession": (study.attrib.get("acc", "") if study is not None else "").strip(),
        "study_name": (study.attrib.get("name", "") if study is not None else "").strip(),
        "experiment_accession": (exp.attrib.get("acc", "") if exp is not None else "").strip(),
        "organism": (org.attrib.get("ScientificName", "") if org is not None else "").strip(),
    }


def esummary_sra(uids: List[str], batch_size: int = ESUMMARY_BATCH_SIZE) -> Tuple[Dict[str, Dict[str, Any]], str]:
    """Summarize every UID, in batches (previous callers truncated to 500)."""
    out: Dict[str, Dict[str, Any]] = {}
    first_url = ""
    for i in range(0, len(uids), batch_size):
        batch_out, url = _esummary_sra_batch(uids[i:i + batch_size])
        out.update(batch_out)
        first_url = first_url or url
    return out, first_url


def _esummary_sra_batch(uids: List[str]) -> Tuple[Dict[str, Dict[str, Any]], str]:
    if not uids:
        return {}, ""
    root, url = esummary("sra", uids)
    out: Dict[str, Dict[str, Any]] = {}

    for d in root.findall(".//DocSum"):
        uid = (d.findtext("Id") or "").strip()
        if not uid:
            continue
        items: Dict[str, Any] = {}
        for it in d.findall("Item"):
            name = it.attrib.get("Name", "")
            if not name:
                continue
            if list(it):
                sub = [x.text for x in it.findall(".//Item") if x.text]
                items[name] = sub if sub else (it.text or "").strip()
            else:
                items[name] = (it.text or "").strip()

        exp = parse_sra_expxml(items.get("ExpXml", "") if isinstance(items.get("ExpXml"), str) else "")
        title = (items.get("Title") or exp.get("title") or "").strip()

        bioproject_guess = ""
        for v in items.values():
            if isinstance(v, str):
                m = BIOPROJECT_RE.search(v)
                if m:
                    bioproject_guess = m.group(0).upper()
                    break
            elif isinstance(v, list):
                for s in v:
                    m = BIOPROJECT_RE.search(s or "")
                    if m:
                        bioproject_guess = m.group(0).upper()
                        break

        out[uid] = {
            "uid": uid,
            "title": title,
            "study_name": exp.get("study_name", ""),
            "experiment_accession": exp.get("experiment_accession", ""),
            "bioproject_guess": bioproject_guess,
            "items": items,
        }

    return out, url
