from __future__ import annotations
import urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple

from .config import EUTILS, NCBI_API_KEY, TOOL_NAME, NCBI_EMAIL, BIOPROJECT_RE
import re
import time

from .utils import _sleep_backoff, log, warn, num, paint

# Running totals for progress and summary lines.
STATS = {"requests": 0, "retries": 0, "failures": 0, "bytes": 0, "seconds": 0.0}


def _redact(url: str) -> str:
    return re.sub(r"(api_key|email)=[^&]+", r"\1=REDACTED", url)


# --- TLS -------------------------------------------------------------------
# Certificates are always verified. Python ships its own CA bundle and ignores
# the macOS keychain, so networks that inspect HTTPS (institutional proxies
# whose root certificate IT installs in the System keychain) fail with
# CERTIFICATE_VERIFY_FAILED. On macOS UMDB then retries once with the roots
# from the System keychain added. UMDB_CA_BUNDLE points at an explicit PEM file.
import os
import ssl
import subprocess
import sys

_SSL_CONTEXT: "ssl.SSLContext | None" = None
_KEYCHAIN_TRIED = False
MACOS_KEYCHAINS = ("/Library/Keychains/System.keychain",
                   "/System/Library/Keychains/SystemRootCertificates.keychain")


def _ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT
    if _SSL_CONTEXT is None:
        _SSL_CONTEXT = ssl.create_default_context()
        bundle = os.getenv("UMDB_CA_BUNDLE", "").strip()
        if bundle:
            _SSL_CONTEXT.load_verify_locations(cafile=os.path.expanduser(bundle))
            log(f"Using extra CA certificates from UMDB_CA_BUNDLE={bundle}")
    return _SSL_CONTEXT


def _macos_keychain_bundle() -> "str | None":
    """Export certificates from the macOS System keychains to a PEM file."""
    try:
        pem = subprocess.run(["security", "find-certificate", "-a", "-p", *MACOS_KEYCHAINS],
                             capture_output=True, text=True, timeout=60).stdout
    except Exception:
        return None
    if "BEGIN CERTIFICATE" not in pem:
        return None
    path = os.path.join(os.path.expanduser("~"), ".cache", "umdb", "macos-system-roots.pem")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(pem)
    return path


def _is_cert_error(exc: Exception) -> bool:
    reason = getattr(exc, "reason", exc)
    return isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(exc)


def _try_keychain_roots() -> bool:
    """Add macOS System keychain roots to the context, once. True if added."""
    global _KEYCHAIN_TRIED
    if _KEYCHAIN_TRIED or sys.platform != "darwin":
        return False
    _KEYCHAIN_TRIED = True
    path = _macos_keychain_bundle()
    if not path:
        return False
    _ssl_context().load_verify_locations(cafile=path)
    log(paint("Python's CA bundle could not verify NCBI's certificate chain; "
              f"added the macOS System keychain roots ({path}). Certificates are still verified.", "dim"))
    return True


CERT_HELP = (
    "TLS certificate verification failed for NCBI. Something on your network presents a certificate "
    "Python does not trust (often an institutional HTTPS-inspection proxy). Fixes: (1) get your "
    "organisation's root certificate (.pem) and set UMDB_CA_BUNDLE=/path/to/root.pem in .env; "
    "(2) run from a network without inspection (e.g. off VPN); (3) ask IT to install the root in the "
    "macOS System keychain. Verification is never disabled."
)


def http_get(url: str, retries: int = 6) -> bytes:
    headers = {"User-Agent": f"{TOOL_NAME}/1.0 ({NCBI_EMAIL or 'no-email'})"}
    req = urllib.request.Request(url, headers=headers)
    last_err = ""
    attempt = 0
    while attempt < retries:
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=60, context=_ssl_context()) as r:
                data = r.read()
            dt_s = time.time() - t0
            STATS["requests"] += 1
            STATS["bytes"] += len(data)
            STATS["seconds"] += dt_s
            log(f"    GET {_redact(url)[:160]} -> {len(data):,} B in {dt_s:.2f}s", level=2)
            return data
        except Exception as exc:
            last_err = str(exc)
            if _is_cert_error(exc):
                if _try_keychain_roots():
                    continue  # retry immediately with the extra roots; not a counted attempt
                STATS["failures"] += 1
                raise RuntimeError(f"{CERT_HELP} (error: {last_err})") from exc
            STATS["retries"] += 1
            attempt += 1
            if attempt < retries:
                warn(f"NCBI request failed ({last_err}); retry {attempt}/{retries - 1}: {_redact(url)[:120]}")
                _sleep_backoff(attempt - 1)
    STATS["failures"] += 1
    raise RuntimeError(f"HTTP failed after {retries} attempts ({last_err}): {_redact(url)}")


def stats_line() -> str:
    s = STATS
    avg = (s["seconds"] / s["requests"]) if s["requests"] else 0.0
    failures = paint(f"{s['failures']} failures", "err" if s["failures"] else "dim")
    retries = paint(f"{s['retries']} retries", "warn" if s["retries"] else "dim")
    return (f"NCBI: {num(s['requests'])} requests, {retries}, {failures}, "
            f"{s['bytes'] / 1e6:.1f} MB downloaded, {avg:.2f}s per request")

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

# SRA supports only "pdat" (public release date) and "mdat" (modification
# date). "edat" is silently ignored and returns zero hits, and dates must be
# YYYY/MM/DD: ISO "YYYY-MM-DD" also returns zero hits.
SRA_DATETYPE = "pdat"


def _eutils_date(day: str) -> str:
    return (day or "").strip().replace("-", "/")


ESEARCH_PAGE_MAX = 10000  # largest retmax E-utilities accepts per request


def esearch_day(db: str, term: str, day: str, retmax: int, datetype: str = SRA_DATETYPE) -> Tuple[List[str], str]:
    """All UIDs released on one day, paging with retstart.

    retmax <= 0 means no cap: keep paging until NCBI's reported Count is
    reached. A positive retmax caps the total returned.
    """
    day = _eutils_date(day)
    page = ESEARCH_PAGE_MAX if retmax <= 0 else min(retmax, ESEARCH_PAGE_MAX)
    ids: List[str] = []
    first_url = ""
    start = 0
    while True:
        params = _eutils_params({
            "db": db, "term": term, "retmode": "xml",
            "mindate": day, "maxdate": day, "datetype": datetype,
            "retstart": str(start), "retmax": str(page),
        })
        url = EUTILS + "esearch.fcgi?" + urllib.parse.urlencode(params)
        first_url = first_url or url
        root = parse_xml(http_get(url))
        batch = [x.text for x in root.findall(".//Id") if x.text]
        count = int((root.findtext(".//Count") or "0").strip() or "0")
        ids.extend(batch)
        start += len(batch)
        if not batch or start >= count or (retmax > 0 and len(ids) >= retmax):
            break
    return (ids[:retmax] if retmax > 0 else ids), first_url

def esearch_recent(db: str, term: str, reldate_days: int, retmax: int, datetype: str = SRA_DATETYPE) -> Tuple[List[str], str]:
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
