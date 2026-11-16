"""The harvested catalog lives in GitHub Release assets, not in git.

The JSONL run catalog, the NCBI caches and the seen lists grow with every
harvest (hundreds of MB after a deep backfill) and change on every run, so
storing them in git makes the repository grow without bound. Instead:

  umdb catalog push   pack data/ into dist/catalog/umdb-catalog.tar.gz plus a
                      manifest (file list, sizes, SHA-256) and upload both to
                      the rolling release `catalog-latest` (and, with
                      --snapshot, to a dated `catalog-YYYY.MM.DD` release)
  umdb catalog pull   download, verify and unpack that bundle into data/
  umdb catalog status what is local, what was last pulled, what is published

Every push records the bundle it was based on (`parent_sha256`). A push is
refused when the published bundle is no longer the one last pulled, so two
harvesters (a laptop and the scheduled workflow) cannot overwrite each
other's records: pull with --merge first. Merging is safe because the catalog
is append-only and the newest record per run wins (see `umdb compact`).

Uses the GitHub CLI (`gh`), already authenticated locally and in Actions.
"""
from __future__ import annotations

import glob
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from typing import Any, Dict, List, Optional

from .config import DATA_DIR, CACHE_DIR
from .utils import log, warn, paint, num, utc_now

LATEST_TAG = "catalog-latest"
BUNDLE = "umdb-catalog.tar.gz"
MANIFEST = "catalog-manifest.json"
OUT_DIR = os.path.join("dist", "catalog")
STATE_FILE = os.path.join(DATA_DIR, ".catalog-state.json")  # git-ignored: what was last pulled/pushed


def catalog_files() -> List[str]:
    """Paths (relative to the repo root) that make up the catalog."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "srr_catalog_*.jsonl")))
    files += sorted(glob.glob(os.path.join(CACHE_DIR, "*.json")))
    files += sorted(p for p in glob.glob(os.path.join(DATA_DIR, "seen_*.txt")))
    return [f for f in files if os.path.getsize(f) > 0 or f.endswith(".txt")]


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_state(**kw) -> None:
    state = _read_state()
    state.update(kw)
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    if not shutil.which("gh"):
        raise RuntimeError("the GitHub CLI (gh) is required: https://cli.github.com, then `gh auth login`")
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=check)


def pack(out_dir: str = OUT_DIR) -> Dict[str, Any]:
    """Write the bundle and its manifest to out_dir; return the manifest."""
    files = catalog_files()
    if not files:
        raise FileNotFoundError(f"no catalog files under {DATA_DIR}/")
    os.makedirs(out_dir, exist_ok=True)
    bundle = os.path.join(out_dir, BUNDLE)
    entries = []
    raw = 0
    with tarfile.open(bundle + ".tmp", "w:gz", compresslevel=6) as tar:
        for f in files:
            size = os.path.getsize(f)
            raw += size
            entries.append({"path": f, "bytes": size, "sha256": _sha256(f)})
            tar.add(f, arcname=f)
    os.replace(bundle + ".tmp", bundle)
    manifest = {
        "name": "UMDB harvested catalog",
        "created_utc": utc_now(),
        "bundle": BUNDLE,
        "bundle_bytes": os.path.getsize(bundle),
        "bundle_sha256": _sha256(bundle),
        "parent_sha256": _read_state().get("bundle_sha256", ""),
        "raw_bytes": raw,
        "license": "CC-BY-NC-SA-4.0",
        "files": entries,
    }
    with open(os.path.join(out_dir, MANIFEST), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log(f"Packed {num(len(files))} files ({raw / 1e6:.0f} MB) into {bundle} "
        f"({manifest['bundle_bytes'] / 1e6:.0f} MB)")
    return manifest


def unpack(bundle: str, manifest: Dict[str, Any], merge: bool = False) -> int:
    """Verify and extract a bundle into the working tree.

    merge=False replaces local catalog files. merge=True keeps local records
    and appends the published lines this checkout does not have (catalog JSONL
    and seen lists); caches are combined, local entries winning.
    """
    if _sha256(bundle) != manifest.get("bundle_sha256"):
        raise RuntimeError(f"{bundle} does not match the manifest checksum; download again")
    allowed = {e["path"] for e in manifest.get("files", [])}
    with tempfile.TemporaryDirectory() as tmp:
        with tarfile.open(bundle, "r:gz") as tar:
            for m in tar.getmembers():
                if m.name not in allowed or not m.isfile() or m.name.startswith(("/", "..")) or ".." in m.name.split("/"):
                    raise RuntimeError(f"unexpected entry in bundle: {m.name}")
            tar.extractall(tmp, filter="data") if hasattr(tarfile, "data_filter") else tar.extractall(tmp)
        for e in manifest["files"]:
            src = os.path.join(tmp, e["path"])
            if _sha256(src) != e["sha256"]:
                raise RuntimeError(f"{e['path']} failed its checksum")
        if not merge:
            for f in catalog_files():
                if f not in allowed:
                    os.remove(f)
            for e in manifest["files"]:
                os.makedirs(os.path.dirname(e["path"]) or ".", exist_ok=True)
                shutil.copyfile(os.path.join(tmp, e["path"]), e["path"])
            return len(manifest["files"])
        return _merge_into_local(tmp, manifest)


def _merge_into_local(tmp: str, manifest: Dict[str, Any]) -> int:
    from .utils import append_jsonl
    added = 0
    local_lines = set()
    for f in glob.glob(os.path.join(DATA_DIR, "srr_catalog_*.jsonl")):
        with open(f, encoding="utf-8") as fh:
            local_lines.update(hashlib.sha1(ln.strip().encode()).digest() for ln in fh if ln.strip())
    for e in manifest["files"]:
        src, dst = os.path.join(tmp, e["path"]), e["path"]
        name = os.path.basename(dst)
        if name.startswith("srr_catalog_"):
            year_base = os.path.join(DATA_DIR, name.split("_part")[0].replace(".jsonl", "") + ".jsonl")
            new = []
            with open(src, encoding="utf-8") as fh:
                for ln in fh:
                    if ln.strip() and hashlib.sha1(ln.strip().encode()).digest() not in local_lines:
                        new.append(json.loads(ln))
            if new:
                append_jsonl(year_base, new)
                added += len(new)
        elif name.startswith("seen_"):
            have = set()
            if os.path.exists(dst):
                with open(dst, encoding="utf-8") as fh:
                    have = {x.strip() for x in fh}
            with open(src, encoding="utf-8") as fh:
                extra = [x.strip() for x in fh if x.strip() and x.strip() not in have]
            if extra:
                with open(dst, "a", encoding="utf-8") as fh:
                    fh.write("\n".join(extra) + "\n")
        elif dst.endswith(".json"):
            with open(src, encoding="utf-8") as fh:
                remote = json.load(fh)
            local = {}
            if os.path.exists(dst):
                with open(dst, encoding="utf-8") as fh:
                    local = json.load(fh)
            if isinstance(remote, dict) and isinstance(local, dict):
                remote.update(local)
                from .utils import write_json
                write_json(dst, remote)
    log(f"Merged {num(added)} published catalog record(s) into data/; run `umdb compact` to drop superseded ones")
    return added


def _download(tag: str, dest: str) -> Optional[Dict[str, Any]]:
    """Download the manifest (and, if dest has room, nothing else). None if absent."""
    r = _gh("release", "download", tag, "--pattern", MANIFEST, "--dir", dest, "--clobber", check=False)
    if r.returncode != 0:
        if "release not found" in (r.stderr or "").lower() or "no assets" in (r.stderr or "").lower():
            return None
        raise RuntimeError(f"gh release download failed: {r.stderr.strip()}")
    with open(os.path.join(dest, MANIFEST), encoding="utf-8") as fh:
        return json.load(fh)


def pull(tag: str = LATEST_TAG, merge: bool = False, force: bool = False) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        manifest = _download(tag, tmp)
        if manifest is None:
            raise RuntimeError(f"release {tag} has no catalog yet (push one with `umdb catalog push`)")
        if not merge and not force and catalog_files():
            state = _read_state()
            local_changed = state.get("local_sha256") and state.get("local_sha256") != _local_fingerprint()
            if local_changed:
                raise RuntimeError("local catalog has changes since the last pull/push; use --merge to keep them, "
                                   "or --force to replace them with the published catalog")
        log(f"Downloading {tag}/{BUNDLE} ({manifest['bundle_bytes'] / 1e6:.0f} MB, created {manifest['created_utc']})")
        r = _gh("release", "download", tag, "--pattern", BUNDLE, "--dir", tmp, "--clobber", check=False)
        if r.returncode != 0:
            raise RuntimeError(f"gh release download failed: {r.stderr.strip()}")
        n = unpack(os.path.join(tmp, BUNDLE), manifest, merge=merge)
    _write_state(bundle_sha256=manifest["bundle_sha256"], pulled_tag=tag, pulled_utc=utc_now(),
                 local_sha256=_local_fingerprint())
    log(paint(f"Catalog {'merged' if merge else 'restored'} from {tag}", "good")
        + (f": {num(n)} files" if not merge else ""))
    return n


def _local_fingerprint() -> str:
    h = hashlib.sha256()
    for f in catalog_files():
        st = os.stat(f)
        h.update(f"{f}:{st.st_size}:{int(st.st_mtime)}".encode())
    return h.hexdigest()


def push(snapshot: bool = False, force: bool = False, repo: str = "") -> Dict[str, Any]:
    state = _read_state()
    with tempfile.TemporaryDirectory() as tmp:
        remote = _download(LATEST_TAG, tmp)
    if remote and not force and remote["bundle_sha256"] != state.get("bundle_sha256"):
        raise RuntimeError(
            f"the published catalog ({remote['created_utc']}) is not the one this checkout last pulled or pushed. "
            "Run `umdb catalog pull --merge` first (or push --force to overwrite it)")
    manifest = pack()
    files = [os.path.join(OUT_DIR, BUNDLE), os.path.join(OUT_DIR, MANIFEST)]
    tags = [LATEST_TAG]
    if snapshot:
        tags.append("catalog-" + manifest["created_utc"][:10].replace("-", "."))
    extra = ["--repo", repo] if repo else []
    for tag in tags:
        exists = _gh("release", "view", tag, *extra, check=False).returncode == 0
        if not exists:
            notes = ("Harvested UMDB catalog: JSON Lines run records, NCBI BioSample/BioProject caches and "
                     "seen lists, as one tar.gz with a manifest of SHA-256 checksums. Restore with "
                     "`umdb catalog pull" + ("" if tag == LATEST_TAG else f" --tag {tag}") + "`. "
                     "Licensed CC BY-NC-SA 4.0.")
            _gh("release", "create", tag, *extra, "--title", f"UMDB catalog ({tag})", "--notes", notes,
                "--latest=false", *(["--prerelease"] if tag == LATEST_TAG else []))
        _gh("release", "upload", tag, *files, "--clobber", *extra)
        log(f"Uploaded to release {paint(tag, 'key')}")
    _write_state(bundle_sha256=manifest["bundle_sha256"], pushed_utc=manifest["created_utc"],
                 local_sha256=_local_fingerprint())
    return manifest


def status() -> Dict[str, Any]:
    files = catalog_files()
    state = _read_state()
    local_bytes = sum(os.path.getsize(f) for f in files)
    log(f"Local catalog: {num(len(files))} files, {local_bytes / 1e6:.0f} MB under {DATA_DIR}/")
    if state:
        changed = state.get("local_sha256") != _local_fingerprint()
        log(f"Last pulled: {state.get('pulled_utc', 'never')}  last pushed: {state.get('pushed_utc', 'never')}  "
            f"local changes since: {paint('yes', 'warn') if changed else 'no'}")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            remote = _download(LATEST_TAG, tmp)
        if remote:
            same = remote["bundle_sha256"] == state.get("bundle_sha256")
            log(f"Published ({LATEST_TAG}): {remote['created_utc']}, {remote['bundle_bytes'] / 1e6:.0f} MB, "
                f"{len(remote['files'])} files" + ("" if same else paint("  (differs from the last pull/push)", "warn")))
        else:
            log(f"Published: none yet (release {LATEST_TAG} not found)")
    except RuntimeError as exc:
        warn(f"Could not check the published catalog: {exc}")
    return state
