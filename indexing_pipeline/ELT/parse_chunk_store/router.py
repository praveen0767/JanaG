#!/usr/bin/env python3
"""
Router that enumerates raw files (S3 or local FS), dispatches to format-specific
parsers, records manifests, and ensures idempotency.

Notes / fixes:
- Normalizes STORAGE env for downstream parser modules: router accepts 's3' or 'fs'
  but parser modules expect 's3' or 'local'. We set os.environ["STORAGE"] = 'local'
  when router STORAGE == 'fs' so pdf/html modules import cleanly (prevents pdf being skipped).
- load_local_module searches for both "<name>.py" and "_<name>.py" as before.
- Improved FS listing to include RAW_PREFIX as fallback, and safer path handling.
- Deterministic manifest writes (sort_keys=True) and consistent logging.
"""
from __future__ import annotations
import os
import sys
import json
import time
import hashlib
import logging
import traceback
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger("router")
handler = logging.StreamHandler(sys.stdout)
fmt = logging.Formatter('{"ts":"%(asctime)s","level":"%(levelname)s","event":"%(message)s"}', "%Y-%m-%dT%H:%M:%S")
handler.setFormatter(fmt)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def log(level: str, event: str, **extra):
    o = {"ts": now_ts(), "level": level, "event": event}
    o.update(extra)
    line = json.dumps(o, ensure_ascii=False)
    if level.lower() in ("error", "err", "critical"):
        print(line, file=sys.stderr, flush=True)
    else:
        print(line, flush=True)

# Router-level configuration
STORAGE = os.getenv("STORAGE", "s3").strip().lower()   # accepted: 's3' or 'fs'
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
AWS_REGION = os.getenv("AWS_REGION", "").strip()
RAW_PREFIX = (os.getenv("RAW_PREFIX") or os.getenv("STORAGE_RAW_PREFIX") or "data/raw/").rstrip("/") + "/"
CHUNKED_PREFIX = (os.getenv("CHUNKED_PREFIX") or os.getenv("STORAGE_CHUNKED_PREFIX") or "data/chunked/").rstrip("/") + "/"
PARSER_VERSION = os.getenv("PARSER_VERSION", "router-v1")
FORCE_PROCESS = os.getenv("FORCE_PROCESS", "false").lower() == "true"

# Accept both "fs" (router-friendly) and "local" (parser modules)
if STORAGE not in ("s3", "fs", "local"):
    log("error", "config", msg=f"Unsupported STORAGE='{STORAGE}'. Use 's3' or 'fs' (or 'local').")
    sys.exit(2)

# For downstream parser modules (pdf.py, _html.py, etc.) expect STORAGE to be 's3' or 'local'.
# If router was configured 'fs', export STORAGE='local' so those modules validate correctly.
if STORAGE == "fs":
    os.environ["STORAGE"] = "local"
else:
    os.environ["STORAGE"] = STORAGE

# Validate S3 config when needed
if os.environ["STORAGE"] == "s3" and not S3_BUCKET:
    log("error", "config", msg="S3_BUCKET must be set when STORAGE='s3'")
    sys.exit(2)

class StorageBackend:
    def list_raw(self, prefix: str) -> List[str]:
        raise NotImplementedError
    def exists(self, path: str) -> bool:
        raise NotImplementedError
    def open_read(self, path: str):
        raise NotImplementedError
    def write_bytes_atomic(self, path: str, data: bytes) -> None:
        raise NotImplementedError
    def write_text_atomic(self, path: str, text: str) -> None:
        raise NotImplementedError

class S3Backend(StorageBackend):
    def __init__(self, bucket: str, region: Optional[str] = None):
        try:
            import boto3
        except Exception as e:
            log("error", "import", msg="boto3 required for s3 STORAGE backend", error=str(e))
            raise SystemExit(2)
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region or None)

    def list_raw(self, prefix: str) -> List[str]:
        out: List[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix.lstrip("/")):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key and not key.endswith(".manifest.json"):
                    out.append(key)
        return out

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def open_read(self, key: str):
        resp = self.s3.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    def write_bytes_atomic(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def write_text_atomic(self, key: str, text: str) -> None:
        self.write_bytes_atomic(key, text.encode("utf-8"))

class FSBackend(StorageBackend):
    def __init__(self, root_fs_prefix: str = ""):
        # root_fs_prefix unused for now; using absolute/relative paths as-is
        self.root_prefix = root_fs_prefix.rstrip("/") + "/" if root_fs_prefix else ""

    def _full(self, rel: str) -> str:
        # return normalized POSIX path (relative or absolute)
        return str(Path(rel).as_posix())

    def list_raw(self, prefix: str) -> List[str]:
        base = Path(prefix)
        out: List[str] = []
        if not base.exists():
            # Try prefix relative to cwd
            base = Path(".") / prefix
            if not base.exists():
                return out
        for p in base.rglob("*"):
            if p.is_file() and not p.name.endswith(".manifest.json"):
                # return path relative to cwd for consistent keys
                try:
                    out.append(str(p.relative_to(Path(".")).as_posix()))
                except Exception:
                    out.append(str(p.as_posix()))
        return out

    def exists(self, relpath: str) -> bool:
        return Path(relpath).exists()

    def open_read(self, relpath: str):
        p = Path(relpath)
        if not p.exists():
            # try under RAW_PREFIX
            candidate = Path(RAW_PREFIX) / p.name
            if candidate.exists():
                p = candidate
        with p.open("rb") as f:
            return f.read()

    def write_bytes_atomic(self, relpath: str, data: bytes) -> None:
        p = Path(relpath)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(p)

    def write_text_atomic(self, relpath: str, text: str) -> None:
        self.write_bytes_atomic(relpath, text.encode("utf-8"))

# Instantiate backend
if os.environ["STORAGE"] == "s3":
    backend = S3Backend(S3_BUCKET, region=AWS_REGION or None)
    STORAGE_ROOT = f"s3://{S3_BUCKET}/"
else:
    backend = FSBackend()
    STORAGE_ROOT = ""

def compute_sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def save_manifest(key: str, manifest: Dict[str, Any]) -> bool:
    manifest_key = key + ".manifest.json"
    try:
        text = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
        backend.write_text_atomic(manifest_key, text)
        log("info", "saved_manifest", key=manifest_key, document_id=manifest.get("document_id"))
        return True
    except Exception as e:
        log("error", "save_manifest_failed", key=manifest_key, error=str(e))
        return False

def detect_ext_from_key(key: str) -> str:
    p = key.split("?", 1)[0].split("#", 1)[0]
    ext = Path(p).suffix.lstrip(".").lower()
    return ext

FORMAT_MAP = {
    "pdf": "pdf",
    "html": "html", "htm": "html",
    "jpg": "images", "jpeg": "images", "png": "images", "webp": "images",
    "tif": "images", "tiff": "images"
}

MODULE_CACHE: Dict[str, Any] = {}

def make_fallback_parser(module_name: str, trace: Optional[str] = None):
    def parse_file(key: str, manifest: Dict[str, Any]) -> Dict[str, Any]:
        err = {
            "error": f"fallback_parser_invoked_for_{module_name}",
            "traceback": trace or "no_trace",
            "saved_chunks": 0
        }
        manifest.update(err)
        return {"saved_chunks": 0}
    ns = type("ns", (), {})()
    setattr(ns, "parse_file", parse_file)
    return ns

def load_local_module(mod_name: str):
    """
    Load module from same directory as this router file. Try <mod_name>.py then _<mod_name>.py.
    Cache loaded modules. On import errors return fallback parser (with traceback in manifest).
    """
    if mod_name in MODULE_CACHE:
        return MODULE_CACHE[mod_name]
    candidates = [f"{mod_name}.py", f"_{mod_name}.py"]
    base = Path(__file__).resolve().parent
    for cand in candidates:
        mod_path = base / cand
        if mod_path.exists():
            spec = importlib.util.spec_from_file_location(f"local_elts_{mod_name}", str(mod_path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                try:
                    # Important: module import may read os.environ["STORAGE"] so we ensured it above.
                    spec.loader.exec_module(mod)  # type: ignore
                    MODULE_CACHE[mod_name] = mod
                    log("info", "module_loaded", module=mod_name, path=str(mod_path))
                    return MODULE_CACHE[mod_name]
                except Exception:
                    tb = traceback.format_exc()
                    log("error", "module_import_failed", module=mod_name, path=str(mod_path), traceback=tb)
                    MODULE_CACHE[mod_name] = make_fallback_parser(mod_name, tb)
                    return MODULE_CACHE[mod_name]
    MODULE_CACHE[mod_name] = make_fallback_parser(mod_name)
    log("warn", "module_missing", module=mod_name, path=str(base))
    return MODULE_CACHE[mod_name]

def is_already_processed(file_hash: str) -> bool:
    if FORCE_PROCESS:
        return False
    prefix = f"{CHUNKED_PREFIX}{file_hash}"
    try:
        if os.environ["STORAGE"] == "s3":
            paginator = backend.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=backend.bucket, Prefix=prefix.lstrip("/"), PaginationConfig={"MaxItems": 1}):
                if page.get("KeyCount", 0) > 0 or page.get("Contents"):
                    return True
            return False
        else:
            base = Path(CHUNKED_PREFIX)
            if not base.exists():
                return False
            for p in base.rglob(f"{file_hash}*"):
                if p.is_file():
                    return True
            return False
    except Exception:
        return False

def sniff_format_from_bytes(b: bytes) -> Optional[str]:
    head = b[:4096].lower()
    if head.startswith(b"%pdf"):
        return "pdf"
    if b"\x00" in head and not b"<html" in head:
        return None
    if b"<html" in head or b"<!doctype" in head or b"<body" in head or b"<div" in head:
        return "html"
    if head.startswith(b"\xff\xd8"):
        return "images"
    if head.startswith(b"\x89png"):
        return "images"
    if head.startswith(b"riff") and b"webp" in head:
        return "images"
    if head.startswith(b"ii") or head.startswith(b"mm"):
        return "images"
    return None

def main():
    run_id = os.getenv("RUN_ID") or str(int(time.time()))
    parser_version = PARSER_VERSION
    log("info", "startup", storage=os.environ["STORAGE"], raw_prefix=RAW_PREFIX, chunked_prefix=CHUNKED_PREFIX, run_id=run_id)
    try:
        keys = backend.list_raw(RAW_PREFIX.lstrip("/") if os.environ["STORAGE"] == "s3" else RAW_PREFIX)
    except Exception as e:
        log("error", "list_raw_failed", error=str(e))
        sys.exit(1)
    log("info", "scan", found=len(keys))
    for key in keys:
        s3_key = key
        try:
            if s3_key.endswith(".manifest.json"):
                continue
            try:
                raw_bytes = backend.open_read(s3_key)
            except Exception as e:
                tb = traceback.format_exc()
                manifest = {
                    "file_hash": None,
                    "s3_key": s3_key,
                    "pipeline_run_id": run_id,
                    "mime_ext": detect_ext_from_key(s3_key),
                    "timestamp": now_ts(),
                    "parser_version": parser_version,
                    "error": f"read_failed: {str(e)}",
                    "traceback": tb,
                }
                save_manifest(s3_key, manifest)
                log("error", "read_failed", key=s3_key, error=str(e))
                continue
            try:
                file_hash = compute_sha256_bytes(raw_bytes)
            except Exception as e:
                tb = traceback.format_exc()
                manifest = {
                    "file_hash": None,
                    "s3_key": s3_key,
                    "pipeline_run_id": run_id,
                    "mime_ext": detect_ext_from_key(s3_key),
                    "timestamp": now_ts(),
                    "parser_version": parser_version,
                    "error": f"hash_failed: {str(e)}",
                    "traceback": tb,
                }
                save_manifest(s3_key, manifest)
                log("error", "hash_failed", key=s3_key, error=str(e))
                continue

            if is_already_processed(file_hash):
                log("info", "skip_already_processed", file_hash=file_hash, key=s3_key)
                continue

            ext = detect_ext_from_key(s3_key)
            module_name = FORMAT_MAP.get(ext)
            if not module_name:
                sniff = sniff_format_from_bytes(raw_bytes)
                if sniff:
                    module_name = sniff
                else:
                    if ext in ("", "php"):
                        module_name = "html"
                    else:
                        log("warn", "unsupported_ext", key=s3_key, ext=ext)
                        module_name = "unsupported"

            # Ensure module import sees storage environment that parser expects (already set above).
            mod = load_local_module(module_name)

            ts = now_ts()
            manifest: Dict[str, Any] = {
                "file_hash": file_hash,
                "s3_key": s3_key,
                "pipeline_run_id": run_id,
                "mime_ext": ext,
                "timestamp": ts,
                "parser_version": parser_version,
            }

            try:
                result = mod.parse_file(s3_key, manifest)
                if not isinstance(result, dict) or "saved_chunks" not in result:
                    raise ValueError("parse_file must return dict with 'saved_chunks'")
            except Exception as e:
                tb = traceback.format_exc()
                manifest.setdefault("error", str(e))
                manifest.setdefault("traceback", tb)
                save_manifest(s3_key, manifest)
                log("error", "parse_failed", key=s3_key, error=str(e))
                continue

            saved = int(result.get("saved_chunks", 0) or 0)
            manifest["saved_chunks"] = saved
            if os.environ["STORAGE"] == "s3":
                manifest["s3_url"] = f"s3://{S3_BUCKET}/{s3_key}"
            else:
                manifest["local_path"] = s3_key
            save_manifest(s3_key, manifest)
            log("info", "parsed", key=s3_key, saved_chunks=saved, file_hash=file_hash)

        except Exception as exc:
            tb = traceback.format_exc()
            log("error", "outer_loop_exception", key=s3_key if 's3_key' in locals() else None, error=str(exc), traceback=tb)
            try:
                save_manifest(s3_key if 's3_key' in locals() else f"{RAW_PREFIX}unknown", {"file_hash": None, "s3_key": s3_key if 's3_key' in locals() else None, "error": str(exc), "traceback": tb})
            except Exception:
                pass
            continue

if __name__ == "__main__":
    main()
