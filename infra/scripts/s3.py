#!/usr/bin/env python3
# infra/scripts/sync_s3_with_local_fs.py
"""
Resilient S3 <-> local sync utility.

Key changes to avoid "stuck" behavior:
 - All boto3 S3 clients are created with a botocore Config that sets connect/read timeouts and limited retries.
 - S3 metadata (head_object) calls are executed via a bounded ThreadPoolExecutor and each call uses future.result(timeout=...)
   so a single slow network call cannot block the whole run.
 - list_remote_objects parallelizes head_object/info calls (bounded concurrency) and skips items that time out, logging warnings.
 - The main loop catches KeyboardInterrupt and shuts down thread pools cleanly.
 - Small, conservative defaults provided via env var S3_CALL_TIMEOUT (seconds) and reuse a shared executor.
This keeps behavior compatible with the original script while preventing the process from getting stuck on slow network ops.
"""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

try:
    import boto3
    import botocore
    from botocore.config import Config as BotoConfig
    import logging
except Exception as e:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(json.dumps({"ts": ts, "level": "ERROR", "event": "import_failure", "service": "s3sync", "msg": "missing dependency 'boto3'. Install: pip install boto3", "exception": str(e)}))
    raise SystemExit(2)

SERVICE_NAME = "s3sync"
DISABLE_THIRD_PARTY_LOGS = os.environ.get("DISABLE_THIRD_PARTY_LOGS", "false").lower() in ("1","true","yes")
if DISABLE_THIRD_PARTY_LOGS:
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("s3transfer").setLevel(logging.CRITICAL)

def ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def log(level: str, event: str, msg: str, **kwargs) -> None:
    o = {"ts": ts(), "level": level, "service": SERVICE_NAME, "event": event, "msg": msg}
    if kwargs:
        o.update(kwargs)
    print(json.dumps(o, default=str), flush=True)

def info(event: str, msg: str, **k): log("INFO", event, msg, **k)
def warn(event: str, msg: str, **k): log("WARN", event, msg, **k)
def error(event: str, msg: str, **k): log("ERROR", event, msg, **k)

def compute_hashes(path: str, chunk_size: int = 8 * 1024 * 1024) -> Tuple[str, str]:
    md5 = hashlib.md5()
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
            sha.update(chunk)
    return md5.hexdigest(), sha.hexdigest()

def _hex_from_base64(b64: str) -> Optional[str]:
    try:
        raw = base64.b64decode(b64)
        return raw.hex()
    except Exception:
        return None

def _normalize_etag(etag: str) -> str:
    if not etag:
        return ""
    e = etag.strip()
    if e.startswith("W/"):
        e = e[2:]
    e = e.strip('"').strip("'")
    if e.startswith("0x") or e.startswith("0X"):
        e = e[2:]
    return e.lower()

def run(cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        raise RuntimeError(f"Command not found: {cmd[0]}. Install AWS CLI or provide key/env.")
    out = (proc.stdout or "").strip()
    er = (proc.stderr or "").strip()
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout: {out}\nstderr: {er}")
    return proc.returncode, out, er

DEFAULT_PREFIX = os.environ.get("DEFAULT_PREFIX", "data")
LOCAL_BASE = os.environ.get("LOCAL_BASE", "data")
DEFAULT_CONCURRENCY = int(os.environ.get("CONCURRENT_FILES", "4"))
VERIFY_META_RETRIES = int(os.environ.get("VERIFY_META_RETRIES", "3"))
VERIFY_META_SLEEP = float(os.environ.get("VERIFY_META_SLEEP", "0.7"))

# New env var: per-S3-call timeout (seconds)
S3_CALL_TIMEOUT = float(os.environ.get("S3_CALL_TIMEOUT", "10.0"))

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_PROFILE = os.environ.get("AWS_PROFILE")

def validate_auth_preconditions():
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        info("auth", "Using AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY")
        return
    if AWS_PROFILE:
        info("auth", "Using AWS_PROFILE for credentials", profile=AWS_PROFILE)
        return
    info("auth", "No explicit AWS_ACCESS_KEY_ID/SECRET set; relying on boto3 credential chain")
    return

def get_s3_client() -> "botocore.client.S3":
    """
    Create an S3 client with conservative timeouts and retry policy to avoid hanging.
    """
    config = BotoConfig(
        connect_timeout=5,
        read_timeout=max(8, int(S3_CALL_TIMEOUT)),  # read timeout must be >= call timeout in practice
        retries={"max_attempts": 3, "mode": "standard"},
    )
    kwargs: Dict[str, Any] = {"config": config}
    if AWS_REGION:
        kwargs["region_name"] = AWS_REGION
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if AWS_PROFILE and not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        session = boto3.Session(profile_name=AWS_PROFILE, **({"region_name": AWS_REGION} if AWS_REGION else {}))
        return session.client("s3", **kwargs)
    return boto3.client("s3", **kwargs)

# Shared executor for S3 info/head operations (bounded by DEFAULT_CONCURRENCY)
S3_INFO_WORKERS = max(2, DEFAULT_CONCURRENCY)
_S3_THREADPOOL: Optional[ThreadPoolExecutor] = None
def s3_threadpool() -> ThreadPoolExecutor:
    global _S3_THREADPOOL
    if _S3_THREADPOOL is None:
        _S3_THREADPOOL = ThreadPoolExecutor(max_workers=S3_INFO_WORKERS)
    return _S3_THREADPOOL

class S3Fs:
    def __init__(self, client):
        self.client = client

    def _parse(self, full: str) -> Tuple[str, Optional[str]]:
        parts = full.split("/", 1)
        if len(parts) == 1:
            return parts[0], None
        return parts[0], parts[1]

    def find(self, root: str) -> List[str]:
        bucket, prefix = self._parse(root)
        prefix = (prefix or "").lstrip("/")
        out: List[str] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            # Use a conservative page size to avoid extremely long single calls
            page_it = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}) if prefix else paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 1000})
            for page in page_it:
                # defensive: page might be large; iterate entries quickly
                for obj in page.get("Contents", []) or []:
                    out.append(f"{bucket}/{obj['Key']}")
        except Exception as e:
            warn("find_failed", "list_objects_v2 failed", root=root, exception=str(e))
        return out

    def _head_object_with_timeout(self, bucket: str, key: str, timeout: float) -> Optional[Dict[str, Any]]:
        """
        Perform head_object but wrap in threadpool to enforce a hard timeout per call.
        Returns the boto3 response dict or None on timeout/error.
        """
        def _call():
            return self.client.head_object(Bucket=bucket, Key=key)
        fut = s3_threadpool().submit(_call)
        try:
            resp = fut.result(timeout=timeout)
            return resp
        except TimeoutError:
            warn("head_object_timeout", "head_object timed out", bucket=bucket, key=key, timeout=timeout)
            return None
        except Exception as e:
            warn("head_object_failed", "head_object failed", bucket=bucket, key=key, exception=str(e))
            return None

    def info(self, full: str) -> Dict:
        bucket, key = self._parse(full)
        if not key:
            return {}
        try:
            resp = self._head_object_with_timeout(bucket, key, S3_CALL_TIMEOUT)
            if not resp:
                return {}
            meta = resp.get("Metadata", {}) or {}
            etag = resp.get("ETag", "")
            size = int(resp.get("ContentLength", 0) or 0)
            content_md5 = None
            if "ContentMD5" in resp:
                content_md5 = resp.get("ContentMD5")
            info_obj = {"name": key, "path": full, "size": size, "etag": etag, "metadata": meta}
            if content_md5:
                info_obj["content_md5"] = content_md5
            return info_obj
        except Exception as e:
            warn("info_failed", "head_object failed", object=full, exception=str(e))
            return {}

    def put(self, local_path: str, full_remote_path: str, metadata: Optional[Dict[str,str]] = None, content_type: str = "application/octet-stream"):
        bucket, key = self._parse(full_remote_path)
        if not key:
            raise ValueError("remote path must include object key")
        try:
            extra = {"Metadata": metadata or {}, "ContentType": content_type}
            # upload_file uses high-level transfer; rely on botocore timeouts configured earlier
            self.client.upload_file(local_path, bucket, key, ExtraArgs=extra)
        except Exception as e:
            raise

    def get(self, full_remote_path: str, local_target: str):
        bucket, key = self._parse(full_remote_path)
        if not key:
            raise ValueError("remote path must include object key")
        target_path = Path(local_target)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(bucket, key, str(target_path))
        except Exception as e:
            raise

    def rm(self, full_remote_path: str):
        bucket, key = self._parse(full_remote_path)
        if not key:
            try:
                self.client.delete_bucket(Bucket=bucket)
            except Exception as e:
                warn("rm_failed", "delete_bucket failed", container=bucket, exception=str(e))
            return
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            warn("rm_failed", "delete_object failed", object=full_remote_path, exception=str(e))

    def setxattrs(self, full_remote_path: str, metadata: Dict[str,str]):
        bucket, key = self._parse(full_remote_path)
        if not key:
            return False
        try:
            copy_source = {"Bucket": bucket, "Key": key}
            head = self._head_object_with_timeout(bucket, key, S3_CALL_TIMEOUT)
            if not head:
                warn("setxattrs_head_missing", "Failed head in setxattrs", object=full_remote_path)
                return False
            content_type = head.get("ContentType")
            extra = {"Metadata": metadata or {}, "MetadataDirective": "REPLACE"}
            if content_type:
                extra["ContentType"] = content_type
            self.client.copy_object(Bucket=bucket, Key=key, CopySource=copy_source, **extra)
            return True
        except Exception as e:
            warn("setxattrs_failed", "copy_object(REPLACE metadata) failed", object=full_remote_path, exception=str(e))
            return False

def get_fs(_protocol: Optional[str] = None):
    client = get_s3_client()
    fs = S3Fs(client)
    return fs, "s3-sdk"

def safe_rel_normalize(p: str) -> str:
    return p.replace("\\", "/").lstrip("/")

def join_remote(bucket: str, prefix: str, rel: str) -> str:
    reln = safe_rel_normalize(rel)
    if prefix:
        p = prefix.strip("/").rstrip("/")
        key = f"{p}/{reln}"
    else:
        key = reln
    return f"{bucket}/{key}"

def list_remote_objects(fs: S3Fs, bucket: str, prefix: str) -> List[Tuple[str, str, int, Dict]]:
    """
    List remote objects under bucket/prefix and fetch head_object metadata in parallel with per-call timeouts.
    Returns a list of tuples: (full, rel, size, info_obj)
    """
    prefix_clean = prefix.strip("/").rstrip("/")
    root = f"{bucket}/{prefix_clean}" if prefix_clean else f"{bucket}"
    out: List[Tuple[str,str,int,Dict]] = []
    found = fs.find(root)
    if not found:
        return out

    # Parallelize info() calls (bounded)
    pool = s3_threadpool()
    future_map = {}
    for full in sorted(found):
        fut = pool.submit(fs.info, full)
        future_map[fut] = full

    for fut in as_completed(future_map):
        full = future_map[fut]
        try:
            info_obj = fut.result(timeout=S3_CALL_TIMEOUT + 2)  # small buffer
        except TimeoutError:
            warn("info_timeout", "Fetching metadata timed out for object, skipping", object=full, timeout=S3_CALL_TIMEOUT)
            continue
        except Exception as e:
            warn("info_failed", "Fetching metadata failed; skipping", object=full, exception=str(e))
            continue
        if not info_obj:
            continue
        lead = f"{bucket}/"
        rel = full[len(lead):] if full.startswith(lead) else full
        if prefix_clean:
            if rel.startswith(prefix_clean + "/"):
                rel = rel[len(prefix_clean)+1:]
            elif rel == prefix_clean:
                rel = ""
        rel = safe_rel_normalize(rel)
        size = int(info_obj.get("size", 0) or 0)
        out.append((full, rel, size, info_obj))
    return out

def extract_remote_values(info_obj: Dict) -> Dict[str, Optional[str]]:
    meta = (info_obj.get("metadata") or {}) if isinstance(info_obj, dict) else {}
    metadata_sha = None
    for k in ("sha256","SHA256","Sha256"):
        if meta.get(k):
            metadata_sha = meta.get(k)
            break
    content_md5 = info_obj.get("content_md5") or None
    etag = info_obj.get("etag") or ""
    return {"metadata_sha256": metadata_sha, "content_md5": content_md5, "etag": etag, "raw_info": info_obj}

def upload_file_fs(fs: S3Fs, local_path: str, full_remote_path: str, sha256: Optional[str], content_type: str = "application/octet-stream", dry_run: bool = False, verify_retries: int = VERIFY_META_RETRIES):
    if dry_run:
        return {"rel_path": full_remote_path, "action": "dry_run"}
    metadata = {"sha256": sha256} if sha256 else {}
    fs.put(local_path, full_remote_path, metadata=metadata, content_type=content_type)
    try:
        fs.setxattrs(full_remote_path, metadata)
    except Exception:
        pass
    for attempt in range(1, verify_retries+1):
        try:
            info_obj = fs.info(full_remote_path)
            meta = (info_obj.get("metadata") or {}) if isinstance(info_obj, dict) else {}
            remote_sha = meta.get("sha256") or meta.get("SHA256") or meta.get("Sha256")
            if sha256 and remote_sha == sha256:
                return {"rel_path": full_remote_path, "action": "uploaded", "verified": True}
        except Exception:
            pass
        time.sleep(VERIFY_META_SLEEP)
    return {"rel_path": full_remote_path, "action": "uploaded", "verified": False}

def download_file_fs(fs: S3Fs, full_remote_path: str, local_target: str, dry_run: bool = False):
    if dry_run:
        return {"rel_path": full_remote_path, "action": "dry_run"}
    fs.get(full_remote_path, local_target)
    return {"rel_path": full_remote_path, "action": "downloaded"}

def delete_remote_file_fs(fs: S3Fs, full_remote_path: str, dry_run: bool = False):
    if dry_run:
        return full_remote_path
    fs.rm(full_remote_path)
    return full_remote_path

def list_local_files(base_dir: str) -> List[Tuple[str,str]]:
    base = Path(base_dir)
    if not base.exists():
        return []
    out: List[Tuple[str,str]] = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            try:
                rel = p.relative_to(base).as_posix()
            except Exception:
                rel = p.name
            out.append((str(p.resolve()), safe_rel_normalize(rel)))
    return out

def safe_remove_local(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except PermissionError:
        try:
            os.chmod(path, stat.S_IWUSR | stat.S_IRUSR)
            os.remove(path)
            return True
        except Exception as e:
            warn("delete_local_perm_failed", "chmod+delete failed", path=path, error=str(e))
            return False
    except FileNotFoundError:
        return True
    except Exception as e:
        warn("delete_local_failed", "delete local failed", path=path, error=str(e))
        return False

def should_skip_upload(local_path: str, remote_info: Optional[Dict], verbose: bool = False) -> Tuple[bool,str]:
    if not remote_info:
        return False, "remote_missing"
    try:
        local_size = Path(local_path).stat().st_size
    except Exception:
        local_size = None
    remote_meta_sha = remote_info.get("metadata_sha256")
    remote_etag = (remote_info.get("etag") or "") or ""
    remote_content_md5 = remote_info.get("content_md5")
    if remote_meta_sha:
        try:
            _, local_sha = compute_hashes(local_path)
            if local_sha == remote_meta_sha:
                return True, "match_metadata_sha256"
            return False, "metadata_sha256_mismatch"
        except Exception as e:
            return False, f"local_hash_failed:{e}"
    if remote_content_md5:
        try:
            local_md5, _ = compute_hashes(local_path)
            if local_md5 == remote_content_md5:
                return True, "match_content_md5_hex"
            hex_from_b64 = _hex_from_base64(remote_content_md5)
            if hex_from_b64 and local_md5 == hex_from_b64:
                return True, "match_content_md5_base64"
            return False, "content_md5_mismatch"
        except Exception as e:
            return False, f"local_hash_failed:{e}"
    if local_size is not None:
        remote_size = int(remote_info.get("size", 0) or 0)
        if local_size == remote_size and remote_etag:
            norm = _normalize_etag(remote_etag)
            if all(c in "0123456789abcdef" for c in norm) and len(norm) == 32:
                try:
                    local_md5, _ = compute_hashes(local_path)
                    if local_md5 == norm:
                        return True, "match_etag_md5"
                    return False, "etag_mismatch"
                except Exception as e:
                    return False, f"local_hash_failed:{e}"
    return False, "no_reliable_remote_checksum"

def should_skip_download(local_path: str, remote_info: Dict) -> bool:
    try:
        if not Path(local_path).exists():
            return False
        local_size = Path(local_path).stat().st_size
    except Exception:
        return False
    remote_meta_sha = remote_info.get("metadata_sha256")
    remote_etag = (remote_info.get("etag") or "") or ""
    remote_content_md5 = remote_info.get("content_md5")
    if remote_meta_sha:
        try:
            _, local_sha = compute_hashes(local_path)
            return local_sha == remote_meta_sha
        except Exception:
            return False
    if remote_content_md5:
        try:
            local_md5, _ = compute_hashes(local_path)
            if local_md5 == remote_content_md5:
                return True
            hex_from_b64 = _hex_from_base64(remote_content_md5)
            if hex_from_b64 and local_md5 == hex_from_b64:
                return True
        except Exception:
            return False
    remote_size = int(remote_info.get("size", 0) or 0)
    if local_size == remote_size and remote_etag:
        norm = _normalize_etag(remote_etag)
        if all(c in "0123456789abcdef" for c in norm) and len(norm) == 32:
            try:
                local_md5, _ = compute_hashes(local_path)
                return local_md5 == norm
            except Exception:
                return False
    return False

def upload_directory(base_dir: str, bucket: str, prefix: str, concurrency: int, dry_run: bool = False, verbose: bool = False, delete_orphans: bool = True):
    info("upload_start", "Upload mirror starting", local=base_dir, bucket=bucket, prefix=prefix, concurrency=concurrency, delete_orphans=delete_orphans)
    fs, proto = get_fs(None)
    local_entries = list_local_files(base_dir)
    local_rel_map = {rel: abs_path for abs_path, rel in local_entries}

    # Fetch remote listing (with resilient metadata fetch)
    remote_entries = list_remote_objects(fs, bucket, prefix)
    remote_map: Dict[str, Dict] = {}
    for full, rel, size, info_obj in remote_entries:
        vals = extract_remote_values(info_obj)
        vals["full"] = full
        vals["size"] = size
        remote_map[safe_rel_normalize(rel)] = vals

    remote_rels = set(remote_map.keys())
    local_rels = set(local_rel_map.keys())
    stale_remote = sorted(remote_rels - local_rels)
    info("delete_orphans", "Deleting remote orphans (if enabled)", orphan_count=len(stale_remote), delete_orphans=delete_orphans)
    if delete_orphans and stale_remote:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = {ex.submit(delete_remote_file_fs, fs, remote_map[rel]["full"], dry_run): rel for rel in stale_remote}
            try:
                for fut in as_completed(futures):
                    rel = futures[fut]
                    try:
                        key = fut.result()
                        info("deleted_remote_orphan", "Deleted remote orphan", rel=rel, remote=key)
                    except Exception as e:
                        warn("delete_orphan_failed", "Failed deleting remote orphan", rel=rel, error=str(e))
            except KeyboardInterrupt:
                warn("abort", "Interrupted during orphan deletion; cancelling remaining tasks")
                for fut in futures:
                    fut.cancel()
                raise

    # Refresh remote map after deletes
    remote_entries = list_remote_objects(fs, bucket, prefix)
    remote_map = {}
    for full, rel, size, info_obj in remote_entries:
        vals = extract_remote_values(info_obj)
        vals["full"] = full
        vals["size"] = size
        remote_map[safe_rel_normalize(rel)] = vals

    successes = skipped = failed = 0
    errors: List[str] = []
    tasks = {}
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for rel in sorted(local_rel_map.keys()):
            local_path = local_rel_map[rel]
            remote_info = remote_map.get(rel)
            try:
                skip, reason = should_skip_upload(local_path, remote_info, verbose=verbose)
            except Exception as e:
                warn("skip_check_failed", "Checksum decision failed; will upload", rel=rel, error=str(e))
                skip, reason = False, "skip_check_error"
            if skip:
                skipped += 1
                info("skipped_upload", "Skipped upload (unchanged)", rel=rel, local=local_path, reason=reason)
                continue
            try:
                _, local_sha = compute_hashes(local_path)
            except Exception as e:
                warn("hash_failed", "Failed computing hashes; will upload without sha metadata", rel=rel, error=str(e))
                local_sha = None
            full_remote = join_remote(bucket, prefix, rel)
            tasks[ex.submit(upload_file_fs, fs, local_path, full_remote, local_sha, "application/octet-stream", dry_run)] = (rel, local_path, full_remote, local_sha)
        try:
            for fut in as_completed(tasks):
                rel, local_path, full_remote, sha256 = tasks[fut]
                try:
                    result = fut.result()
                    action = result.get("action")
                    verified = result.get("verified", False)
                    if action == "dry_run":
                        info("upload_dryrun", "Dry-run would upload", rel=rel, remote=full_remote, sha256=sha256)
                    else:
                        successes += 1
                        info("uploaded", "Uploaded file", rel=rel, remote=full_remote, verified=bool(verified), sha256=sha256)
                except Exception as e:
                    failed += 1
                    errors.append(f"{rel}: {e}")
                    warn("upload_failed", "Upload failed", rel=rel, error=str(e))
        except KeyboardInterrupt:
            warn("abort", "Interrupted during uploads; cancelling remaining tasks")
            for fut in tasks:
                fut.cancel()
            raise

    info("upload_finished", "Upload finished", succeeded=successes, skipped=skipped, failed=failed)
    for e in errors[:20]:
        warn("upload_error", "Upload error detail", detail=e)

def download_directory(bucket: str, base_dir: str, prefix: str, concurrency: int, dry_run: bool = False, verbose: bool = False, delete_orphans: bool = True):
    info("download_start", "Download mirror starting", bucket=bucket, local=base_dir, prefix=prefix, concurrency=concurrency, delete_orphans=delete_orphans)
    fs, proto = get_fs(None)
    remote_entries = list_remote_objects(fs, bucket, prefix)
    remote_map: Dict[str, Dict] = {}
    for full, rel, size, info_obj in remote_entries:
        vals = extract_remote_values(info_obj)
        vals["full"] = full
        vals["size"] = size
        remote_map[safe_rel_normalize(rel)] = vals

    local_entries = list_local_files(base_dir)
    local_rel_map = {rel: abs_path for abs_path, rel in local_entries}

    remote_rels = set(remote_map.keys())
    local_rels = set(local_rel_map.keys())
    stale_local = sorted(local_rels - remote_rels)
    info("delete_local_orphans", "Deleting local orphans (if enabled)", orphan_count=len(stale_local), delete_orphans=delete_orphans)
    if delete_orphans and stale_local:
        for rel in stale_local:
            path = local_rel_map[rel]
            try:
                if dry_run:
                    info("delete_local_dryrun", "Would delete local orphan", rel=rel, path=path)
                    continue
                ok = safe_remove_local(path)
                if ok:
                    info("deleted_local_orphan", "Deleted local orphan", rel=rel, path=path)
                else:
                    warn("delete_local_failed", "Failed to delete local orphan", rel=rel, path=path)
            except Exception as e:
                warn("delete_local_failed", "Failed to delete local orphan", rel=rel, error=str(e))

    local_entries = list_local_files(base_dir)
    local_rel_map = {rel: abs_path for abs_path, rel in local_entries}

    successes = skipped = failed = 0
    errors: List[str] = []
    tasks = {}
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for rel in sorted(remote_map.keys()):
            rinfo = remote_map[rel]
            full = rinfo["full"]
            local_path = str(Path(base_dir) / rel)
            try:
                if should_skip_download(local_path, rinfo):
                    skipped += 1
                    info("skipped_download", "Skipped download (unchanged)", rel=rel, local=local_path)
                    continue
            except Exception as e:
                warn("skip_download_failed", "Checksum decision failed for download; will attempt download", rel=rel, error=str(e))
            tasks[ex.submit(download_file_fs, fs, full, local_path, dry_run)] = rel
        try:
            for fut in as_completed(tasks):
                rel = tasks[fut]
                try:
                    result = fut.result()
                    if result.get("action") == "dry_run":
                        info("download_dryrun", "Dry-run would download", rel=rel)
                    else:
                        successes += 1
                        info("downloaded", "Downloaded file", rel=rel)
                except Exception as e:
                    failed += 1
                    errors.append(f"{rel}: {e}")
                    warn("download_failed", "Download failed", rel=rel, error=str(e))
        except KeyboardInterrupt:
            warn("abort", "Interrupted during downloads; cancelling remaining tasks")
            for fut in tasks:
                fut.cancel()
            raise
    info("download_finished", "Download finished", succeeded=successes, skipped=skipped, failed=failed)
    for e in errors[:20]:
        warn("download_error", "Download error detail", detail=e)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deterministic mirror sync local <-> S3 (metadata sha256 preferred).")
    gp = p.add_mutually_exclusive_group(required=True)
    gp.add_argument("--upload", action="store_true", help="Mirror local -> S3 (skip unchanged, delete remote orphans)")
    gp.add_argument("--download", action="store_true", help="Mirror S3 -> local (skip unchanged, delete local orphans)")
    gp.add_argument("--merge-upload", action="store_true", help="Merge upload: upload changed files only, DO NOT delete remote orphans")
    gp.add_argument("--merge-download", action="store_true", help="Merge download: download changed files only, DO NOT delete local orphans")
    p.add_argument("--max-concurrency", type=int, default=0, help="Override concurrency (0 = auto/env)")
    p.add_argument("--dry-run", action="store_true", help="Do not perform state-changing operations; print actions only")
    p.add_argument("--verbose", action="store_true", help="Emit additional debug logs")
    return p.parse_args()

def compute_concurrency(override: int = 0) -> int:
    if override and override > 0:
        return max(1, override)
    return max(1, DEFAULT_CONCURRENCY)

def main() -> None:
    args = parse_args()
    bucket = S3_BUCKET
    if not bucket:
        error("missing_env", "S3_BUCKET env variable is not set")
        raise SystemExit(2)

    validate_auth_preconditions()

    concurrency = compute_concurrency(args.max_concurrency)
    prefix = os.environ.get("DEFAULT_PREFIX", DEFAULT_PREFIX).strip("/")
    dry_run = args.dry_run
    verbose = args.verbose

    try:
        fs, proto = get_fs(None)
    except Exception as e:
        error("fs_init_failed", "Filesystem initialization failed", exception=str(e))
        raise SystemExit(3)

    try:
        probe = fs.find(bucket)
        info("fs_ok", "Filesystem initialized and bucket probe OK", protocol=proto, bucket=bucket, sample_count=len(probe))
    except Exception as e:
        warn("bucket_access", "Bucket may not exist or probe failed", bucket=bucket, error=str(e))

    try:
        if args.upload:
            upload_directory(LOCAL_BASE, bucket, prefix, concurrency, dry_run=dry_run, verbose=verbose, delete_orphans=True)
        elif args.download:
            download_directory(bucket, LOCAL_BASE, prefix, concurrency, dry_run=dry_run, verbose=verbose, delete_orphans=True)
        elif args.merge_upload:
            upload_directory(LOCAL_BASE, bucket, prefix, concurrency, dry_run=dry_run, verbose=verbose, delete_orphans=False)
        elif args.merge_download:
            download_directory(bucket, LOCAL_BASE, prefix, concurrency, dry_run=dry_run, verbose=verbose, delete_orphans=False)
        else:
            error("cli_usage", "Please specify --upload/--download/--merge-upload/--merge-download")
            raise SystemExit(1)
    except KeyboardInterrupt:
        warn("abort", "Interrupted by user; shutting down")
    finally:
        # Clean shutdown of shared threadpool if created
        global _S3_THREADPOOL
        if _S3_THREADPOOL:
            try:
                _S3_THREADPOOL.shutdown(wait=False)
            except Exception:
                pass

if __name__ == "__main__":
    main()
