#!/usr/bin/env python3
"""
Idempotent web scraper for the civic pipeline.

Design invariants (enforced here):
- Content-addressed storage: raw objects stored at <RAW_PREFIX>/<source_id>/by-hash/<sha>.<ext>
- Single authoritative manifest per canonical URL at <RAW_PREFIX>/<source_id>/latest/<doc_id>.manifest.json
- If manifest exists and FORCE_MANIFEST is false, the URL is skipped (no network/further writes).
- No timestamps in filepaths or directory names.
- Minimal deterministic metadata saved in manifest:
  file_hash, mime_ext, original_url, timestamp, storage_url, size_bytes, remote_etag, remote_lastmod, scraper_version.
- All writes are idempotent / guarded by HEAD checks on S3 (or local filesystem existence).
"""

from __future__ import annotations
import re
import os
import sys
import json
import time
import random
import hashlib
import tempfile
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Set, Tuple
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl
from concurrent.futures import ThreadPoolExecutor

import httpx
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

# optional playwright usage is allowed but not required
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # noqa: F401
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False

# --------------------------
# Environment (explicit)
# --------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "web_scraper_prod").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip() or None
AWS_REGION = os.getenv("AWS_REGION", "").strip() or None
SEED_URLS = [s.strip() for s in os.getenv("SEED_URLS", "").split(",") if s.strip()]
ALLOWED_DOMAINS = [d.strip().lower() for d in os.getenv("ALLOWED_DOMAINS", "").split(",") if d.strip()]
SKIP_WEB_SCRAPING = os.getenv("SKIP_WEB_SCRAPING", "false").lower() == "true"
MAX_TIME_IN_SECONDS = int(os.getenv("MAX_TIME_IN_SECONDS", os.getenv("MAX_RUNTIME_SECONDS", "120")))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "0.6"))
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "CivicBotScraper/1.0 (+mailto:ops@example.org)")
DEFAULT_CRAWL_DELAY = float(os.getenv("CRAWL_DELAY_SECONDS", "0.6"))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw/").rstrip("/") + "/"
RAW_PREFIX = os.getenv("RAW_PREFIX", "data/raw/").strip()
ALLOWED_EXTENSIONS = [e.strip().lower() for e in os.getenv("ALLOWED_EXTENSIONS", "pdf,doc,docx,xls,xlsx,csv,html,htm,json").split(",") if e.strip()]
MAX_CRAWL_DEPTH = int(os.getenv("MAX_CRAWL_DEPTH", "2"))
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "6"))
SCRAPER_VERSION = os.getenv("SCRAPER_VERSION", "web_scraper@1.0.0")
SPOOL_DIR = os.getenv("SPOOL_DIR", "/tmp/web_scraper_spool/")
MIN_CONTENT_WORDS = int(os.getenv("MIN_CONTENT_WORDS", "80"))
MIN_HTML_BYTES = int(os.getenv("MIN_HTML_BYTES", str(2 * 1024)))
MAX_CONTENT_BYTES = int(os.getenv("MAX_CONTENT_BYTES", str(250 * 1024 * 1024)))
FORCE_MANIFEST = os.getenv("FORCE_MANIFEST", "false").lower() == "true"

# quick guards
if SKIP_WEB_SCRAPING:
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"), "msg": "web_scraping_skipped", "reason": "SKIP_WEB_SCRAPING=true"}))
    sys.exit(0)
if not SEED_URLS:
    sys.stderr.write("FATAL: SEED_URLS environment variable must be set (comma-separated list)\n")
    sys.exit(2)
if not S3_BUCKET:
    sys.stderr.write("FATAL: S3_BUCKET environment variable must be set\n")
    sys.exit(2)

# --------------------------
# Logging
# --------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("web_scraper")

def iso_ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def jlog(event: str, **fields):
    rec = {"ts": iso_ts(), "event": event}
    rec.update(fields)
    # sort keys for deterministic logs
    log.info(json.dumps(rec, ensure_ascii=False, separators=(",", ":"), sort_keys=True))

# --------------------------
# Utilities
# --------------------------
def _compute_sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _canonicalize_url(url: str) -> str:
    p = urlparse(url)
    q = parse_qsl(p.query, keep_blank_values=True)
    q = [(k, v) for (k, v) in q if not (k.startswith("utm_") or k in ("fbclid", "gclid", "mc_cid", "mc_eid"))]
    q_sorted = "&".join(f"{k}={v}" for k, v in sorted(q))
    netloc = p.netloc.lower()
    rebuilt = urlunparse((p.scheme.lower() or "http", netloc, p.path or "/", "", q_sorted, ""))
    if rebuilt.endswith("/"):
        rebuilt = rebuilt[:-1]
    return rebuilt

def _ext_from_url_or_ct(url: str, content_type: Optional[str]) -> str:
    p = urlparse(url)
    name = Path(p.path).name.lower()
    if "." in name:
        return name.rsplit(".", 1)[-1]
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct == "application/pdf": return "pdf"
        if "msword" in ct or "word" in ct: return "doc"
        if ct.startswith("text/html"): return "html"
        if "json" in ct: return "json"
        if ct.startswith("image/"): return ct.split("/", 1)[1]
    return "bin"

def _safe_name_from_url(url: str, ext: str) -> str:
    name = Path(urlparse(url).path).name
    if not name:
        name = f"response.{ext}"
    name = "".join(c for c in name if c.isalnum() or c in ("-", "_", "."))
    return name[:240]

def _ensure_disk(min_free_bytes: int = 100 * 1024 * 1024) -> bool:
    try:
        total, used, free = shutil.disk_usage("/")
        return free >= min_free_bytes
    except Exception:
        return True

def backoff_sleep(base: float, attempt: int, cap: float = 60.0) -> None:
    jitter = random.uniform(0.8, 1.2)
    s = min(cap, base * (2 ** attempt)) * jitter
    time.sleep(s)

def _normalize_etag(et: Optional[str]) -> Optional[str]:
    if not et:
        return None
    et = et.strip()
    if et.startswith('"') and et.endswith('"'):
        et = et[1:-1]
    return et

# --------------------------
# Storage client (idempotent boundary)
# --------------------------
class StorageClient:
    """
    - upload_binary_by_hash: stores raw bytes under by-hash key; idempotent (head-check first)
    - write_latest_manifest: writes single manifest per doc_id (head-check first; no overwrite unless FORCE_MANIFEST)
    Note: spool names are deterministic (use sha) — no timestamps in paths.
    """
    def __init__(self, s3_bucket: Optional[str] = None, aws_region: Optional[str] = None):
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.s3 = None
        if s3_bucket:
            cfg = BotoConfig(retries={"max_attempts": 8, "mode": "standard"})
            session_args = {}
            if aws_region:
                session_args["region_name"] = aws_region
            self.s3 = boto3.client("s3", config=cfg, **session_args)
        self.spool_dir = SPOOL_DIR
        Path(self.spool_dir).mkdir(parents=True, exist_ok=True)
        Path(RAW_DATA_PATH).mkdir(parents=True, exist_ok=True)

    def preflight_s3_access(self) -> bool:
        if not self.s3:
            return True
        try:
            self.s3.head_bucket(Bucket=self.s3_bucket)
            jlog("s3_preflight_ok", bucket=self.s3_bucket)
            return True
        except Exception as e:
            jlog("s3_preflight_failed", bucket=self.s3_bucket, error=str(e))
            return False

    def _by_hash_key(self, source_id: str, sha: str, ext: str) -> str:
        # deterministic content-addressed path
        return f"{RAW_PREFIX.rstrip('/')}/{source_id}/by-hash/{sha}.{ext}"

    def _latest_manifest_key(self, source_id: str, doc_id: str) -> str:
        return f"{RAW_PREFIX.rstrip('/')}/{source_id}/latest/{doc_id}.manifest.json"

    def manifest_exists_latest(self, source_id: str, doc_id: str) -> Optional[str]:
        """
        Return S3 URL or local path if manifest already present (idempotency check).
        """
        key = self._latest_manifest_key(source_id, doc_id)
        if self.s3:
            try:
                self.s3.head_object(Bucket=self.s3_bucket, Key=key)
                return f"s3://{self.s3_bucket}/{key}"
            except ClientError:
                return None
            except Exception:
                return None
        else:
            local = os.path.join(RAW_DATA_PATH, source_id, "latest", f"{doc_id}.manifest.json")
            return local if os.path.exists(local) else None

    def read_latest_manifest(self, source_id: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Read and return manifest JSON dict if exists. None otherwise.
        """
        path_or_s3 = self.manifest_exists_latest(source_id, doc_id)
        if not path_or_s3:
            return None
        if self.s3:
            key = self._latest_manifest_key(source_id, doc_id)
            try:
                obj = self.s3.get_object(Bucket=self.s3_bucket, Key=key)
                return json.loads(obj["Body"].read().decode("utf-8"))
            except Exception:
                return None
        else:
            try:
                with open(path_or_s3, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                return None

    def upload_binary_by_hash(self, source_id: str, sha: str, ext: str, local_path: str) -> Dict[str, Any]:
        """
        Idempotent upload: HEAD first; if exists return metadata. On failure to upload to S3, spool locally.
        Returns dict: storage_url, storage_etag, size_bytes
        """
        key = self._by_hash_key(source_id, sha, ext)
        if self.s3:
            try:
                # HEAD first - idempotent check
                head = self.s3.head_object(Bucket=self.s3_bucket, Key=key)
                return {"storage_url": f"s3://{self.s3_bucket}/{key}", "storage_etag": head.get("ETag"), "size_bytes": head.get("ContentLength")}
            except ClientError:
                # not present -> upload
                try:
                    with open(local_path, "rb") as fh:
                        self.s3.upload_fileobj(fh, self.s3_bucket, key)
                    head = self.s3.head_object(Bucket=self.s3_bucket, Key=key)
                    return {"storage_url": f"s3://{self.s3_bucket}/{key}", "storage_etag": head.get("ETag"), "size_bytes": head.get("ContentLength")}
                except Exception as e:
                    # spool locally for later ingest if upload fails (deterministic spool name)
                    try:
                        spool_name = os.path.join(self.spool_dir, f"{sha}_{Path(local_path).name}")
                        shutil.copy(local_path, spool_name)
                        jlog("s3_upload_failed_spooled", bucket=self.s3_bucket, key=key, spool=spool_name, error=str(e))
                        return {"storage_url": spool_name, "storage_etag": None, "size_bytes": os.path.getsize(spool_name)}
                    except Exception as se:
                        jlog("s3_upload_and_spool_failed", bucket=self.s3_bucket, key=key, error=str(e), spool_error=str(se))
                        raise
            except Exception as e:
                jlog("s3_head_failed", bucket=self.s3_bucket, key=key, error=str(e))
                raise
        else:
            dest_dir = os.path.join(RAW_DATA_PATH, source_id, "by-hash")
            Path(dest_dir).mkdir(parents=True, exist_ok=True)
            dest_path = os.path.join(dest_dir, f"{sha}.{ext}")
            if os.path.exists(dest_path):
                return {"storage_url": dest_path, "storage_etag": None, "size_bytes": os.path.getsize(dest_path)}
            try:
                # move local tmp file into deterministic location (atomic on same fs)
                shutil.move(local_path, dest_path)
                return {"storage_url": dest_path, "storage_etag": None, "size_bytes": os.path.getsize(dest_path)}
            except Exception as e:
                # fallback to copy then remove
                try:
                    shutil.copy(local_path, dest_path)
                    return {"storage_url": dest_path, "storage_etag": None, "size_bytes": os.path.getsize(dest_path)}
                except Exception:
                    jlog("local_store_failed", src=local_path, dst=dest_path, error=str(e))
                    raise

    def write_latest_manifest(self, source_id: str, doc_id: str, manifest_obj: Dict[str, Any]) -> str:
        """
        Idempotent manifest write: if manifest exists and FORCE_MANIFEST is False, returns existing manifest path.
        Otherwise writes manifest and returns path.
        """
        key = self._latest_manifest_key(source_id, doc_id)
        body = json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        if self.s3:
            try:
                if not FORCE_MANIFEST:
                    try:
                        self.s3.head_object(Bucket=self.s3_bucket, Key=key)
                        return f"s3://{self.s3_bucket}/{key}"
                    except ClientError:
                        pass
                self.s3.put_object(Bucket=self.s3_bucket, Key=key, Body=body, ContentType="application/json")
                return f"s3://{self.s3_bucket}/{key}"
            except Exception as e:
                jlog("s3_manifest_put_failed", key=key, error=str(e))
                raise
        else:
            local_dir = os.path.join(RAW_DATA_PATH, source_id, "latest")
            Path(local_dir).mkdir(parents=True, exist_ok=True)
            local_path = os.path.join(local_dir, f"{doc_id}.manifest.json")
            if os.path.exists(local_path) and not FORCE_MANIFEST:
                return local_path
            tmp = local_path + ".tmp"
            try:
                with open(tmp, "wb") as fh:
                    fh.write(body)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, local_path)
                return local_path
            finally:
                if os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except Exception:
                        pass

# --------------------------
# Robots / HTML extraction (unchanged)
# --------------------------
class RobotsCache:
    def __init__(self, http: httpx.Client, ua: str):
        self._http = http
        self._ua = ua
        self._cache: Dict[str, Dict[str, Any]] = {}
    def _fetch(self, origin: str, robots_url: str) -> Dict[str, Any]:
        import urllib.robotparser
        rp = urllib.robotparser.RobotFileParser()
        try:
            resp = self._http.get(robots_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": self._ua})
            status = resp.status_code
            text = resp.text if status == 200 else ""
            if status == 200 and text:
                rp.parse(text.splitlines())
                cd = rp.crawl_delay(self._ua)
                return {"parser": rp, "status": status, "crawl_delay": cd}
            return {"parser": rp, "status": status, "crawl_delay": None}
        except Exception as e:
            jlog("robots_fetch_error", robots_url=robots_url, error=str(e))
            return {"parser": rp, "status": None, "crawl_delay": None}
    def info_for(self, url: str) -> Dict[str, Any]:
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin in self._cache:
            return self._cache[origin]
        robots_url = urljoin(origin, "/robots.txt")
        info = self._fetch(origin, robots_url)
        self._cache[origin] = info
        return info
    def allowed(self, url: str) -> bool:
        info = self.info_for(url)
        status = info.get("status")
        parser = info.get("parser")
        if status == 200:
            try:
                return parser.can_fetch(HTTP_USER_AGENT, url)
            except Exception:
                return True
        return True
    def crawl_delay(self, url: str) -> float:
        info = self.info_for(url)
        cd = info.get("crawl_delay")
        return float(cd) if cd is not None else DEFAULT_CRAWL_DELAY

class SimpleHTMLExtractor:
    from html.parser import HTMLParser
    def __init__(self, base_url: str):
        self._parser = self.HTMLParser(convert_charrefs=True)
        self.base_url = base_url
        self._texts = []
        self.links = []
        self._in_ignorable = False
        self._in_anchor = False
    def feed(self, text: str):
        self._parser.reset()
        self._parser.convert_charrefs = True
        self._parser.base_url = self.base_url
        self._parser.handle_starttag = self._handle_starttag
        self._parser.handle_endtag = self._handle_endtag
        self._parser.handle_data = self._handle_data
        self._parser.feed(text)
    def _handle_starttag(self, tag, attrs):
        tagl = tag.lower()
        if tagl in ("script", "style", "noscript", "iframe"):
            self._in_ignorable = True
        if tagl == "a":
            self._in_anchor = True
            attrs_d = dict(attrs)
            href = attrs_d.get("href") or ""
            try:
                full = urljoin(self.base_url, href)
            except Exception:
                full = href
            self.links.append({"href": full, "text": "", "attrs": attrs_d})
        if tagl in ("area", "link"):
            attrs_d = dict(attrs)
            href = attrs_d.get("href") or attrs_d.get("src") or ""
            if href:
                try:
                    full = urljoin(self.base_url, href)
                except Exception:
                    full = href
                self.links.append({"href": full, "text": "", "attrs": attrs_d})
    def _handle_endtag(self, tag):
        if tag.lower() in ("script", "style", "noscript", "iframe"):
            self._in_ignorable = False
        if tag.lower() == "a":
            self._in_anchor = False
    def _handle_data(self, data):
        if not self._in_ignorable:
            txt = data.strip()
            if txt:
                self._texts.append(txt)
                if self._in_anchor and self.links:
                    self.links[-1]["text"] += ((" " + txt) if self.links[-1]["text"] else txt)
    def text(self) -> str:
        return " ".join(self._texts)
    def link_density(self) -> float:
        words = len(self.text().split())
        link_count = len(self.links)
        if words == 0:
            return float("inf") if link_count > 0 else 0.0
        return link_count / max(1, words)

# --------------------------
# WebScraper (enforces idempotency)
# --------------------------
class WebScraper:
    def __init__(self, storage: StorageClient):
        self.storage = storage
        self.http = httpx.Client(timeout=HTTP_TIMEOUT, headers={"User-Agent": HTTP_USER_AGENT})
        self.robots = RobotsCache(self.http, HTTP_USER_AGENT)
        self.browser = None
        if _HAS_PLAYWRIGHT:
            # keep placeholder, but do not initialize heavy browser unless needed
            self.browser = None
            jlog("playwright_available")
        self.seen: Set[str] = set()
        self.seen_hashes: Set[str] = set()
        self.metrics = {"fetch_attempts": 0, "fetch_success": 0, "fetch_failures": 0, "duplicates_skipped": 0, "bytes_ingested": 0, "manifests_written": 0, "links_harvested": 0}
        self.start_time = time.time()
        self.url_regex = re.compile(r"""(?:"|')((?:/|https?://)[^"'\s]{6,400}\.(?:pdf|PDF|doc|docx|xls|xlsx|csv|zip))["']""")
        self.api_url_regex = re.compile(r"""(?:"|')((?:/|https?://)[^"'\s]{10,300}\.json(?:\?[^\s"']*)?)["']""", re.IGNORECASE)

    def _time_exceeded(self) -> bool:
        return (time.time() - self.start_time) > MAX_TIME_IN_SECONDS

    def _seen_and_record(self, url: str) -> bool:
        c = _canonicalize_url(url)
        if c in self.seen:
            self.metrics["duplicates_skipped"] += 1
            return True
        self.seen.add(c)
        return False

    def _enforce_crawl_delay(self, url: str) -> None:
        cd = self.robots.crawl_delay(url)
        time.sleep(cd)

    def _head(self, url: str) -> Tuple[Optional[int], Dict[str, str]]:
        try:
            r = self.http.head(url, follow_redirects=True, timeout=HTTP_TIMEOUT)
            status = r.status_code
            headers = dict(r.headers or {})
            if status is not None and status < 400:
                return status, headers
            hdrs = {"User-Agent": HTTP_USER_AGENT, "Range": "bytes=0-1023"}
            with self.http.stream("GET", url, headers=hdrs, follow_redirects=True, timeout=HTTP_TIMEOUT) as g:
                return g.status_code, dict(g.headers or {})
        except Exception:
            try:
                hdrs = {"User-Agent": HTTP_USER_AGENT, "Range": "bytes=0-1023"}
                with self.http.stream("GET", url, headers=hdrs, follow_redirects=True, timeout=HTTP_TIMEOUT) as g:
                    return g.status_code, dict(g.headers or {})
            except Exception:
                return None, {}

    def _static_fetch_to_file(self, url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, str], str, str, List[Dict[str, Any]]]:
        tmpf = tempfile.NamedTemporaryFile(delete=False)
        tmpf_path = tmpf.name
        tmpf.close()
        captured = []
        with self.http.stream("GET", url, headers=headers or {}, follow_redirects=True, timeout=HTTP_TIMEOUT) as r:
            status = r.status_code
            final_url = str(r.url)
            headers_out = dict(r.headers)
            written = 0
            with open(tmpf_path, "wb") as fh:
                for chunk in r.iter_bytes(chunk_size=8192):
                    if not chunk:
                        break
                    fh.write(chunk)
                    written += len(chunk)
                    if written > MAX_CONTENT_BYTES:
                        raise RuntimeError("content_too_large")
        # lightweight link capture for discovery (best-effort)
        try:
            with open(tmpf_path, "rb") as fh:
                text = fh.read().decode(errors="replace")
            for m in self.url_regex.finditer(text):
                u = m.group(1)
                full = urljoin(final_url, u)
                captured.append({"type": "candidate_doc_url", "url": full})
            for m in self.api_url_regex.finditer(text):
                u = m.group(1)
                full = urljoin(final_url, u)
                captured.append({"type": "candidate_api_url", "url": full})
        except Exception:
            pass
        return status, headers_out, tmpf_path, final_url, captured

    def _process_fetch(self, url: str, depth: int = 0, parent: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
        """
        Strictly idempotent control flow:
        1) compute canonical + doc_id + source_id
        2) if manifest exists and FORCE_MANIFEST is false -> skip (no network/further writes)
        3) perform HEAD; if HEAD indicates 304/unchanged via ETag/Last-Modified compared to manifest -> skip
        4) fetch body; compute sha; if sha equals existing manifest.file_hash -> do not create new by-hash object or manifest (but ensure storage_url present)
        5) otherwise, upload by-hash and write latest manifest atomically (head-check then put)
        """
        if self._time_exceeded():
            return {"url": url, "error": "time_exceeded"}
        self.metrics["fetch_attempts"] += 1
        if not _ensure_disk():
            return {"url": url, "error": "disk_low"}
        canonical = _canonicalize_url(url)
        if self._seen_and_record(canonical):
            return {"url": url, "skipped": True, "reason": "seen_in_run"}
        if not is_allowed_domain(url):
            jlog("domain_not_allowed", url=url)
            return {"url": url, "error": "domain_not_allowed"}
        doc_id = hashlib.sha256(canonical.encode()).hexdigest()
        source_host = (urlparse(url).hostname or "unknown")
        source_id = f"{source_host}_{doc_id[:12]}"

        # --- Early idempotency check: existing manifest
        existing_manifest = self.storage.read_latest_manifest(source_id, doc_id)
        if existing_manifest and not force and not FORCE_MANIFEST:
            jlog("skip_existing_manifest", url=canonical, doc_id=doc_id, reason="manifest_present", manifest_summary={
                "file_hash": existing_manifest.get("file_hash"),
                "storage_url": existing_manifest.get("storage_url"),
                "remote_etag": existing_manifest.get("remote_etag"),
                "remote_lastmod": existing_manifest.get("remote_lastmod"),
                "timestamp": existing_manifest.get("timestamp")
            })
            return {"url": url, "skipped": True, "reason": "manifest_present"}

        # check robots
        if not self.robots.allowed(url):
            jlog("robots_disallowed", url=url)
            return {"url": url, "error": "robots_disallow"}

        # enforce crawl delay per-origin
        self._enforce_crawl_delay(url)

        # HEAD to detect changes if a manifest exists (best-effort)
        head_status, head_headers = self._head(url)
        head_etag = _normalize_etag((head_headers or {}).get("etag") if head_headers else None)
        head_lastmod = (head_headers or {}).get("last-modified") if head_headers else None

        # If we have an existing manifest and the HEAD info matches, skip fetching
        if existing_manifest and not force:
            em_etag = _normalize_etag(existing_manifest.get("remote_etag"))
            em_lastmod = existing_manifest.get("remote_lastmod")
            if head_status == 304:
                jlog("skip_not_modified_304", url=canonical, doc_id=doc_id)
                return {"url": url, "skipped": True, "reason": "not_modified_304"}
            if head_etag and em_etag and head_etag == em_etag:
                jlog("skip_etag_match", url=canonical, doc_id=doc_id, etag=head_etag)
                return {"url": url, "skipped": True, "reason": "etag_match"}
            if head_lastmod and em_lastmod and head_lastmod == em_lastmod:
                jlog("skip_lastmod_match", url=canonical, doc_id=doc_id, lastmod=head_lastmod)
                return {"url": url, "skipped": True, "reason": "lastmod_match"}

        # Fetch with retries
        attempts_meta = []
        attempt = 0
        last_exc = None
        while attempt <= HTTP_RETRIES:
            if self._time_exceeded():
                return {"url": url, "error": "time_exceeded"}
            attempt += 1
            start_at = datetime.now(timezone.utc)
            tmpf_path = None
            try:
                get_headers = {"User-Agent": HTTP_USER_AGENT}
                if head_etag:
                    get_headers["If-None-Match"] = head_etag
                if head_lastmod:
                    get_headers["If-Modified-Since"] = head_lastmod
                status, headers, tmpf_path, final_url, captured_network_files = self._static_fetch_to_file(url, headers=get_headers)
                if status == 304:
                    jlog("not_modified_304_after_get", url=url)
                    try:
                        if tmpf_path and os.path.exists(tmpf_path):
                            os.unlink(tmpf_path)
                    except Exception:
                        pass
                    return {"url": url, "skipped": True, "reason": "not_modified_304"}
                raw_sha256 = _compute_sha256_file(tmpf_path)
                if raw_sha256 in self.seen_hashes:
                    try:
                        if tmpf_path and os.path.exists(tmpf_path):
                            os.unlink(tmpf_path)
                    except Exception:
                        pass
                    jlog("duplicate_in_run_skipped", url=url, sha256=raw_sha256)
                    return {"url": url, "skipped": True, "reason": "duplicate_in_run", "sha256": raw_sha256}
                self.seen_hashes.add(raw_sha256)
                size_bytes = os.path.getsize(tmpf_path)
                attempts_meta.append({"started_at": start_at.isoformat().replace("+00:00", "Z"), "finished_at": iso_ts(), "status_code": status, "bytes_received": size_bytes, "error": None})
                ext = _ext_from_url_or_ct(final_url, headers.get("content-type") if headers else None)
                safe_name = _safe_name_from_url(final_url, ext)

                # If an existing manifest exists and the raw sha matches, ensure storage URL recorded; don't reupload
                if existing_manifest and existing_manifest.get("file_hash") == raw_sha256:
                    jlog("content_unchanged_sha_matches", url=canonical, doc_id=doc_id, sha=raw_sha256)
                    if not existing_manifest.get("storage_url"):
                        try:
                            upload_meta = self.storage.upload_binary_by_hash(source_id, raw_sha256, ext, tmpf_path)
                            existing_manifest["storage_url"] = upload_meta.get("storage_url")
                            existing_manifest["size_bytes"] = upload_meta.get("size_bytes")
                            existing_manifest["remote_etag"] = head_etag or existing_manifest.get("remote_etag")
                            existing_manifest["remote_lastmod"] = head_lastmod or existing_manifest.get("remote_lastmod")
                            existing_manifest["timestamp"] = iso_ts()
                            manifest_path = self.storage.write_latest_manifest(source_id, doc_id, existing_manifest)
                            self.metrics["manifests_written"] += 1
                            jlog("manifest_fixed_storage_url", url=canonical, doc_id=doc_id, manifest_path=manifest_path)
                        except Exception as e:
                            jlog("fix_manifest_storage_failed", url=canonical, error=str(e))
                    try:
                        if tmpf_path and os.path.exists(tmpf_path):
                            os.unlink(tmpf_path)
                    except Exception:
                        pass
                    self.metrics["fetch_success"] += 1
                    return {"url": url, "skipped": True, "reason": "sha_matches_existing_manifest", "sha256": raw_sha256}

                # Otherwise, upload content to by-hash and write new manifest
                upload_meta = self.storage.upload_binary_by_hash(source_id, raw_sha256, ext, tmpf_path)
                minimal_manifest = {
                    "file_hash": raw_sha256,
                    "mime_ext": ext or "",
                    "original_url": final_url,
                    "timestamp": iso_ts(),
                    "storage_url": upload_meta.get("storage_url"),
                    "size_bytes": upload_meta.get("size_bytes"),
                    "remote_etag": _normalize_etag(headers.get("etag") if headers else None),
                    "remote_lastmod": headers.get("last-modified") if headers else None,
                    "scraper_version": SCRAPER_VERSION
                }
                try:
                    # validate minimal manifest before writing
                    manifest_validator_minimal(minimal_manifest)
                except Exception as e:
                    jlog("manifest_validation_failed", url=final_url, error=str(e))
                manifest_path = self.storage.write_latest_manifest(source_id, doc_id, minimal_manifest)
                self.metrics["fetch_success"] += 1
                self.metrics["bytes_ingested"] += upload_meta.get("size_bytes", 0) or 0
                self.metrics["manifests_written"] += 1

                # extract links for discovery when HTML (best-effort)
                candidate_links: List[str] = []
                ct = headers.get("content-type") if headers else ""
                is_html = ("text/html" in (ct or "")) or safe_name.lower().endswith((".html", ".htm", "response.html"))
                if is_html:
                    try:
                        # read either the uploaded local file or the tmp file (upload to s3 returns s3:// path)
                        read_path = None
                        if upload_meta.get("storage_url") and not (self.storage.s3 and str(upload_meta.get("storage_url")).startswith("s3://")):
                            read_path = upload_meta.get("storage_url")
                        else:
                            read_path = tmpf_path
                        with open(read_path, "rb") as fh:
                            text = fh.read().decode(errors="replace")
                        extractor = SimpleHTMLExtractor(final_url)
                        extractor.feed(text)
                        ld = extractor.link_density()
                        txt_words = len(extractor.text().split())
                        links = extractor.links[:400]
                        for link in links:
                            href = link.get("href") or ""
                            anchor_text = link.get("text") or ""
                            attrs = link.get("attrs") or {}
                            if not href:
                                continue
                            if not is_allowed_domain(href):
                                continue
                            if anchor_implies_document(href, anchor_text, attrs):
                                candidate_links.append(_canonicalize_url(href))
                            else:
                                if any(k in (anchor_text or "").lower() for k in ("scheme","eligibility","apply","how to","guidelines","notice")):
                                    candidate_links.append(_canonicalize_url(href))
                        # also harvest candidate urls from regex matches
                        for m in self.api_url_regex.finditer(text):
                            candidate_links.append(urljoin(final_url, m.group(1)))
                        for m in self.url_regex.finditer(text):
                            candidate_links.append(urljoin(final_url, m.group(1)))
                    except Exception:
                        pass

                jlog("fetch_and_commit_success",
                     url=url,
                     final_url=final_url,
                     file_path=upload_meta.get("storage_url"),
                     manifest_path=manifest_path,
                     sha256=raw_sha256,
                     links=len(candidate_links))
                try:
                    if tmpf_path and os.path.exists(tmpf_path):
                        os.unlink(tmpf_path)
                except Exception:
                    pass

                prioritized = []
                next_discovery = []
                for l in candidate_links:
                    if not l:
                        continue
                    if any(l.lower().endswith("." + e) for e in ALLOWED_EXTENSIONS) or any(p in l.lower() for p in ("/documents/","/uploads/","/files/")) or "file=" in l.lower():
                        prioritized.append(l)
                    else:
                        if depth + 1 <= MAX_CRAWL_DEPTH:
                            next_discovery.append(l)
                return {"manifest": minimal_manifest, "links": prioritized + next_discovery}
            except Exception as e:
                last_exc = e
                try:
                    if tmpf_path and os.path.exists(tmpf_path):
                        os.unlink(tmpf_path)
                except Exception:
                    pass
                backoff_sleep(HTTP_BACKOFF, attempt)
                self.metrics["fetch_failures"] += 1
                jlog("fetch_attempt_exception", url=url, attempt=attempt, error=str(e))
                continue
        jlog("fetch_failed_all_attempts", url=url, error=str(last_exc))
        return {"url": url, "error": str(last_exc) if last_exc else "fetch_failed"}

    def run(self, seeds: List[str]) -> None:
        prioritized_docs: List[Tuple[str,int]] = []
        discovery_queue: List[Tuple[str,int]] = []
        for s in seeds:
            discovery_queue.append((s, 0))
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as ex:
            futures = {}
            while (discovery_queue or prioritized_docs) and not self._time_exceeded():
                while prioritized_docs and len(futures) < CONCURRENT_WORKERS:
                    url, depth = prioritized_docs.pop(0)
                    futures[ex.submit(self._process_fetch, url, depth, None)] = (url, depth)
                while discovery_queue and len(futures) < CONCURRENT_WORKERS:
                    url, depth = discovery_queue.pop(0)
                    futures[ex.submit(self._process_fetch, url, depth, None)] = (url, depth)
                if not futures:
                    break
                for fut in list(futures):
                    if fut.done():
                        url, depth = futures.pop(fut)
                        try:
                            res = fut.result()
                        except Exception as e:
                            jlog("worker_exception", url=url, error=str(e))
                            continue
                        if isinstance(res, dict) and res.get("links"):
                            for l in res["links"]:
                                if not l:
                                    continue
                                if any(l.lower().endswith("." + e) for e in ALLOWED_EXTENSIONS) or any(p in l.lower() for p in ("/documents/","/uploads/","/files/")):
                                    prioritized_docs.append((l, depth + 1))
                                else:
                                    if depth + 1 <= MAX_CRAWL_DEPTH:
                                        discovery_queue.append((l, depth + 1))
            jlog("pipeline_complete", metrics=self.metrics, elapsed_seconds=int(time.time()-self.start_time))

# --------------------------
# Validation helpers
# --------------------------
def manifest_validator_minimal(man: Dict[str, Any]) -> None:
    required = ["file_hash", "mime_ext", "original_url", "timestamp"]
    missing = [k for k in required if k not in man]
    if missing:
        raise ValueError(f"manifest missing required fields: {missing}")
    if not isinstance(man.get("file_hash"), str) or len(man.get("file_hash", "")) < 8:
        raise ValueError("invalid file_hash")
    try:
        _ = datetime.fromisoformat(man["timestamp"].replace("Z", "+00:00"))
    except Exception:
        raise ValueError("timestamp not ISO8601")

def anchor_implies_document(href: str, anchor_text: str, attrs: Dict[str, str]) -> bool:
    txt = (anchor_text or "").lower()
    for kw in ("download", "pdf", "guideline", "application", "form", "gazette", "notification", "circular"):
        if kw in txt:
            return True
    href_l = (href or "").lower()
    if any(p in href_l for p in ("/documents/", "/uploads/", "/files/", "/pdf", "/download", "attachment")):
        return True
    if any(href_l.endswith("." + e) for e in ALLOWED_EXTENSIONS):
        return True
    return False

def is_allowed_domain(url: str) -> bool:
    if not ALLOWED_DOMAINS:
        return True
    try:
        host = urlparse(url).hostname or ""
        for allowed in ALLOWED_DOMAINS:
            if host.endswith(allowed):
                return True
        return False
    except Exception:
        return False

# --------------------------
# Main
# --------------------------
def main() -> int:
    storage = StorageClient(s3_bucket=S3_BUCKET, aws_region=AWS_REGION)
    if S3_BUCKET and not storage.preflight_s3_access():
        jlog("s3_preflight_failed", bucket=S3_BUCKET)
        return 2
    scraper = WebScraper(storage=storage)
    jlog("starting_scraper", seeds=len(SEED_URLS), allowed_domains=ALLOWED_DOMAINS, use_browser=bool(scraper.browser))
    scraper.run(SEED_URLS)
    jlog("exiting_scraper", metrics=scraper.metrics)
    return 0

if __name__ == "__main__":
    try:
        rc = main()
    except Exception as e:
        jlog("unhandled_exception", error=str(e))
        rc = 1
    sys.exit(rc)
