#!/usr/bin/env python3
"""
Idempotent HTML parser (final, hardened).

Key guarantees:
- One raw object -> one chunk JSONL file at:
    {CHUNKED_PREFIX}/{CHUNKED_SCHEMA_VERSION}/{document_id}.chunks.jsonl
- Raw manifest (<raw_key>.manifest.json) is extended with a `chunked` subdocument
  including chunked_sha256, chunk_file, chunk_count, etc.
- If raw manifest already has the same chunked_sha256, parsing is skipped (idempotent).
- Chunk JSONL is deterministic (json.dumps(..., sort_keys=True)) to make sha stable.
- Chunk payload contains only required fields + limited meta fields expected by the indexer.
- No language detection, no extraneous fields.
"""
from __future__ import annotations
import os
import sys
import json
import time
import hashlib
import traceback
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# optional libs
try:
    import trafilatura
except Exception:
    trafilatura = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

try:
    import tiktoken
except Exception:
    tiktoken = None

# -------------------------
# Environment / config
# -------------------------
STORAGE = os.getenv("STORAGE", "s3").strip().lower()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
RAW_PREFIX = (os.getenv("RAW_PREFIX") or os.getenv("STORAGE_RAW_PREFIX") or "data/raw/").rstrip("/") + "/"
CHUNKED_PREFIX = (os.getenv("CHUNKED_PREFIX") or os.getenv("STORAGE_CHUNKED_PREFIX") or "data/chunked/").rstrip("/") + "/"
PARSER_VERSION = os.getenv("PARSER_VERSION", "html-trafilatura-v1")
MIN_TOKENS_PER_CHUNK = int(os.getenv("MIN_TOKENS_PER_CHUNK", "100"))
MAX_TOKENS_PER_CHUNK = int(os.getenv("MAX_TOKENS_PER_CHUNK", "512"))
OVERLAP_SENTENCES = int(os.getenv("OVERLAP_SENTENCES", "2"))
ENC_NAME = os.getenv("ENC_NAME", "cl100k_base")
CHUNKED_SCHEMA_VERSION = os.getenv("CHUNKED_SCHEMA_VERSION", "chunked_v1").rstrip("/")
FORCE_PROCESS = os.getenv("FORCE_PROCESS", "false").lower() == "true"
PUT_RETRIES = int(os.getenv("PUT_RETRIES", "3"))
PUT_BACKOFF = float(os.getenv("PUT_BACKOFF", "0.3"))

# validations
if STORAGE not in ("s3", "local"):
    raise SystemExit(f"Invalid STORAGE value: {STORAGE!r}. Expected 's3' or 'local'.")
if STORAGE == "s3" and not S3_BUCKET:
    raise SystemExit("S3_BUCKET env required when STORAGE='s3'")
if not CHUNKED_SCHEMA_VERSION:
    raise SystemExit("CHUNKED_SCHEMA_VERSION must be set and non-empty (e.g. 'chunked_v1').")

# -------------------------
# Logging helpers
# -------------------------
def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def log(level: str, event: str, **extra):
    obj = {"ts": now_ts(), "level": level, "event": event}
    obj.update(extra)
    line = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    if level.lower() in ("error","err","critical"):
        print(line, file=sys.stderr, flush=True)
    else:
        print(line, flush=True)

# -------------------------
# S3 helpers (atomic writes)
# -------------------------
def _ensure_s3_client():
    try:
        import boto3
    except Exception as e:
        raise RuntimeError("boto3 is required for S3 STORAGE backend: " + str(e))
    return boto3.client("s3")

def _s3_atomic_put(final_key: str, data: bytes, content_type: Optional[str] = "application/octet-stream"):
    """
    Atomic write to S3: put temp key, copy to final key, delete temp key.
    final_key must be the S3 object key (no bucket prefix).
    Retries on transient errors.
    """
    client = _ensure_s3_client()
    temp_key = f"{final_key}.tmp.{os.getpid()}.{int(time.time()*1000)}"
    last_exc = None
    for attempt in range(1, PUT_RETRIES + 1):
        try:
            client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=data, ContentType=content_type)
            client.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': temp_key}, Key=final_key)
            client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
            return
        except Exception as e:
            last_exc = e
            try:
                client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
            except Exception:
                pass
            if attempt < PUT_RETRIES:
                time.sleep(PUT_BACKOFF * attempt)
                continue
            raise last_exc

# -------------------------
# Atomic writes for chunked files
# -------------------------
def write_text_atomic_to_chunked(target_relpath: str, text: str) -> None:
    """
    Write deterministic JSONL to CHUNKED_PREFIX/<target_relpath> atomically.
    target_relpath is relative (e.g. "chunked_v1/<document_id>.chunks.jsonl")
    """
    target_relpath = target_relpath.lstrip("/")
    final_key = f"{CHUNKED_PREFIX.rstrip('/')}/{target_relpath}"
    if STORAGE == "s3":
        _s3_atomic_put(final_key, text.encode("utf-8"), content_type="application/json")
    else:
        p = Path(final_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("wb") as fh:
            fh.write(text.encode("utf-8"))
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(p)

def compute_sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def s3_url_for_raw(key: str) -> Optional[str]:
    if STORAGE == "s3" and S3_BUCKET:
        return f"s3://{S3_BUCKET}/{key}"
    return None

# -------------------------
# Tokenization / splitting
# -------------------------
def canonicalize_text(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+", " ", s).strip()
    return s

_sentence_re = re.compile(r'(.+?[\.\?\!\n]+)|(.+?$)', re.DOTALL)

def sentence_spans(text: str) -> List[Tuple[str,int,int]]:
    spans = []
    cursor = 0
    for m in _sentence_re.finditer(text):
        s = (m.group(1) or m.group(2) or "").strip()
        if not s:
            continue
        start = text.find(s, cursor)
        if start == -1:
            start = cursor
        end = start + len(s)
        spans.append((s, start, end))
        cursor = end
    return spans

def get_tokenizer():
    if tiktoken is None:
        return None
    try:
        return tiktoken.get_encoding(ENC_NAME)
    except Exception:
        try:
            return tiktoken.get_encoding("cl100k_base")
        except Exception:
            return None

def encode_tokens(text: str):
    enc = get_tokenizer()
    if enc is None:
        return text.split()
    try:
        return enc.encode(text)
    except Exception:
        return text.split()

def decode_tokens(tokens) -> str:
    enc = get_tokenizer()
    if enc is None:
        return " ".join(tokens)
    try:
        return enc.decode(tokens)
    except Exception:
        if isinstance(tokens, list):
            return " ".join(str(t) for t in tokens)
        return str(tokens)

def split_into_windows(text: str, max_tokens: int = MAX_TOKENS_PER_CHUNK,
                       min_tokens: int = MIN_TOKENS_PER_CHUNK,
                       overlap_sentences: int = OVERLAP_SENTENCES):
    text = canonicalize_text(text)
    if not text:
        yield {"window_index": 0, "text": "", "token_count": 0, "token_start": 0, "token_end": 0}
        return
    spans = sentence_spans(text)
    sent_items = []
    token_cursor = 0
    for s, sc, ec in spans:
        toks = encode_tokens(s)
        tok_len = len(toks) if isinstance(toks, (list,tuple)) else 1
        sent_items.append({"text": s, "start_char": sc, "end_char": ec, "token_len": tok_len, "tokens": toks})
    if not sent_items:
        toks = encode_tokens(text)
        yield {"window_index": 0, "text": text, "token_count": len(toks) if isinstance(toks, (list,tuple)) else 1, "token_start": 0, "token_end": len(toks) if isinstance(toks,(list,tuple)) else 1}
        return
    for si in sent_items:
        si["token_start_idx"] = token_cursor
        si["token_end_idx"] = token_cursor + si["token_len"]
        token_cursor = si["token_end_idx"]
    windows = []
    i = 0
    window_index = 0
    while i < len(sent_items):
        cur_tokens = 0
        chunk_texts = []
        chunk_token_start = sent_items[i]["token_start_idx"]
        chunk_token_end = chunk_token_start
        start_i = i
        while i < len(sent_items):
            sent = sent_items[i]
            if cur_tokens + sent["token_len"] > max_tokens:
                break
            chunk_texts.append(sent["text"])
            cur_tokens += sent["token_len"]
            chunk_token_end = sent.get("token_end_idx", chunk_token_start + cur_tokens)
            i += 1
        if not chunk_texts:
            sent = sent_items[i]
            toks = sent["tokens"]
            if isinstance(toks, (list,tuple)):
                prefix = toks[:max_tokens]
                try:
                    prefix_text = decode_tokens(prefix)
                except Exception:
                    prefix_text = " ".join(str(x) for x in prefix)
                cur_tokens = len(prefix)
                remainder = toks[max_tokens:]
                if remainder:
                    sent_items[i]["tokens"] = remainder
                    sent_items[i]["token_len"] = len(remainder)
                else:
                    i += 1
                chunk_token_end = chunk_token_start + cur_tokens
                chunk_texts = [prefix_text]
            else:
                chunk_texts = [sent["text"]]
                cur_tokens = sent["token_len"]
                i += 1
        chunk_text = " ".join(chunk_texts).strip()
        chunk_meta = {"window_index": window_index, "text": chunk_text, "token_count": int(cur_tokens), "token_start": int(chunk_token_start), "token_end": int(chunk_token_end), "start_sentence_idx": start_i, "end_sentence_idx": i}
        window_index += 1
        if windows and chunk_meta["token_count"] < min_tokens:
            prev = windows[-1]
            prev["text"] = prev["text"] + " " + chunk_meta["text"]
            prev["token_count"] = prev["token_count"] + chunk_meta["token_count"]
            prev["token_end"] = chunk_meta["token_end"]
            prev["end_sentence_idx"] = chunk_meta["end_sentence_idx"]
        else:
            windows.append(chunk_meta)
        i = max(start_i + 1, chunk_meta["end_sentence_idx"] - overlap_sentences)
    for w in windows:
        yield w

def derive_semantic_region(token_start: int, token_end: int, document_total_tokens: int) -> str:
    try:
        if document_total_tokens is None or document_total_tokens <= 0:
            return "unknown"
        midpoint = (float(token_start) + float(token_end)) / 2.0 / float(document_total_tokens)
        if midpoint < 0.10:
            return "intro"
        if midpoint < 0.30:
            return "early"
        if midpoint < 0.75:
            return "middle"
        if midpoint < 0.95:
            return "late"
        return "footer"
    except Exception:
        return "unknown"

# -------------------------
# Chunk path helpers & idempotency checks
# -------------------------
def _chunk_relpath_for_document(document_id: str) -> str:
    return f"{CHUNKED_SCHEMA_VERSION}/{document_id}.chunks.jsonl"

def _exists_chunk_for_document(document_id: str) -> bool:
    rel = _chunk_relpath_for_document(document_id)
    full = f"{CHUNKED_PREFIX.rstrip('/')}/{rel}"
    if STORAGE == "s3":
        try:
            s3 = _ensure_s3_client()
            s3.head_object(Bucket=S3_BUCKET, Key=full)
            return True
        except Exception:
            return False
    else:
        p = Path(full)
        return p.exists()

def _raw_manifest_key_for_raw_key(raw_key: str) -> str:
    if STORAGE == "s3":
        return raw_key + ".manifest.json"
    p = Path(raw_key)
    if p.exists():
        return str(p.with_suffix(p.suffix + ".manifest.json"))
    return str(Path(RAW_PREFIX) / (p.name + ".manifest.json"))

def _read_raw_manifest(raw_manifest_key: str) -> Dict[str, Any]:
    if STORAGE == "s3":
        s3 = _ensure_s3_client()
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=raw_manifest_key)
            body = resp["Body"].read()
            return json.loads(body.decode("utf-8"))
        except Exception:
            return {}
    else:
        try:
            with open(raw_manifest_key, "rb") as fh:
                return json.load(fh)
        except Exception:
            return {}

def _write_raw_manifest(raw_manifest_key: str, manifest_obj: Dict[str, Any]) -> None:
    if STORAGE == "s3":
        s3 = _ensure_s3_client()
        body = json.dumps(manifest_obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
        _s3_atomic_put(raw_manifest_key, body, content_type="application/json")
    else:
        p = Path(raw_manifest_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("wb") as fh:
            fh.write(json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"))
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(p)

def _write_chunks_and_extend_raw_manifest(document_id: str, chunks: List[Dict[str, Any]], raw_key: str, raw_sha: str, original_url: Optional[str] = None) -> Tuple[str,int,str]:
    """
    Write JSONL for document_id and atomically extend raw manifest with `chunked` metadata.
    Deterministic: json lines are dumped with sort_keys=True.
    """
    rel = _chunk_relpath_for_document(document_id)
    jsonl_text = "\n".join(json.dumps(c, ensure_ascii=False, sort_keys=True) for c in chunks) + "\n"
    jsonl_bytes = jsonl_text.encode("utf-8")
    sha = compute_sha256_bytes(jsonl_bytes)
    size = len(jsonl_bytes)

    raw_manifest_key = _raw_manifest_key_for_raw_key(raw_key)
    existing = _read_raw_manifest(raw_manifest_key) or {}
    existing_chunked = existing.get("chunked", {})
    if existing_chunked and existing_chunked.get("chunked_sha256") == sha and not FORCE_PROCESS:
        log("info", "raw_manifest_chunked_up_to_date", raw_manifest=raw_manifest_key, chunk_rel=rel, chunked_sha256=sha)
        if STORAGE == "s3":
            chunk_file = f"s3://{S3_BUCKET}/{CHUNKED_PREFIX.rstrip('/')}/{rel}"
        else:
            chunk_file = str(Path(CHUNKED_PREFIX) / rel)
        return chunk_file, size, sha

    # write chunk file
    try:
        write_text_atomic_to_chunked(rel, jsonl_text)
    except Exception as e:
        log("error", "chunk_write_failed", raw_key=raw_key, error=str(e))
        raise

    if STORAGE == "s3":
        chunk_file = f"s3://{S3_BUCKET}/{CHUNKED_PREFIX.rstrip('/')}/{rel}"
    else:
        chunk_file = str(Path(CHUNKED_PREFIX) / rel)

    chunked_meta = {
        "chunk_file": chunk_file,
        "chunk_format": "jsonl",
        "schema_version": CHUNKED_SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "ingest_time": now_ts(),
        "chunk_count": len(chunks),
        "chunked_sha256": sha,
        "chunked_size_bytes": size,
    }

    existing.setdefault("file_hash", existing.get("file_hash") or raw_sha)
    existing.setdefault("timestamp", existing.get("timestamp") or now_ts())
    existing["parser_version"] = PARSER_VERSION
    if original_url:
        existing["original_url"] = original_url
    existing["chunked"] = chunked_meta
    existing["saved_chunks"] = len(chunks)
    existing["chunked_manifest_written_at"] = now_ts()

    try:
        _write_raw_manifest(raw_manifest_key, existing)
    except Exception as e:
        log("error", "raw_manifest_write_failed", raw_manifest=raw_manifest_key, error=str(e))
        raise

    log("info", "raw_manifest_extended", raw_manifest=raw_manifest_key, chunk_file=chunk_file, chunks=len(chunks), sha256=sha, size=size)
    return chunk_file, size, sha

# -------------------------
# read raw bytes (S3/local)
# -------------------------
def read_raw_bytes(key: str) -> bytes:
    if STORAGE == "s3":
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET required for STORAGE='s3'")
        s3 = _ensure_s3_client()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return resp["Body"].read()
    else:
        p = Path(key)
        if not p.exists():
            candidate = Path(RAW_PREFIX) / Path(key).name
            if candidate.exists():
                p = candidate
        with p.open("rb") as f:
            return f.read()

# -------------------------
# main parse_file
# -------------------------
def parse_file(key: str, manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse one raw object referenced by `key`. Returns {"saved_chunks": int, ...}.
    `manifest` may be empty dict or the dict read from raw manifest.
    """
    start = time.perf_counter()
    try:
        try:
            raw_bytes = read_raw_bytes(key)
        except Exception as e:
            tb = traceback.format_exc()
            log("error", "read_failed", key=key, error=str(e), traceback=tb)
            return {"saved_chunks": 0, "error": str(e)}

        raw_sha = compute_sha256_bytes(raw_bytes)
        document_id = manifest.get("file_hash") or raw_sha

        # idempotency: if chunk file exists and not forcing, skip
        if not FORCE_PROCESS and _exists_chunk_for_document(document_id):
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "skip_existing", document_id=document_id, key=key, duration_ms=duration_ms)
            return {"saved_chunks": 0, "skipped": True}

        # decode bytes to text (best-effort)
        text_candidate = None
        try:
            text_candidate = raw_bytes.decode("utf-8")
        except Exception:
            try:
                text_candidate = raw_bytes.decode("latin-1")
            except Exception:
                text_candidate = None

        extracted_text = None
        parsed_meta: Dict[str,Any] = {}
        title: Optional[str] = None

        # prefer trafilatura (if HTML-like)
        if trafilatura is not None and text_candidate is not None:
            try:
                json_doc = trafilatura.extract(text_candidate, output_format="json", with_metadata=True)
                if json_doc:
                    parsed_meta = json.loads(json_doc)
                    extracted_text = parsed_meta.get("text") or parsed_meta.get("excerpt") or parsed_meta.get("body") or ""
                    title = parsed_meta.get("title") or title
            except Exception:
                try:
                    extracted_text = trafilatura.extract(text_candidate, output_format="text")
                except Exception:
                    extracted_text = None

        # fallback to BeautifulSoup if needed
        if not extracted_text and BeautifulSoup is not None and text_candidate is not None:
            try:
                soup = BeautifulSoup(text_candidate, "html.parser")
                if not manifest.get("source_url"):
                    can = soup.find("link", rel="canonical")
                    if can and can.get("href"):
                        manifest["source_url"] = can.get("href")
                if not title:
                    t = soup.title.string if soup.title and soup.title.string else None
                    title = t or title
                paras = [p.get_text(separator=" ", strip=True) for p in soup.find_all(["p","li"])]
                extracted_text = "\n\n".join([p for p in paras if p])
            except Exception:
                extracted_text = None

        # final fallback: use raw text candidate
        if not extracted_text:
            if text_candidate:
                extracted_text = text_candidate
            else:
                log("warn", "no_text_extracted", key=key)
                return {"saved_chunks": 0, "error": "no_extractable_text"}

        canonical_text = canonicalize_text(extracted_text)
        try:
            token_items = encode_tokens(canonical_text)
            total_tokens = len(token_items) if isinstance(token_items, (list,tuple)) else 1
        except Exception:
            total_tokens = len(canonical_text.split())

        windows = list(split_into_windows(canonical_text, max_tokens=MAX_TOKENS_PER_CHUNK, min_tokens=MIN_TOKENS_PER_CHUNK, overlap_sentences=OVERLAP_SENTENCES))
        if not windows:
            log("info", "no_windows_generated", key=key)
            return {"saved_chunks": 0}

        # build minimal chunks list (only required + limited metadata)
        chunks: List[Dict[str,Any]] = []
        headings: List[str] = []
        if isinstance(parsed_meta, dict):
            if parsed_meta.get("title"):
                headings.append(parsed_meta.get("title"))
        if title and title not in headings:
            headings.insert(0, title)

        source_url_authoritative = manifest.get("original_url") or manifest.get("source_url") or parsed_meta.get("url") if isinstance(parsed_meta, dict) else (s3_url_for_raw(key) or (key if STORAGE != "s3" else None))

        provenance_base = {"raw_sha256": raw_sha, "raw_key": key, "original_url": manifest.get("original_url")}

        for w in windows:
            idx = int(w.get("window_index", 0))
            chunk_index = idx  # zero-based chunk_index is fine; embedder normalizes if needed
            chunk_id = f"{document_id}_c{str(idx).zfill(4)}"
            token_start = int(w.get("token_start", 0))
            token_end = int(w.get("token_end", 0))
            sem = derive_semantic_region(token_start, token_end, total_tokens)
            chunk: Dict[str,Any] = {
                # core required fields (embedding pipeline expects these)
                "document_id": document_id,
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "text": w.get("text",""),
                "token_count": int(w.get("token_count", 0)),
                "token_range": [int(token_start), int(token_end)],
                "document_total_tokens": int(total_tokens),
                # minimal provenance & retrieval aids
                "semantic_region": sem,
                "file_type": (manifest.get("mime_ext") or "html"),
                "source_url": source_url_authoritative,
                "page_number": None,
                # ingestion
                "ingest_time": now_ts(),
                "parser_version": PARSER_VERSION,
                # metadata keys (these will be copied into meta by the indexer)
                "headings": headings or None,
                "heading_path": headings or None,
                "layout_tags": ["html"],
                "figures": [],
                "used_ocr": False,
                "provenance": provenance_base,
                # keep original manifest minimal reference (not large) - optional, small
                # DO NOT include language detection or large original manifests.
            }
            # remove None entries to keep payload minimal
            chunk = {k: v for k, v in chunk.items() if v is not None}
            chunks.append(chunk)

        if not chunks:
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "no_chunks_after_processing", key=key, document_id=document_id, duration_ms=duration_ms)
            return {"saved_chunks": 0}

        # race check: ensure another process didn't already write same document
        if not FORCE_PROCESS and _exists_chunk_for_document(document_id):
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "race_skip_existing_after_generation", document_id=document_id, key=key, duration_ms=duration_ms)
            return {"saved_chunks": 0, "skipped": True}

        # write chunk file and extend raw manifest deterministically
        try:
            chunk_file, size, sha = _write_chunks_and_extend_raw_manifest(document_id, chunks, key, raw_sha, manifest.get("original_url"))
        except Exception as e:
            tb = traceback.format_exc()
            log("error", "chunk_write_failed", key=key, error=str(e), traceback=tb)
            return {"saved_chunks": 0, "error": str(e)}

        duration_ms = int((time.perf_counter() - start) * 1000)
        log("info", "html_parsed", key=key, document_id=document_id, saved_chunks=len(chunks), chunk_file=chunk_file, chunk_size_bytes=size, chunk_sha256=sha, duration_ms=duration_ms)
        return {"saved_chunks": len(chunks)}
    except Exception as exc:
        tb = traceback.format_exc()
        log("error", "parse_exception", key=key, error=str(exc), traceback=tb)
        return {"saved_chunks": 0, "error": str(exc), "traceback": tb}

# -------------------------
# helpers used by parse_file but defined after to keep file structured
# -------------------------
# (read_raw_bytes already defined above)

# -------------------------
# validation helpers
# -------------------------
def manifest_validator_minimal(man: Dict[str, Any]) -> None:
    required = ["file_hash", "mime_ext", "original_url", "timestamp"]
    missing = [k for k in required if k not in man]
    if missing:
        raise ValueError(f"manifest missing required fields: {missing}")
    if not isinstance(man.get("file_hash"), str) or len(man.get("file_hash","")) < 8:
        raise ValueError("invalid file_hash")
    try:
        _ = datetime.fromisoformat(man["timestamp"].replace("Z","+00:00"))
    except Exception:
        raise ValueError("timestamp not ISO8601")

def anchor_implies_document(href: str, anchor_text: str, attrs: Dict[str, str]) -> bool:
    txt = (anchor_text or "").lower()
    for kw in ("download","pdf","guideline","application","form","gazette","notification","circular"):
        if kw in txt:
            return True
    href_l = (href or "").lower()
    if any(p in href_l for p in ("/documents/","/uploads/","/files/","/pdf","/download","attachment")):
        return True
    if any(href_l.endswith("." + e) for e in ["pdf","doc","docx","xls","xlsx","csv","json","zip","html","htm"]):
        return True
    return False

def is_allowed_domain(url: str) -> bool:
    # policy: allow all here; top-level controller enforces domain allowlist.
    return True

# -------------------------
# module loaded log
# -------------------------
if __name__ == "__main__":
    log("info", "module_loaded", module=__file__, parser_version=PARSER_VERSION)
