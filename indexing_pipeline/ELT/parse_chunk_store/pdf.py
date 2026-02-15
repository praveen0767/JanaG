#!/usr/bin/env python3
"""
indexing_pipeline/ELT/parse_chunk_store/pdf.py
Converged PDF parser (idempotent, single-chunk-jsonl-per-document)

Assumptions & external contracts:
- Runtime: Python 3.8+ (tested with 3.11). Optional libs: boto3 (for S3), fitz (PyMuPDF) or pymupdf, pdfplumber,
  Pillow, pytesseract, tiktoken. The parser falls back gracefully when optional libs are absent.
- STORAGE env expected to be 's3' or 'local' (router will set STORAGE='local' if configured 'fs').
- When STORAGE == 's3', S3_BUCKET must be set.
- Raw bytes are read from a 'key' (S3 object key or local path/relative filename).
- Idempotency boundary:
  * One JSONL chunk file is written to CHUNKED_PREFIX/<CHUNKED_SCHEMA>/<document_id>.chunks.jsonl
  * Raw manifest at <raw_key>.manifest.json is extended with `chunked` metadata including chunked_sha256.
  * If manifest already contains identical chunked_sha256 and FORCE_OVERWRITE is false => skip (no writes).
- All S3 writes are atomic via temp-key -> copy -> delete pattern. Local writes are atomic via temp file -> replace.
- parse_file(key, manifest) -> dict with at least "saved_chunks": int. If skipped, include "skipped": True.
- Determinism: document_id derived from raw content SHA256 by default (or manifest.file_hash if provided).

Invariants:
- Re-running parse_file on unchanged raw content produces no additional writes once raw_manifest.chunked.chunked_sha256
  equals the computed JSONL sha.
- Chunk filename derivation and raw manifest extension are deterministic and minimal.

Logging:
- Every important decision logs a structured JSON line.

Return:
- parse_file returns {"saved_chunks": int, ...}
"""

from __future__ import annotations
import os
import sys
import io
import json
import time
import hashlib
import tempfile
import traceback
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

# -------------------------
# Environment / config (explicit at top)
# -------------------------
STORAGE = os.getenv("STORAGE", "s3").strip().lower()            # 's3' or 'local'
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
RAW_PREFIX = (os.getenv("RAW_PREFIX") or os.getenv("STORAGE_RAW_PREFIX") or "data/raw/").rstrip("/") + "/"
CHUNKED_PREFIX = (os.getenv("CHUNKED_PREFIX") or os.getenv("STORAGE_CHUNKED_PREFIX") or "data/chunked/").rstrip("/") + "/"
PARSER_VERSION = os.getenv("PARSER_VERSION_PDF", "pdf-tesseract-v1")
CHUNKED_SCHEMA = os.getenv("CHUNKED_SCHEMA_VERSION", "chunked_v1").rstrip("/")
FORCE_OVERWRITE = os.getenv("FORCE_OVERWRITE", "false").lower() == "true"
PDF_TESSERACT_LANG = os.getenv("PDF_TESSERACT_LANG", "eng")
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "tesseract")
PDF_OCR_RENDER_DPI = int(os.getenv("PDF_OCR_RENDER_DPI", "300"))
PDF_MIN_IMG_SIZE_BYTES = int(os.getenv("PDF_MIN_IMG_SIZE_BYTES", "3072"))
MAX_TOKENS_PER_CHUNK = int(os.getenv("MAX_TOKENS_PER_CHUNK", "512"))
MIN_TOKENS_PER_CHUNK = int(os.getenv("MIN_TOKENS_PER_CHUNK", "100"))
OVERLAP_SENTENCES = int(os.getenv("OVERLAP_SENTENCES", "2"))
TMPDIR = os.getenv("TMPDIR", None)
PUT_RETRIES = int(os.getenv("PUT_RETRIES", "3"))
PUT_BACKOFF = float(os.getenv("PUT_BACKOFF", "0.3"))
ENC_NAME = os.getenv("ENC_NAME", "cl100k_base")

# validations (fail-fast)
if STORAGE not in ("s3", "local"):
    raise SystemExit(f"Invalid STORAGE: {STORAGE!r}. Use 's3' or 'local'.")

if STORAGE == "s3" and not S3_BUCKET:
    raise SystemExit("S3_BUCKET env required when STORAGE='s3'")

# -------------------------
# Logging helpers
# -------------------------
def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def log(level: str, event: str, **extra):
    obj = {"ts": now_ts(), "level": level, "event": event}
    obj.update(extra)
    line = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    if level.lower() in ("error", "err", "critical"):
        print(line, file=sys.stderr, flush=True)
    else:
        print(line, flush=True)

# -------------------------
# Optional libs (lazy failures handled)
# -------------------------
try:
    from PIL import Image
except Exception:
    Image = None

try:
    import pytesseract
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
except Exception:
    pytesseract = None

_fitz = None
_pdfplumber = None

def import_fitz():
    global _fitz
    if _fitz is not None:
        return _fitz
    try:
        import fitz as _m
        _fitz = _m
        return _fitz
    except Exception:
        try:
            import pymupdf as _m
            _fitz = _m
            return _fitz
        except Exception as e:
            raise RuntimeError("PyMuPDF (fitz/pymupdf) not available: " + str(e))

def import_pdfplumber():
    global _pdfplumber
    if _pdfplumber is not None:
        return _pdfplumber
    try:
        import pdfplumber as _p
        _pdfplumber = _p
        return _pdfplumber
    except Exception as e:
        raise RuntimeError("pdfplumber not available: " + str(e))

try:
    import tiktoken
except Exception:
    tiktoken = None

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
    final_key is the object key (no bucket prefix).
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
            # best-effort cleanup
            try:
                client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
            except Exception:
                pass
            if attempt < PUT_RETRIES:
                time.sleep(PUT_BACKOFF * attempt)
                continue
            raise last_exc

# -------------------------
# Atomic writes (S3 and local)
# -------------------------
def write_text_atomic_to_chunked(rel_path: str, text: str) -> None:
    rel_path = rel_path.lstrip("/")
    final_key = f"{CHUNKED_PREFIX.rstrip('/')}/{rel_path}"
    if STORAGE == "s3":
        _s3_atomic_put(final_key, text.encode("utf-8"), content_type="application/json")
    else:
        p = Path(final_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(p.parent), suffix=".tmp")
        try:
            tmp.write(text.encode("utf-8"))
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            Path(tmp.name).replace(p)
        finally:
            if os.path.exists(tmp.name):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass

def write_bytes_atomic_to_chunked(rel_path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    rel_path = rel_path.lstrip("/")
    final_key = f"{CHUNKED_PREFIX.rstrip('/')}/{rel_path}"
    if STORAGE == "s3":
        _s3_atomic_put(final_key, data, content_type=content_type)
    else:
        p = Path(final_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(p.parent), suffix=".tmp")
        try:
            tmp.write(data)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            Path(tmp.name).replace(p)
        finally:
            if os.path.exists(tmp.name):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass

def compute_sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

# -------------------------
# Tokenization & splitting (deterministic)
# -------------------------
class TokenEncoder:
    def __init__(self, enc_name: str = ENC_NAME):
        if tiktoken is not None:
            try:
                enc = tiktoken.get_encoding(enc_name)
                self.encode = lambda txt: enc.encode(txt)
                self.decode = lambda toks: enc.decode(toks)
                self.backend = "tiktoken"
                return
            except Exception:
                pass
        self.encode = lambda txt: txt.split()
        self.decode = lambda toks: " ".join(str(x) for x in toks)
        self.backend = "whitespace"

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

def split_into_windows(text: str, max_tokens: int = MAX_TOKENS_PER_CHUNK,
                       min_tokens: int = MIN_TOKENS_PER_CHUNK,
                       overlap_sentences: int = OVERLAP_SENTENCES,
                       encoder: Optional[TokenEncoder] = None):
    if encoder is None:
        encoder = TokenEncoder()
    text = re.sub(r'\s+', ' ', text).strip()
    if not text:
        yield {"window_index": 0, "text": "", "token_count": 0, "token_start": 0, "token_end": 0}
        return
    spans = sentence_spans(text)
    sent_items = []
    token_cursor = 0
    for s, sc, ec in spans:
        toks = encoder.encode(s)
        tok_len = len(toks) if isinstance(toks, (list,tuple)) else 1
        sent_items.append({"text": s, "start_char": sc, "end_char": ec, "token_len": tok_len, "tokens": toks})
    if not sent_items:
        toks = encoder.encode(text)
        yield {"window_index": 0, "text": text, "token_count": len(toks) if isinstance(toks,(list,tuple)) else 1, "token_start": 0, "token_end": len(toks) if isinstance(toks,(list,tuple)) else 1}
        return
    for si in sent_items:
        si["token_start_idx"] = token_cursor
        si["token_end_idx"] = token_cursor + si["token_len"]
        token_cursor = si["token_end_idx"]
    windows = []
    i = 0
    window_index = 0
    while i < len(sent_items):
        cur_token_count = 0
        chunk_sent_texts = []
        chunk_token_start = sent_items[i]["token_start_idx"]
        chunk_token_end = chunk_token_start
        start_i = i
        while i < len(sent_items):
            sent = sent_items[i]
            if cur_token_count + sent["token_len"] > max_tokens:
                break
            chunk_sent_texts.append(sent["text"])
            cur_token_count += sent["token_len"]
            chunk_token_end = sent.get("token_end_idx", chunk_token_start + cur_token_count)
            i += 1
        if not chunk_sent_texts:
            sent = sent_items[i]
            toks = sent["tokens"]
            if isinstance(toks, (list,tuple)):
                truncated = toks[:max_tokens]
                chunk_text = encoder.decode(truncated)
                cur_token_count = len(truncated)
                remaining = toks[max_tokens:]
                if remaining:
                    sent_items[i]["tokens"] = remaining
                    sent_items[i]["token_len"] = len(remaining)
                    sent_items[i]["token_start_idx"] = None
                    sent_items[i]["token_end_idx"] = None
                else:
                    i += 1
            else:
                chunk_text = sent["text"]
                cur_token_count = sent.get("token_len",1)
                i += 1
            chunk_token_end = chunk_token_start + cur_token_count
        else:
            chunk_text = " ".join(chunk_sent_texts).strip()
        chunk_meta = {
            "window_index": window_index,
            "text": chunk_text,
            "token_count": int(cur_token_count),
            "token_start": int(chunk_token_start),
            "token_end": int(chunk_token_end),
            "start_sentence_idx": int(start_i),
            "end_sentence_idx": int(i)
        }
        window_index += 1
        if windows and chunk_meta["token_count"] < min_tokens:
            prev = windows[-1]
            prev["text"] = prev["text"] + " " + chunk_meta["text"]
            prev["token_count"] = prev["token_count"] + chunk_meta["token_count"]
            prev["token_end"] = chunk_meta["token_end"]
            prev["end_sentence_idx"] = chunk_meta["end_sentence_idx"]
        else:
            windows.append(chunk_meta)
        new_i = max(start_i + 1, (chunk_meta["end_sentence_idx"] - overlap_sentences))
        i = new_i
    for w in windows:
        yield w

def derive_semantic_region(cumulative_before: int, chunk_tok: int, total_tokens: int, page_num: int = 1, total_pages: int = 1) -> str:
    try:
        if not total_tokens or total_tokens <= 0:
            if page_num == 1:
                return "intro"
            if page_num == total_pages:
                return "footer"
            return "middle"
        midpoint = (float(cumulative_before) + (float(chunk_tok)/2.0)) / float(total_tokens)
        if midpoint < 0.10:
            return "intro"
        if midpoint < 0.30:
            return "early"
        if midpoint < 0.70:
            return "middle"
        if midpoint < 0.90:
            return "late"
        return "footer"
    except Exception:
        return "unknown"

# -------------------------
# PDF extraction helpers (fitz + pdfplumber + OCR)
# -------------------------
def download_to_tempfile(bytes_data: bytes, suffix: str = ".pdf") -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=TMPDIR)
    try:
        tmp.write(bytes_data)
        tmp.flush()
        tmp.close()
        return tmp.name
    finally:
        try:
            tmp.close()
        except Exception:
            pass

def crop_page_to_png_bytes(page, bbox: Tuple[float,float,float,float], dpi:int = PDF_OCR_RENDER_DPI) -> bytes:
    fitz = import_fitz()
    rect = fitz.Rect(bbox)
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
    pix = page.get_pixmap(matrix=mat, clip=rect, alpha=False)
    return pix.tobytes("png")

def reflow_and_clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'[\x00-\x1F]+', ' ', text)
    text = text.replace('\r\n','\n').replace('\r','\n')
    text = re.sub(r'\n{2,}', '\n\n', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_page_clean_and_figures(pdf_path: str, pageno: int) -> Tuple[str, List[str]]:
    """
    Extract main flowing text (attempt column reflow) and extract figure/table OCR text when appropriate.
    Returns (clean_text, list_of_figures_texts)
    """
    fitz = import_fitz()
    pdfplumb = import_pdfplumber()
    doc = fitz.open(pdf_path)
    plumb = pdfplumb.open(pdf_path)
    try:
        page = doc[pageno]
        p_plumb = plumb.pages[pageno]
    except Exception:
        plumb.close(); doc.close()
        raise
    blocks = page.get_text("dict").get("blocks", [])
    text_blocks = []
    image_bboxes = []
    for b in blocks:
        if b.get("type") == 0:
            text = ""
            for line in b.get("lines", []):
                spans = [s.get("text","") for s in line.get("spans",[])]
                text += " ".join(spans) + "\n"
            text_blocks.append({"bbox": tuple(b.get("bbox")), "text": text.strip()})
        elif b.get("type") == 1:
            bbox = tuple(b.get("bbox"))
            try:
                png = crop_page_to_png_bytes(page, bbox, dpi=PDF_OCR_RENDER_DPI)
                if len(png) >= PDF_MIN_IMG_SIZE_BYTES:
                    image_bboxes.append(bbox)
            except Exception:
                continue
    try:
        tables = p_plumb.find_tables() or []
    except Exception:
        tables = []
    table_bboxes = [tuple(t.bbox) for t in tables] if tables else []
    figure_bboxes = table_bboxes + image_bboxes

    # classify text blocks vs figure captions
    content_blocks = []
    caption_map: Dict[Tuple[float,float,float,float], List[str]] = {}
    for tb in text_blocks:
        tb_bbox = tb["bbox"]
        overlapped = False
        for fb in figure_bboxes:
            xa0, ya0, xa1, ya1 = tb_bbox
            xb0, yb0, xb1, yb1 = fb
            inter_w = max(0, min(xa1, xb1) - max(xa0, xb0))
            inter_h = max(0, min(ya1, yb1) - max(ya0, yb0))
            inter_area = inter_w * inter_h
            a_area = max(1.0, (xa1-xa0)*(ya1-ya0))
            if inter_area / a_area > 0.25:
                overlapped = True
                # caption if appears below the figure and close
                if ya0 >= yb1 and (ya0 - yb1) < 80:
                    caption_map.setdefault(fb, []).append(tb["text"])
                break
        if not overlapped:
            content_blocks.append(tb)

    # reflow heuristics: left-to-right then top-to-bottom grouping
    if not content_blocks:
        clean_text = ""
    else:
        centers = [(((b["bbox"][0]+b["bbox"][2])/2.0), i) for i,b in enumerate(content_blocks)]
        centers.sort(key=lambda x: x[0])
        xs = [c for c,_ in centers]
        gaps = [xs[i+1]-xs[i] for i in range(len(xs)-1)] or [0]
        med_gap = sorted(gaps)[len(gaps)//2] if gaps else 0
        if med_gap == 0:
            med_gap = max(gaps) if gaps else 50
        split_idxs = [i for i,g in enumerate(gaps) if g > med_gap * 1.5]
        groups = []
        start = 0
        for si in split_idxs:
            group_idxs = [centers[j][1] for j in range(start, si+1)]
            groups.append([content_blocks[k] for k in group_idxs])
            start = si+1
        group_idxs = [centers[j][1] for j in range(start, len(centers))]
        groups.append([content_blocks[k] for k in group_idxs])
        col_texts = []
        for col in groups:
            col_sorted = sorted(col, key=lambda b: b["bbox"][1])
            pieces = []
            prev_y = None
            for b in col_sorted:
                y0 = b["bbox"][1]
                if prev_y is None or (y0 - prev_y) > 50:
                    pieces.append(b["text"].strip())
                else:
                    pieces.append(" " + b["text"].strip())
                prev_y = b["bbox"][3]
            col_texts.append("\n\n".join(pieces).strip())
        clean_text = "\n\n".join([ct for ct in col_texts if ct]).strip()
    clean_text = reflow_and_clean_text(clean_text)

    figures_texts: List[str] = []
    # extract tables text
    for t in tables:
        try:
            rows = t.extract() if hasattr(t, "extract") else t.extract_table() if hasattr(t, "extract_table") else None
            if rows:
                lines = []
                for row in rows:
                    lines.append("\t".join([str(c) if c is not None else "" for c in row]))
                figures_texts.append("\n".join(lines))
        except Exception:
            continue
    # OCR image regions if needed
    for fb in image_bboxes:
        try:
            png_bytes = crop_page_to_png_bytes(page, fb, dpi=PDF_OCR_RENDER_DPI)
            if len(png_bytes) < PDF_MIN_IMG_SIZE_BYTES:
                continue
            ocr_text = ""
            if pytesseract is not None and Image is not None:
                try:
                    img = Image.open(io.BytesIO(png_bytes))
                    ocr_text = pytesseract.image_to_string(img, lang=PDF_TESSERACT_LANG)
                except Exception:
                    ocr_text = ""
            capt = caption_map.get(fb, [])
            combined = ("\n".join(capt) + "\n" + ocr_text).strip() if capt else (ocr_text or "")
            combined = reflow_and_clean_text(combined)
            if combined:
                figures_texts.append(combined)
        except Exception:
            continue

    plumb.close(); doc.close()
    return clean_text, figures_texts

# -------------------------
# Chunk path & manifest helpers
# -------------------------
def _chunk_rel_for_doc(document_id: str) -> str:
    return f"{CHUNKED_SCHEMA.rstrip('/')}/{document_id}.chunks.jsonl"

def _chunk_exists(document_id: str) -> bool:
    rel = _chunk_rel_for_doc(document_id)
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
    # raw_key is either S3 key or local path returned by router; manifest stored alongside raw by appending .manifest.json
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
        body = json.dumps(manifest_obj, ensure_ascii=False).encode("utf-8")
        _s3_atomic_put(raw_manifest_key, body, content_type="application/json")
    else:
        p = Path(raw_manifest_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("wb") as fh:
            fh.write(json.dumps(manifest_obj, ensure_ascii=False, indent=2).encode("utf-8"))
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(p)

def _write_chunks_and_extend_raw_manifest(document_id: str, chunks: List[Dict[str, Any]], raw_key: str, raw_sha: str, original_url: Optional[str] = None) -> Tuple[str, int, str]:
    """
    Idempotent: compute JSONL locally, compute sha; if raw_manifest.chunked.chunked_sha256 == sha && not FORCE_OVERWRITE then skip writes.
    Otherwise atomically write chunk JSONL to CHUNKED_PREFIX and update raw manifest with minimal changes.
    Returns (chunk_file, size_bytes, sha)
    """
    rel = _chunk_rel_for_doc(document_id)
    jsonl_text = "\n".join(json.dumps(c, ensure_ascii=False, sort_keys=True) for c in chunks) + "\n"
    jsonl_bytes = jsonl_text.encode("utf-8")
    sha = compute_sha256_bytes(jsonl_bytes)
    size = len(jsonl_bytes)

    raw_manifest_key = _raw_manifest_key_for_raw_key(raw_key)
    existing = _read_raw_manifest(raw_manifest_key) or {}
    existing_chunked = existing.get("chunked", {})
    if existing_chunked and existing_chunked.get("chunked_sha256") == sha and not FORCE_OVERWRITE:
        log("info", "raw_manifest_already_up_to_date", raw_manifest=raw_manifest_key, chunk_rel=rel, chunked_sha256=sha)
        if STORAGE == "s3":
            chunk_file = f"s3://{S3_BUCKET}/{CHUNKED_PREFIX.rstrip('/')}/{rel}"
        else:
            chunk_file = str(Path(CHUNKED_PREFIX) / rel)
        return chunk_file, size, sha

    # write chunk file atomically
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
        "schema_version": CHUNKED_SCHEMA,
        "parser_version": PARSER_VERSION,
        "ingest_time": now_ts(),
        "chunk_count": len(chunks),
        "chunked_sha256": sha,
        "chunked_size_bytes": size
    }

    existing.setdefault("file_hash", existing.get("file_hash") or raw_sha)
    existing.setdefault("timestamp", existing.get("timestamp") or now_ts())
    existing["parser_version"] = PARSER_VERSION
    if original_url:
        existing["original_url"] = original_url
    existing["chunked"] = chunked_meta
    existing["saved_chunks"] = len(chunks)
    existing["chunked_manifest_written_at"] = now_ts()

    # atomic manifest write
    try:
        _write_raw_manifest(raw_manifest_key, existing)
    except Exception as e:
        log("error", "raw_manifest_write_failed", raw_manifest=raw_manifest_key, error=str(e))
        raise

    log("info", "raw_manifest_extended", raw_manifest=raw_manifest_key, chunk_file=chunk_file, chunks=len(chunks), sha256=sha, size=size)
    return chunk_file, size, sha

# -------------------------
# parse_file: main entrypoint (idempotent)
# -------------------------
def parse_file(key: str, manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse raw PDF bytes referenced by `key` and extend raw manifest with chunk metadata.
    Returns: {"saved_chunks": int, "skipped": bool, ...}
    """
    start = time.perf_counter()
    tmp_pdf_path: Optional[str] = None
    try:
        # basic pre-checks
        if STORAGE == "s3" and not S3_BUCKET:
            return {"saved_chunks": 0, "error": "S3_BUCKET not configured"}
        if Image is None:
            return {"saved_chunks": 0, "error": "Pillow library not available (required)"}
        try:
            import_fitz(); import_pdfplumber()
        except Exception as e:
            return {"saved_chunks": 0, "error": f"pdf libs missing: {str(e)}"}

        # read raw bytes
        try:
            raw_bytes = _read_raw_bytes(key)
        except Exception as e:
            tb = traceback.format_exc()
            log("error", "read_failed", key=key, error=str(e), traceback=tb)
            return {"saved_chunks": 0, "error": f"read_failed: {str(e)}"}

        raw_sha = compute_sha256_bytes(raw_bytes)
        document_id = manifest.get("file_hash") or raw_sha

        # quick idempotency check: chunk file exists
        if not FORCE_OVERWRITE and _chunk_exists(document_id):
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "skip_existing_chunks", document_id=document_id, key=key, duration_ms=duration_ms)
            return {"saved_chunks": 0, "skipped": True}

        # write raw bytes to tempfile for pdf libs
        tmp_pdf_path = download_to_tempfile(raw_bytes, suffix=".pdf")

        # open pdf
        try:
            fitz = import_fitz()
            doc = fitz.open(tmp_pdf_path)
        except Exception as e:
            tb = traceback.format_exc()
            log("error", "open_pdf_failed", key=key, error=str(e), traceback=tb)
            try:
                if tmp_pdf_path and os.path.exists(tmp_pdf_path):
                    os.unlink(tmp_pdf_path)
            except Exception:
                pass
            return {"saved_chunks": 0, "error": f"open_pdf_failed: {str(e)}"}

        encoder = TokenEncoder()
        total_pages = len(doc)
        page_infos: List[Dict[str, Any]] = []

        # extract content per page
        for pageno in range(total_pages):
            try:
                clean_text, figures = extract_page_clean_and_figures(tmp_pdf_path, pageno)
            except Exception as e:
                log("warn", "page_extract_failed", page=pageno+1, error=str(e))
                clean_text, figures = "", []
            try:
                page_token_ct = len(encoder.encode(clean_text)) if clean_text else 0
            except Exception:
                page_token_ct = len(clean_text.split()) if clean_text else 0
            page_infos.append({"clean_text": clean_text, "figures": figures, "page_token_count": page_token_ct})

        total_document_tokens = sum(p.get("page_token_count", 0) for p in page_infos)
        cumulative_tokens = 0
        chunks: List[Dict[str, Any]] = []
        source_url_authoritative = manifest.get("original_url") or (f"s3://{S3_BUCKET}/{key}" if STORAGE == "s3" else key)
        provenance_base = {"raw_sha256": raw_sha, "raw_key": key, "original_url": manifest.get("original_url")}

        for pageno in range(total_pages):
            info = page_infos[pageno]
            clean_text = info.get("clean_text", "") or ""
            figures_texts = info.get("figures", []) or []
            used_ocr = bool(figures_texts) and (pytesseract is not None)
            if not clean_text:
                chunk_id = f"{document_id}_p{pageno+1}_0000"
                region = derive_semantic_region(cumulative_tokens, 0, total_document_tokens, pageno+1, total_pages)
                payload = {
                    "document_id": document_id,
                    "chunk_id": chunk_id,
                    "chunk_index": 0,
                    "chunk_type": "pdf_page",
                    "text": "",
                    "token_count": 0,
                    "token_range": [0, 0],
                    "document_total_tokens": total_document_tokens,
                    "semantic_region": region,
                    "figures": figures_texts,
                    "file_type": "application/pdf",
                    "source_url": source_url_authoritative,
                    "page_number": pageno+1,
                    "ingest_time": now_ts(),
                    "parser_version": PARSER_VERSION,
                    "used_ocr": used_ocr,
                    "original_manifest": manifest,
                    "provenance": provenance_base,
                    "embedding": None
                }
                chunks.append(payload)
                continue

            windows = list(split_into_windows(clean_text, max_tokens=MAX_TOKENS_PER_CHUNK, min_tokens=MIN_TOKENS_PER_CHUNK, overlap_sentences=OVERLAP_SENTENCES, encoder=encoder))
            for idx, w in enumerate(windows):
                token_count = int(w.get("token_count", 0))
                chunk_id = f"{document_id}_p{pageno+1}_{str(idx).zfill(4)}"
                region = derive_semantic_region(cumulative_tokens, token_count, total_document_tokens, pageno+1, total_pages)
                payload = {
                    "document_id": document_id,
                    "chunk_id": chunk_id,
                    "chunk_index": idx,
                    "chunk_type": "pdf_page_window",
                    "text": w.get("text", ""),
                    "token_count": token_count,
                    "token_range": [int(w.get("token_start", 0)), int(w.get("token_end", 0))],
                    "document_total_tokens": total_document_tokens,
                    "semantic_region": region,
                    "figures": figures_texts,
                    "file_type": "application/pdf",
                    "source_url": source_url_authoritative,
                    "page_number": pageno+1,
                    "ingest_time": now_ts(),
                    "parser_version": PARSER_VERSION,
                    "used_ocr": used_ocr,
                    "original_manifest": manifest,
                    "provenance": provenance_base,
                    "embedding": None
                }
                chunks.append(payload)
                cumulative_tokens += token_count

        # cleanup pdf doc + tmp
        try:
            doc.close()
        except Exception:
            pass
        try:
            if tmp_pdf_path and os.path.exists(tmp_pdf_path):
                os.unlink(tmp_pdf_path)
        except Exception:
            pass

        if not chunks:
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "no_chunks", key=key, duration_ms=duration_ms)
            return {"saved_chunks": 0}

        # race check after parse
        if not FORCE_OVERWRITE and _chunk_exists(document_id):
            duration_ms = int((time.perf_counter() - start) * 1000)
            log("info", "race_skip_existing_after_parse", document_id=document_id, key=key, duration_ms=duration_ms)
            return {"saved_chunks": 0, "skipped": True}

        # write chunk file and extend raw manifest atomically
        try:
            chunk_file, size, sha = _write_chunks_and_extend_raw_manifest(document_id, chunks, key, raw_sha, manifest.get("original_url"))
        except Exception as e:
            tb = traceback.format_exc()
            log("error", "write_chunks_failed", key=key, error=str(e), traceback=tb)
            return {"saved_chunks": 0, "error": str(e)}

        duration_ms = int((time.perf_counter() - start) * 1000)
        log("info", "parse_complete", key=key, document_id=document_id, saved_chunks=len(chunks), chunk_file=chunk_file, chunk_size_bytes=size, chunked_sha256=sha, duration_ms=duration_ms)
        # final sanity: returned saved_chunks must equal len(chunks)
        return {"saved_chunks": len(chunks)}
    except Exception as exc:
        tb = traceback.format_exc()
        log("error", "parse_exception", key=key, error=str(exc), traceback=tb)
        return {"saved_chunks": 0, "error": str(exc)}
    finally:
        try:
            if tmp_pdf_path and os.path.exists(tmp_pdf_path):
                os.unlink(tmp_pdf_path)
        except Exception:
            pass

# -------------------------
# internal read helper (bottom)
# -------------------------
def _read_raw_bytes(key: str) -> bytes:
    """
    Read raw bytes for key. If STORAGE='s3' key is the object key; otherwise key may be a filesystem path
    or a filename relative to RAW_PREFIX.
    """
    if STORAGE == "s3":
        s3 = _ensure_s3_client()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return resp["Body"].read()
    else:
        p = Path(key)
        if not p.exists():
            candidate = Path(RAW_PREFIX) / Path(key).name
            if candidate.exists():
                p = candidate
        with p.open("rb") as fh:
            return fh.read()

# -------------------------
# module loaded notice
# -------------------------
if __name__ == "__main__":
    log("info", "module_loaded", module=__file__, parser_version=PARSER_VERSION)
