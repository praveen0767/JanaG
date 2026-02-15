from __future__ import annotations
import os
import sys
import json
import time
import logging
import datetime
import re
import hashlib
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_log = logging.getLogger("core")

def jlog(msg: Any) -> None:
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    if isinstance(msg, str):
        _log.info(f"{ts} | INFO  | core | {msg}")
        return
    level = str(msg.get("level", "INFO")).upper()
    event = msg.get("event", "")
    extras = " ".join(f"{k}={msg[k]}" for k in sorted(k for k in msg.keys() if k not in {"level", "event"}))
    line = f"{ts} | {level:<5} | core |"
    if event:
        line += f" {event}"
    if extras:
        line += f" | {extras}"
    if level == "CRITICAL":
        _log.critical(line)
    elif level in {"ERROR", "ERR"}:
        _log.error(line)
    elif level in {"WARN", "WARNING"}:
        _log.warning(line)
    else:
        _log.info(line)

try:
    import boto3
except Exception as e:
    jlog({"level":"CRITICAL","event":"boto3_missing","detail":str(e)})
    raise SystemExit(2)

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None

def _env(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k)
    return v if v is not None else default

AWS_REGION = _env("AWS_REGION")
if not AWS_REGION:
    jlog({"level":"CRITICAL","event":"env_missing","hint":"AWS_REGION must be set"})
    raise SystemExit(10)

EMBED_MODEL_ID = _env("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
try:
    EMBED_DIM = int(_env("EMBED_DIM", "1024"))
except Exception:
    jlog({"level":"CRITICAL","event":"invalid_env","hint":"EMBED_DIM must be integer"})
    raise SystemExit(11)

PG_HOST = _env("PG_HOST")
PG_PORT = int(_env("PG_PORT", "5432"))
PG_USER = _env("PG_USER", "postgres")
PG_PASSWORD = _env("PG_PASSWORD")
PG_DB = _env("PG_DB", "postgres")
PG_TABLE_NAME = _env("PG_INDEX_NAME", "civic_chunks")
RAW_K = int(_env("RAW_K", "50"))
FINAL_K = int(_env("FINAL_K", "5"))
MIN_SIMILARITY = float(_env("MIN_SIMILARITY", "0.25"))
ASR_CONF_THRESHOLD = float(_env("ASR_CONF_THRESHOLD", "0.25"))
EMBED_SEARCH_BUDGET_SEC = float(_env("EMBED_SEARCH_BUDGET_SEC", "2.5"))
GEN_BUDGET_SEC = float(_env("GEN_BUDGET_SEC", "4.0"))
BEDROCK_MODEL_ID = _env("BEDROCK_MODEL_ID") or "qwen.qwen3-next-80b-a3b"
BEDROCK_MAX_RETRIES = int(_env("BEDROCK_MAX_RETRIES", "1"))
BEDROCK_RETRY_BASE_DELAY_SEC = float(_env("BEDROCK_RETRY_BASE_DELAY_SEC", "0.25"))
AUDIT_S3_BUCKET = _env("AUDIT_S3_BUCKET")
AUDIT_S3_PREFIX = _env("AUDIT_S3_PREFIX", "audits/")
VALIDATION_STRICT = (_env("VALIDATION_STRICT", "0").strip() != "0")

ALLOWED_LANGUAGES = set(["en", "hi", "ta", "bn", "te", "ml", "kn", "mr", "gu"])
ALLOWED_CHANNELS = set(["web", "sms", "voice"])

INTENT_BLOCKLIST_PATTERNS = {
    "medical": re.compile(r"\b(medic(al|ine|ation)|prescrib|diagnos|symptom|treatment|clinic)\b", re.I),
    "legal": re.compile(r"\b(attorney|sue|lawsuit|contract|custody|divorce|legal advice|crime|sentence)\b", re.I),
}
GUIDANCE_KEYS = {
    "medical": "refusal_medical",
    "legal": "refusal_legal",
    "asr_low_confidence": "refusal_asr_low_confidence",
    "insufficient_evidence": "refusal_insufficient_evidence",
    "invalid_request": "refusal_invalid_request",
}

CITATION_PAT = re.compile(r"\[(\d+)\]\s*$")
DISALLOWED_SUBSTRINGS = ("http://", "https://", "www.", "file://")

_s3_client = None
_bedrock = None
_pg_conn = None

def init_s3_client():
    global _s3_client
    if _s3_client is not None:
        return _s3_client
    try:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    except Exception as e:
        jlog({"level":"WARN","event":"s3_client_init_failed","detail":str(e)})
        _s3_client = None
    return _s3_client

def _write_audit(record: Dict[str, Any]) -> None:
    if not AUDIT_S3_BUCKET:
        jlog("audit skipped: no bucket configured")
        return
    client = init_s3_client()
    if client is None:
        jlog("audit skipped: s3 unavailable")
        return
    try:
        safe_query = record.get("query")
        if isinstance(safe_query, str):
            hashed = hashlib.sha256(safe_query.encode("utf-8")).hexdigest()
            record["query_hash"] = hashed
            record.pop("query", None)
        date_prefix = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        key = f"{AUDIT_S3_PREFIX.rstrip('/')}/{date_prefix}/{record.get('request_id')}.json"
        client.put_object(Bucket=AUDIT_S3_BUCKET, Key=key, Body=json.dumps(record, default=str).encode("utf-8"))
        jlog(f"audit written | request_id={record.get('request_id')}")
    except Exception as e:
        jlog({"level":"WARN","event":"audit_write_failed","request_id":record.get("request_id"),"detail":str(e)})

def init_bedrock_client():
    global _bedrock
    if _bedrock is not None:
        return _bedrock
    try:
        _bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    except Exception as e:
        jlog({"level":"CRITICAL","event":"bedrock_client_failed","detail":str(e)})
        _bedrock = None
        raise
    jlog("bedrock client initialized")
    return _bedrock

def get_embedding_from_bedrock(text: str) -> List[float]:
    client = init_bedrock_client()
    body = json.dumps({"inputText": text})
    resp = client.invoke_model(modelId=EMBED_MODEL_ID, body=body, contentType="application/json")
    payload_stream = resp.get("body")
    raw = payload_stream.read() if hasattr(payload_stream, "read") else payload_stream
    mr = json.loads(raw)
    emb = mr.get("embedding") or mr.get("embeddings") or mr.get("vector")
    if not isinstance(emb, list):
        jlog({"level":"ERROR","event":"bedrock_no_embedding","response_sample":str(mr)[:200]})
        raise RuntimeError("bedrock returned no embedding list")
    if len(emb) != EMBED_DIM:
        jlog({"level":"ERROR","event":"bedrock_dim_mismatch","expected":EMBED_DIM,"received":len(emb)})
        raise RuntimeError("embedding_dim_mismatch")
    return [float(x) for x in emb]

def init_pg_connection():
    global _pg_conn
    if _pg_conn is not None:
        return _pg_conn
    if not PG_HOST or not PG_PASSWORD or psycopg is None:
        jlog("postgres not configured; skipping pgvector")
        return None
    conninfo = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}"
    conn = psycopg.connect(conninfo, autocommit=True, row_factory=dict_row)
    _pg_conn = conn
    jlog(f"pg connect ok | host={PG_HOST} db={PG_DB}")
    try:
        _check_table_and_hnsw_index(conn, PG_TABLE_NAME)
    except Exception as e:
        jlog({"level":"WARN","event":"index_check_failed","detail":str(e)})
    return _pg_conn

def _check_table_and_hnsw_index(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) AS reg", (table_name,))
        tr = cur.fetchone()
        if not tr or not tr.get("reg"):
            raise RuntimeError(f"table_missing:{table_name}")
        cur.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = %s AND indexdef ILIKE '%%USING hnsw%%';
            """,
            (table_name,),
        )
        rows = cur.fetchall()
        if not rows:
            jlog("hnsw index missing")
        else:
            jlog(f"hnsw index ok | count={len(rows)}")

def _format_vector_literal(vec: List[float]) -> str:
    pieces = []
    for x in vec:
        fx = float(x)
        pieces.append(format(fx, ".17g"))
    return "[" + ",".join(pieces) + "]"

META_KEY_RE = re.compile(r"^[A-Za-z0-9_]+$")

def pgvector_search(query_emb: List[float], filters: Dict[str, str], raw_k: int) -> List[Dict[str, Any]]:
    conn = init_pg_connection()
    if conn is None:
        return []
    where_clauses = []
    filter_params = []
    if isinstance(filters, dict):
        for k in sorted(filters.keys()):
            if not isinstance(k, str) or not META_KEY_RE.match(k):
                jlog(f"filter key skipped | key={k}")
                continue
            where_clauses.append("meta->>%s = %s")
            filter_params.extend([k, filters[k]])
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    vec_literal = _format_vector_literal(query_emb)
    sql = f"""
    SELECT document_id, chunk_id, chunk_index, content, meta, source_url, page_number,
           (embedding <-> %s::vector) AS distance
    FROM {PG_TABLE_NAME}
    {where_sql}
    ORDER BY embedding <-> %s::vector
    LIMIT %s;
    """
    final_params = [vec_literal]
    final_params.extend(filter_params)
    final_params.extend([vec_literal, raw_k])
    with conn.cursor() as cur:
        cur.execute(sql, tuple(final_params))
        rows = cur.fetchall()
    results = []
    for r in rows:
        results.append(
            {
                "document_id": r.get("document_id"),
                "chunk_id": r.get("chunk_id"),
                "chunk_index": r.get("chunk_index"),
                "text": r.get("content") or "",
                "meta": r.get("meta") or {},
                "source_url": r.get("source_url"),
                "page_number": r.get("page_number"),
                "distance": float(r.get("distance")) if r.get("distance") is not None else None,
            }
        )
    return results

def compute_similarity_from_distance(distance: Optional[float]) -> float:
    if distance is None:
        return 0.0
    try:
        return 1.0 / (1.0 + float(distance))
    except Exception:
        return 0.0

def trust_weight_for(meta: Dict[str, Any]) -> float:
    tl = (meta or {}).get("trust_level") or (meta or {}).get("trust") or ""
    if isinstance(tl, str):
        mapping = {"gov": 1.0, "government": 1.0, "implementing_agency": 0.95, "agency": 0.95, "ngo": 0.8, "news": 0.6}
        return float(mapping.get(tl.lower(), 1.0))
    return 1.0

def re_rank_and_select(candidates: List[Dict[str, Any]], final_k: int) -> List[Dict[str, Any]]:
    scored = []
    for c in candidates:
        dist = c.get("distance")
        sim = compute_similarity_from_distance(dist)
        tw = trust_weight_for(c.get("meta") or {})
        final_score = sim * tw
        c["similarity"] = sim
        c["trust_weight"] = tw
        c["final_score"] = final_score
        scored.append(c)
    scored.sort(key=lambda x: (-x["final_score"], -x["similarity"], x.get("chunk_id", "")))
    return scored[:final_k]

def _normalize_text_key(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def dedupe_candidates_keep_nearest(candidates: List[Dict[str, Any]], max_keep: int) -> List[Dict[str, Any]]:
    seen_keys = set()
    deduped = []
    for c in candidates:
        key = _normalize_text_key(c.get("text"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(c)
        if len(deduped) >= max_keep:
            break
    return deduped

def retrieve(event: Dict[str, Any]) -> Dict[str, Any]:
    start = time.time()
    request_id = event.get("request_id") or f"r-{int(start * 1000)}"
    query_text = (event.get("query") or event.get("question") or "").strip()
    if not query_text:
        jlog({"level":"ERROR","event":"invalid_request","request_id":request_id})
        return {"request_id": request_id, "passages": [], "chunk_ids": [], "top_similarity": 0.0, "error": "invalid_request"}
    top_k = int(event.get("top_k") or FINAL_K)
    raw_k = int(event.get("raw_k") or RAW_K)
    filters = event.get("filters") or {}
    jlog(f"retrieve start | request_id={request_id} | qlen={len(query_text)} | raw_k={raw_k} | top_k={top_k}")
    try:
        emb = get_embedding_from_bedrock(query_text)
    except Exception as e:
        jlog({"level":"ERROR","event":"embed_failed","request_id":request_id,"detail":str(e)})
        return {"request_id": request_id, "passages": [], "chunk_ids": [], "top_similarity": 0.0, "error": "embed_failed"}
    try:
        candidates = pgvector_search(emb, filters, raw_k)
    except Exception as e:
        jlog({"level":"ERROR","event":"vector_search_error","request_id":request_id,"detail":str(e)})
        return {"request_id": request_id, "passages": [], "chunk_ids": [], "top_similarity": 0.0, "error": "vector_search_failed"}
    if not candidates:
        jlog(f"no candidates | request_id={request_id}")
        return {"request_id": request_id, "passages": [], "chunk_ids": [], "top_similarity": 0.0}
    deduped = dedupe_candidates_keep_nearest(candidates, raw_k)
    ranked = re_rank_and_select(deduped, top_k)
    passages = []
    chunk_ids = []
    for i, r in enumerate(ranked):
        passages.append(
            {
                "number": i + 1,
                "chunk_id": r["chunk_id"],
                "document_id": r.get("document_id"),
                "chunk_index": r.get("chunk_index"),
                "text": r.get("text") or "",
                "meta": r.get("meta") or {},
                "source_url": r.get("source_url"),
                "page_number": r.get("page_number"),
                "score": float(r.get("final_score", 0.0)),
                "distance": float(r.get("distance")) if r.get("distance") is not None else None,
            }
        )
        chunk_ids.append(r["chunk_id"])
    top_similarity = passages[0]["score"] if passages else 0.0
    elapsed_ms = int((time.time() - start) * 1000)
    jlog(f"retrieval complete | request_id={request_id} | returned={len(passages)} | top_similarity={top_similarity} | ms={elapsed_ms}")
    return {"request_id": request_id, "passages": passages, "chunk_ids": chunk_ids, "top_similarity": float(top_similarity)}

def call_bedrock(prompt_text: str, model_id: Optional[str] = None) -> str:
    client = init_bedrock_client()
    identifier = model_id or BEDROCK_MODEL_ID
    inference_config = {"maxTokens": int(_env("BEDROCK_MAX_TOKENS") or 256), "temperature": float(_env("BEDROCK_TEMPERATURE") or 0.2), "topP": float(_env("BEDROCK_TOP_P") or 0.95)}
    system = [{"text": ("You are a government assistant. Use the passages. If you cannot answer from passages, reply NOT_ENOUGH_INFORMATION. Each factual sentence must end with a [n] citation.")}]
    messages = [{"role": "user", "content": [{"text": prompt_text}]}]
    try:
        resp = client.converse(modelId=identifier, messages=messages, system=system, inferenceConfig=inference_config)
        out = resp.get("output", {}) or {}
        message = out.get("message") or {}
        content = message.get("content") or []
        if isinstance(content, list) and content:
            for c in content:
                if isinstance(c, dict) and isinstance(c.get("text"), str):
                    return c.get("text").strip()
        return json.dumps(resp)
    except Exception:
        body = json.dumps({"inputText": prompt_text, "inferenceParameters": {"temperature": inference_config["temperature"], "topP": inference_config["topP"], "maxTokens": inference_config["maxTokens"]}}).encode("utf-8")
        resp = init_bedrock_client().invoke_model(modelId=identifier, body=body, contentType="application/json")
        body_stream = resp.get("body")
        raw = body_stream.read() if hasattr(body_stream, "read") else body_stream
        try:
            mr = json.loads(raw)
        except Exception:
            return str(raw)
        for k in ("outputText", "generatedText", "text"):
            v = mr.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return json.dumps(mr)

def _validate_generator_output_and_extract_lines_raw(raw_text: str, passages: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    if not raw_text or not raw_text.strip():
        return "INVALID_OUTPUT", []
    combined = " ".join([l.strip() for l in raw_text.splitlines() if l.strip()])
    if combined.strip().upper() == "NOT_ENOUGH_INFORMATION":
        return "NOT_ENOUGH_INFORMATION", []
    newline_lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    if newline_lines:
        sents = newline_lines
    else:
        sents = re.split(r'(?<=[\.\?\!\u0964\u0965])\s+', combined)
        sents = [s.strip() for s in sents if s.strip()]
        if not sents:
            sents = [combined.strip()]
    max_pass = 0
    for p in passages:
        try:
            n = int(p.get("number", 0))
            if n > max_pass:
                max_pass = n
        except Exception:
            continue
    if max_pass < 1:
        return "INVALID_OUTPUT", []
    placeholder_present = any(re.search(r'\[n\][\.\!\?]?$', s, re.I) for s in sents)
    validated: List[Dict[str, Any]] = []
    explicit_indices = []
    for s in sents:
        m = re.search(r'\[(\d+)\](?:[\.\!\?]?)*$', s)
        if m:
            explicit_indices.append(int(m.group(1)))
    for s in sents:
        t = s.strip()
        if not t:
            continue
        if t.upper() == "NOT_ENOUGH_INFORMATION":
            return "NOT_ENOUGH_INFORMATION", []
    if placeholder_present:
        seq = 1
        for s in sents:
            t = s.strip()
            if not t:
                continue
            if t.upper() == "NOT_ENOUGH_INFORMATION":
                return "NOT_ENOUGH_INFORMATION", []
            if re.search(r'\[n\]([\.\!\?]?$)', t, re.I):
                cited = seq
                t_new = re.sub(r'\[n\]([\.\!\?]?$)', f'[{cited}]\\1', t, flags=re.I)
                seq += 1
            else:
                m = re.search(r'\[(\d+)\](?:[\.\!\?]?)*$', t)
                if not m:
                    return "INVALID_OUTPUT", []
                cited = int(m.group(1))
                t_new = t
            if cited < 1 or cited > max_pass:
                jlog(f"validation failed: citation_out_of_range | line={t_new} | cited={cited} | max_pass={max_pass}")
                return "INVALID_OUTPUT", []
            lower = t_new.lower()
            if any(sub in lower for sub in DISALLOWED_SUBSTRINGS):
                jlog(f"validation failed: disallowed_substring | line={t_new}")
                return "INVALID_OUTPUT", []
            validated.append({"text": t_new})
        if not validated:
            return "INVALID_OUTPUT", []
        return "ACCEPT", validated
    if explicit_indices:
        for s in sents:
            t = s.strip()
            if not t:
                continue
            if t.upper() == "NOT_ENOUGH_INFORMATION":
                return "NOT_ENOUGH_INFORMATION", []
            m = re.search(r'\[(\d+)\](?:[\.\!\?]?)*$', t)
            if m:
                cited = int(m.group(1))
            else:
                nums = re.findall(r'\b(\d{1,3})\b', t)
                cited = None
                for nstr in nums:
                    try:
                        ni = int(nstr)
                        if 1 <= ni <= max_pass:
                            cited = ni
                            break
                    except Exception:
                        continue
                if cited is None:
                    jlog(f"validation failed: missing_sentence_citation | line={t}")
                    return "INVALID_OUTPUT", []
            if cited < 1 or cited > max_pass:
                jlog(f"validation failed: citation_out_of_range | line={t} | cited={cited} | max_pass={max_pass}")
                return "INVALID_OUTPUT", []
            t_new = re.sub(r'\[(\d+)\](?:[\.\!\?]?)*$', '', t).strip()
            appended = f"{t_new} [{cited}]"
            lower = appended.lower()
            if any(sub in lower for sub in DISALLOWED_SUBSTRINGS):
                jlog(f"validation failed: disallowed_substring | line={appended}")
                return "INVALID_OUTPUT", []
            validated.append({"text": appended})
        if not validated:
            return "INVALID_OUTPUT", []
        return "ACCEPT", validated
    nums_anywhere = []
    for s in sents:
        nums_found = re.findall(r'\b(\d{1,3})\b', s)
        if nums_found:
            nums_anywhere.append(True)
        else:
            nums_anywhere.append(False)
    if any(nums_anywhere):
        for s in sents:
            t = s.strip()
            if not t:
                continue
            if t.upper() == "NOT_ENOUGH_INFORMATION":
                return "NOT_ENOUGH_INFORMATION", []
            nums = re.findall(r'\b(\d{1,3})\b', t)
            cited = None
            for nstr in nums:
                try:
                    ni = int(nstr)
                    if 1 <= ni <= max_pass:
                        cited = ni
                        break
                except Exception:
                    continue
            if cited is None:
                cited = 1
            t_new = re.sub(r'\b(?:PASSAGE|PASSAGES|passage|passages)\b[^\d]*', '', t, flags=re.I).strip()
            appended = f"{t_new} [{cited}]"
            lower = appended.lower()
            if any(sub in lower for sub in DISALLOWED_SUBSTRINGS):
                jlog(f"validation failed: disallowed_substring | line={appended}")
                return "INVALID_OUTPUT", []
            validated.append({"text": appended})
        if not validated:
            return "INVALID_OUTPUT", []
        return "ACCEPT", validated
    if len(sents) <= max_pass:
        seq = 1
        for t in sents:
            t = t.strip()
            if not t:
                continue
            if t.upper() == "NOT_ENOUGH_INFORMATION":
                return "NOT_ENOUGH_INFORMATION", []
            appended = f"{t} [{seq}]"
            lower = appended.lower()
            if any(sub in lower for sub in DISALLOWED_SUBSTRINGS):
                jlog(f"validation failed: disallowed_substring | line={appended}")
                return "INVALID_OUTPUT", []
            validated.append({"text": appended})
            seq += 1
        return "ACCEPT", validated
    jlog("validation failed: sentences_gt_passages and no explicit citations")
    return "INVALID_OUTPUT", []

def _hydrate_citation_metadata(passages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    citations = []
    for p in passages:
        src = p.get("source_url")
        final_src = src
        if isinstance(src, str) and src.startswith("s3://"):
            try:
                no_prefix = src[5:]
                parts = no_prefix.split("/", 1)
                bucket = parts[0]
                path = parts[1] if len(parts) > 1 else ""
                final_src = f"https://{bucket}.s3.amazonaws.com/{path}"
            except Exception:
                final_src = src
        citations.append(
            {
                "citation": p.get("number"),
                "chunk_id": p.get("chunk_id"),
                "source_url": final_src,
                "meta": p.get("meta") or {},
            }
        )
    return citations

def _validate_request_shape(ev: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    req_id = ev.get("request_id") or f"r-{int(time.time() * 1000)}"
    language = (ev.get("language") or "").strip().lower()
    channel = (ev.get("channel") or "").strip().lower()
    query_text = (ev.get("query") or ev.get("question") or "").strip()
    session_id = ev.get("session_id")
    try:
        top_k = int(ev.get("top_k")) if ev.get("top_k") is not None else None
    except Exception:
        top_k = None
    try:
        raw_k = int(ev.get("raw_k")) if ev.get("raw_k") is not None else None
    except Exception:
        raw_k = None
    if not language or language not in ALLOWED_LANGUAGES:
        return None, {"error": "invalid_language", "request_id": req_id}
    if not channel or channel not in ALLOWED_CHANNELS:
        return None, {"error": "invalid_channel", "request_id": req_id}
    if not query_text:
        return None, {"error": "empty_query", "request_id": req_id}
    asr_confidence = None
    if channel == "voice":
        try:
            asr_confidence = float(ev.get("asr_confidence")) if ev.get("asr_confidence") is not None else None
        except Exception:
            asr_confidence = None
        if asr_confidence is None:
            return None, {"error": "missing_asr_confidence", "request_id": req_id}
    req = {
        "request_id": req_id,
        "session_id": session_id,
        "language": language,
        "channel": channel,
        "query": query_text,
        "top_k": top_k,
        "raw_k": raw_k,
        "asr_confidence": asr_confidence,
        "region": ev.get("region"),
        "filters": ev.get("filters", {}),
    }
    return req, None

def _intent_blocked(query: str) -> Optional[str]:
    for k, pat in INTENT_BLOCKLIST_PATTERNS.items():
        if pat.search(query):
            return GUIDANCE_KEYS.get(k)
    return None

def _enforce_asr(asr_confidence: Optional[float]) -> Optional[str]:
    if asr_confidence is None:
        return None
    if float(asr_confidence) < ASR_CONF_THRESHOLD:
        return "asr_low_confidence"
    return None

def handle(event: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.time()
    validated, error = _validate_request_shape(event)
    if error:
        jlog({"level":"ERROR","event":"invalid_request_shape","detail":error,"request_id":error.get("request_id")})
        _write_audit({"session_id": event.get("session_id"), "request_id": error.get("request_id"), "language": event.get("language"), "channel": event.get("channel"), "used_chunk_ids": [], "resolution": "refusal", "guidance_key": GUIDANCE_KEYS.get("invalid_request"), "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": error.get("request_id"), "resolution": "refusal", "guidance_key": GUIDANCE_KEYS.get("invalid_request")}
    request_id = validated["request_id"]
    session_id = validated.get("session_id")
    language = validated["language"]
    channel = validated["channel"]
    query_text = validated["query"]
    top_k = validated["top_k"] or FINAL_K
    raw_k = validated["raw_k"] or RAW_K
    filters = validated.get("filters", {})
    jlog(f"request start | request_id={request_id} | session_id={session_id} | language={language} | channel={channel}")
    if channel == "voice":
        asr_conf = validated.get("asr_confidence")
        asr_issue = _enforce_asr(asr_conf)
        if asr_issue:
            jlog(f"refuse_asr | request_id={request_id} | asr_confidence={asr_conf}")
            _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": [], "resolution": "refusal", "guidance_key": GUIDANCE_KEYS.get("asr_low_confidence"), "timing_ms": int((time.time() - start_time) * 1000)})
            return {"request_id": request_id, "resolution": "refusal", "guidance_key": GUIDANCE_KEYS.get("asr_low_confidence")}
    guid = _intent_blocked(query_text)
    if guid:
        jlog(f"intent_blocked | request_id={request_id} | guidance={guid}")
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": [], "resolution": "refusal", "guidance_key": guid, "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "refusal", "guidance_key": guid}
    retr_ev = {"request_id": request_id, "query": query_text, "top_k": top_k, "raw_k": raw_k, "filters": filters}
    t_retr_start = time.time()
    try:
        retr_res = retrieve(retr_ev)
    except Exception as e:
        jlog({"level":"ERROR","event":"retriever_exception","request_id":request_id,"detail":str(e)})
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": [], "resolution": "invalid_output", "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "invalid_output", "error": "retrieval_failed"}
    t_retr_ms = int((time.time() - t_retr_start) * 1000)
    jlog(f"retriever returned | request_id={request_id} | retrieval_ms={t_retr_ms} | top_similarity={retr_res.get('top_similarity')}")
    if t_retr_ms / 1000.0 > EMBED_SEARCH_BUDGET_SEC:
        jlog(f"retrieval slow | request_id={request_id} | ms={t_retr_ms}")
    passages = retr_res.get("passages") or []
    chunk_ids = retr_res.get("chunk_ids") or []
    top_similarity = float(retr_res.get("top_similarity") or 0.0)
    if not passages:
        jlog(f"no_candidates | request_id={request_id}")
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": [], "resolution": "not_enough_info", "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "not_enough_info"}
    if top_similarity < MIN_SIMILARITY:
        jlog(f"too_low_similarity | request_id={request_id} | top_similarity={top_similarity}")
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": chunk_ids, "resolution": "not_enough_info", "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "not_enough_info", "top_similarity": top_similarity}
    prompt_text = "LANGUAGE: " + language + "\n\nPASSAGES:\n"
    for p in passages:
        prompt_text += f"{int(p.get('number'))}. {(p.get('text') or '').strip()}\n"
    prompt_text += "\nQUESTION:\n" + query_text + "\n\nAnswer from these passages if relevant; otherwise reply NOT_ENOUGH_INFORMATION. Each factual sentence must end with a [n] citation."
    gen_start = time.time()
    gen_raw = None
    attempt = 0
    last_exc = None
    while attempt <= BEDROCK_MAX_RETRIES:
        try:
            gen_raw = call_bedrock(prompt_text)
            break
        except Exception as e:
            last_exc = e
            jlog({"level":"ERROR","event":"bedrock_call_failed","request_id":request_id,"attempt":attempt,"detail":str(e)})
            attempt += 1
            if attempt <= BEDROCK_MAX_RETRIES:
                time.sleep(BEDROCK_RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)))
                continue
            break
    t_gen_ms = int((time.time() - gen_start) * 1000)
    jlog(f"generator returned | request_id={request_id} | gen_ms={t_gen_ms}")
    if gen_raw is None:
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": chunk_ids, "resolution": "refusal", "reason": "generator_failed", "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "refusal", "reason": "generator_failed"}
    validation_decision, validated_lines = _validate_generator_output_and_extract_lines_raw(gen_raw, passages)
    citations = _hydrate_citation_metadata(passages)
    if validation_decision == "NOT_ENOUGH_INFORMATION":
        jlog(f"generator_refuse_no_info | request_id={request_id}")
        _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": chunk_ids, "resolution": "not_enough_info", "timing_ms": int((time.time() - start_time) * 1000)})
        return {"request_id": request_id, "resolution": "not_enough_info"}
    if validation_decision != "ACCEPT":
        if not VALIDATION_STRICT:
            jlog(f"relaxed_validation_used | request_id={request_id} | reason={validation_decision}")
            res = {"request_id": request_id, "resolution": "answer", "answer_lines": [{"text": gen_raw.strip()}], "sources": [c.get("source_url") for c in citations], "confidence": "low", "citation_missing": True}
            if isinstance(gen_raw, dict) and gen_raw.get("tts_url"):
                res["tts_url"] = gen_raw.get("tts_url")
            _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "query": query_text, "used_chunk_ids": chunk_ids, "top_similarity": top_similarity, "resolution": res["resolution"], "generator_decision": validation_decision, "timing_ms": int((time.time() - start_time) * 1000)})
            jlog(f"request complete | request_id={request_id} | resolution={res['resolution']} | citation_missing=1")
            return res
        else:
            jlog(f"generator_invalid_output | request_id={request_id} | validation_decision={validation_decision}")
            _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "used_chunk_ids": chunk_ids, "resolution": "invalid_output", "timing_ms": int((time.time() - start_time) * 1000)})
            return {"request_id": request_id, "resolution": "invalid_output"}
    res = {"request_id": request_id, "resolution": "answer", "answer_lines": validated_lines, "citations": citations, "sources": [c.get("source_url") for c in citations], "confidence": "high"}
    _write_audit({"session_id": session_id, "request_id": request_id, "language": language, "channel": channel, "query": query_text, "used_chunk_ids": chunk_ids, "top_similarity": top_similarity, "resolution": res["resolution"], "generator_attempts": attempt + 1, "timing_ms": int((time.time() - start_time) * 1000)})
    jlog(f"request complete | request_id={request_id} | resolution={res['resolution']} | returned_lines={len(validated_lines)}")
    return res

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        return handle(event)
    except Exception as e:
        jlog({"level":"ERROR","event":"handler_unexpected","detail":str(e)})
        req_id = event.get("request_id") if isinstance(event, dict) else None or f"r-{int(time.time() * 1000)}"
        _write_audit({"session_id": event.get("session_id") if isinstance(event, dict) else None, "request_id": req_id, "language": event.get("language") if isinstance(event, dict) else None, "channel": event.get("channel") if isinstance(event, dict) else None, "used_chunk_ids": [], "resolution": "invalid_output", "error": str(e)})
        return {"request_id": req_id, "resolution": "refusal", "reason": "handler_exception"}

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--channel", required=True, choices=list(ALLOWED_CHANNELS))
    p.add_argument("--language", required=True, choices=list(ALLOWED_LANGUAGES))
    p.add_argument("--query", required=True)
    p.add_argument("--top_k", type=int, default=3)
    p.add_argument("--raw_k", type=int, default=50)
    p.add_argument("--session", default=None)
    p.add_argument("--request_id", default=None)
    args = p.parse_args()
    ev = {"session_id": args.session, "request_id": args.request_id, "language": args.language, "channel": args.channel, "query": args.query, "top_k": args.top_k, "raw_k": args.raw_k}
    out = handle(ev)
    print(json.dumps(out, indent=2, ensure_ascii=False))
