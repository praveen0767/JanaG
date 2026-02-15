#!/usr/bin/env python3
# embed_and_index.py
#
# Final, idempotent indexing worker for the public-civic-info-system.
#
# Assumptions & Invariants (fail-fast validated at startup)
# - This process **only** runs with the indexing-role DB credentials (DDL + INSERT allowed).
# - PostgreSQL >= 15 with pgvector >= 0.6.0 (indexing pipeline creates extension/indexes idempotently).
# - The canonical table (civic_chunks) shape is the frozen contract the inference pipeline reads:
#     - columns: document_id, chunk_id, chunk_index, content, embedding vector(<EMBED_DIM>),
#       source_url, page_number, parser_version, meta JSONB
#     - unique constraint on chunk_id
# - Embeddings dimension controlled by EMBED_DIM env var (default 1024) and enforced at runtime.
# - Embedding model: external Bedrock model (EMBED_MODEL_ID) that returns a list of floats named `embedding`.
# - S3 storage for chunk files (S3_BUCKET + S3_PREFIX).
#
# Operational notes:
# - Idempotency strategy:
#   1) DDL uses CREATE IF NOT EXISTS (safe on re-runs).
#   2) We avoid embedding already-stored chunks by pre-checking chunk_id existence in batches.
#   3) Insert uses ON CONFLICT (chunk_id) DO NOTHING.
# - All additional, evolving metadata is stored in `meta JSONB` (opaque to retrievers).
#
# Failure philosophy: fail-fast on environment/permission problems; otherwise continue file-by-file and log.

from __future__ import annotations
import os
import sys
import json
import time
import logging
from typing import Dict, Any, List, Iterator, Optional
from datetime import datetime
import boto3
import botocore
import psycopg
from psycopg.rows import dict_row
from botocore.exceptions import ClientError

# -------------------------
# Environment / defaults
# -------------------------
# Required
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("S3_CHUNKED_PREFIX", os.getenv("S3_PREFIX", "data/chunked/")).lstrip("/").rstrip("/") + "/"
AWS_REGION = os.getenv("AWS_REGION", "").strip()
EMBED_MODEL_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0").strip()
VECTOR_DB = os.getenv("VECTOR_DB", "pgvector").strip().lower()

# Postgres connect params
PG_HOST = os.getenv("PG_HOST", "127.0.0.1").strip()
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres").strip()
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_DB = os.getenv("PG_DB", "postgres").strip()
PG_TABLE_NAME = os.getenv("PG_INDEX_NAME", "civic_chunks").strip()

# Embedding details
EMBED_DIM = int(os.getenv("EMBED_DIM", "1024"))
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
AWS_PROFILE = os.getenv("AWS_PROFILE")  # optional
SLEEP_BETWEEN_FILES = float(os.getenv("SLEEP_BETWEEN_FILES", "0.0"))

# Logging config (deterministic, structured JSON-ish lines)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("embed_and_index")
def jlog(obj: Dict[str, Any]):
    # deterministic timestamp + sorted keys for consistent logs
    obj_out = {"ts": datetime.utcnow().isoformat(timespec="seconds") + "Z"}
    obj_out.update(obj)
    log.info(json.dumps(obj_out, sort_keys=True, default=str))

# -------------------------
# Fail-fast validations
# -------------------------
def fail(msg: str, code: int = 1):
    jlog({"level": "CRITICAL", "event": "startup_check_failed", "detail": msg})
    sys.exit(code)

if not S3_BUCKET:
    fail("S3_BUCKET environment variable must be set", 10)
if not AWS_REGION:
    fail("AWS_REGION environment variable must be set", 11)
if VECTOR_DB != "pgvector":
    fail("Only pgvector vector DB is supported by this script", 12)
if EMBED_DIM <= 0:
    fail("EMBED_DIM must be a positive integer", 13)
if BATCH_SIZE <= 0:
    fail("EMBED_BATCH_SIZE must be a positive integer", 14)

jlog({"event":"startup_checks_ok","s3_bucket":S3_BUCKET,"s3_prefix":S3_PREFIX,"embed_model":EMBED_MODEL_ID,"embed_dim":EMBED_DIM,"pg_table":PG_TABLE_NAME})

# -------------------------
# AWS clients
# -------------------------
boto_session_kwargs = {"region_name": AWS_REGION}
if AWS_PROFILE:
    boto_session = boto3.Session(profile_name=AWS_PROFILE, **({"region_name": AWS_REGION} if AWS_REGION else {}))
else:
    boto_session = boto3.Session(**({"region_name": AWS_REGION} if AWS_REGION else {}))

s3 = boto_session.client("s3")
bedrock = boto_session.client("bedrock-runtime")

# -------------------------
# DB helpers
# -------------------------
def init_pg_connection():
    conninfo = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}"
    try:
        conn = psycopg.connect(conninfo, autocommit=True, row_factory=dict_row)
    except Exception as e:
        jlog({"level":"CRITICAL","event":"pg_connect_failed","detail": str(e)})
        raise SystemExit(20)
    jlog({"event":"pg_connect_ok","host":PG_HOST,"port":PG_PORT,"db":PG_DB})
    return conn

def ensure_schema(conn):
    """
    Create extension, table, and indexes idempotently according to contract:
    - civic_chunks(id, document_id, chunk_id, chunk_index, content, embedding vector(<EMBED_DIM>),
      source_url, page_number, parser_version, meta JSONB)
    - unique constraint on chunk_id
    - HNSW index on embedding (vector_cosine_ops)
    - indexes on document_id and source_url
    """
    with conn.cursor() as cur:
        # extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        # table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {PG_TABLE_NAME} (
          id BIGSERIAL PRIMARY KEY,
          document_id TEXT NOT NULL,
          chunk_id TEXT NOT NULL,
          chunk_index INT NOT NULL,
          content TEXT NOT NULL,
          embedding vector({EMBED_DIM}) NOT NULL,
          source_url TEXT,
          page_number INT,
          parser_version TEXT,
          meta JSONB NOT NULL,
          CONSTRAINT {PG_TABLE_NAME}_unique UNIQUE (chunk_id)
        );
        """
        cur.execute(create_table_sql)
        # indexes
        cur.execute(f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE indexname = '{PG_TABLE_NAME}_embedding_hnsw'
          ) THEN
            CREATE INDEX {PG_TABLE_NAME}_embedding_hnsw
            ON {PG_TABLE_NAME}
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 128);
          END IF;
        END$$;
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS {PG_TABLE_NAME}_document_id ON {PG_TABLE_NAME} (document_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {PG_TABLE_NAME}_source_url ON {PG_TABLE_NAME} (source_url);")
    jlog({"event":"schema_ensured","table":PG_TABLE_NAME,"embed_dim":EMBED_DIM})

# -------------------------
# S3 helpers (stream JSONL)
# -------------------------
def list_chunk_objects(bucket: str, prefix: str) -> Iterator[Dict[str, Any]]:
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            if not key:
                continue
            if key.endswith(".chunks.jsonl"):
                yield {"Key": key, "Size": obj.get("Size", 0)}

def stream_jsonl_from_s3(bucket: str, key: str) -> Iterator[Dict[str, Any]]:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        jlog({"level":"ERROR","event":"s3_get_object_failed","key":key,"error":str(e)})
        raise
    body = resp["Body"]
    for raw in body.iter_lines():
        if not raw:
            continue
        try:
            line = raw.decode("utf-8").strip()
        except Exception:
            try:
                line = raw.decode("latin-1").strip()
            except Exception:
                continue
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            jlog({"level":"ERROR","event":"json_parse_failed","key":key,"sample": line[:200]})
            continue

# -------------------------
# Normalization (minimal required fields)
# -------------------------
CORE_REQUIRED = {"document_id", "chunk_id", "chunk_index", "text", "parser_version"}

def normalize_chunk(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Produce an output dict with the exact columns we will store:
    - document_id (str)
    - chunk_id (str)
    - chunk_index (int)
    - text -> content (str)
    - source_url (optional)
    - page_number (optional int)
    - parser_version (str)
    - meta (JSON dict with any other keys)
    """
    if not isinstance(raw, dict):
        return None
    missing = [k for k in CORE_REQUIRED if k not in raw]
    if missing:
        jlog({"level":"DEBUG","event":"chunk_schema_missing","missing": missing, "chunk_sample": raw.get("chunk_id")})
        return None
    try:
        document_id = str(raw["document_id"])
        chunk_id = str(raw["chunk_id"])
        chunk_index = int(raw["chunk_index"])
        content = (raw.get("text") or "").replace("\r\n", "\n").strip()
        parser_version = str(raw["parser_version"])
    except Exception as e:
        jlog({"level":"DEBUG","event":"chunk_schema_cast_failed","error": str(e), "chunk_sample": raw.get("chunk_id")})
        return None

    # Build meta from any non-core fields (store everything else opaquely).
    meta = {}
    for k, v in raw.items():
        if k in CORE_REQUIRED:
            continue
        # keep common innocuous fields inside meta for future debugging/citation
        meta[k] = v

    normalized = {
        "document_id": document_id,
        "chunk_id": chunk_id,
        "chunk_index": chunk_index,
        "content": content,
        "source_url": raw.get("source_url"),
        "page_number": None if raw.get("page_number") is None else int(raw.get("page_number")),
        "parser_version": parser_version,
        "meta": meta
    }
    return normalized

# -------------------------
# Bedrock embedding call
# -------------------------
def get_embedding_from_bedrock(text: str) -> List[float]:
    # Build request per Bedrock runtime expectation (this may be provider-specific)
    request_body = {"inputText": text, "dimensions": EMBED_DIM, "normalize": True}
    try:
        response = bedrock.invoke_model(modelId=EMBED_MODEL_ID, body=json.dumps(request_body), contentType="application/json")
    except Exception as e:
        jlog({"level":"ERROR","event":"bedrock_invoke_error","detail": str(e)})
        raise
    body_stream = response.get("body")
    if hasattr(body_stream, "read"):
        raw = body_stream.read()
    else:
        raw = body_stream
    try:
        parsed = json.loads(raw)
    except Exception as e:
        jlog({"level":"ERROR","event":"bedrock_response_decode_failed","detail": str(e), "raw_sample": (raw[:200] if isinstance(raw, (bytes,str)) else str(raw))})
        raise
    emb = parsed.get("embedding")
    if not isinstance(emb, list):
        jlog({"level":"ERROR","event":"bedrock_no_embedding","response": parsed})
        raise RuntimeError("bedrock returned no embedding list")
    if len(emb) != EMBED_DIM:
        jlog({"level":"ERROR","event":"bedrock_embedding_dim_mismatch","expected": EMBED_DIM, "received": len(emb)})
        raise RuntimeError("embedding_dim_mismatch")
    return emb

# -------------------------
# DB batch insert
# -------------------------
def pg_existing_chunk_ids(conn, chunk_ids: List[str]) -> set:
    if not chunk_ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(f"SELECT chunk_id FROM {PG_TABLE_NAME} WHERE chunk_id = ANY(%s);", (chunk_ids,))
        rows = cur.fetchall()
    return set(r["chunk_id"] for r in rows)

def pg_insert_batch(conn, docs: List[Dict[str, Any]]):
    """
    Insert docs list in a single transaction (autocommit mode is enabled outside).
    Each doc must have: chunk_id, document_id, chunk_index, content, embedding (list of floats),
    source_url, page_number, parser_version, meta (dict).
    Uses ON CONFLICT DO NOTHING to maintain idempotency.
    """
    if not docs:
        return 0
    insert_sql = f"""
    INSERT INTO {PG_TABLE_NAME} (
      chunk_id, document_id, chunk_index, content, embedding, source_url, page_number, parser_version, meta
    ) VALUES (
      %s, %s, %s, %s, %s::vector, %s, %s, %s, %s
    )
    ON CONFLICT (chunk_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        for d in docs:
            emb = d["embedding"]
            emb_str = "[" + ",".join(map(str, emb)) + "]"
            meta_json = json.dumps(d.get("meta") or {})
            try:
                cur.execute(insert_sql, (
                    d["chunk_id"],
                    d["document_id"],
                    d["chunk_index"],
                    d["content"],
                    emb_str,
                    d.get("source_url"),
                    d.get("page_number"),
                    d.get("parser_version"),
                    meta_json
                ))
            except Exception as e:
                jlog({"level":"ERROR","event":"pg_insert_error","chunk_id": d.get("chunk_id"), "error": str(e)})
                raise
    return len(docs)

# -------------------------
# Main orchestration
# -------------------------
def process_s3_file(conn, bucket: str, key: str):
    jlog({"event":"processing_s3_object","key":key})
    stream = stream_jsonl_from_s3(bucket, key)
    pending_normalized: List[Dict[str, Any]] = []
    total_added_in_file = 0
    total_skipped_schema = 0

    for raw in stream:
        normalized = normalize_chunk(raw)
        if not normalized:
            total_skipped_schema += 1
            continue
        pending_normalized.append(normalized)
        if len(pending_normalized) >= BATCH_SIZE:
            added = process_batch(conn, pending_normalized)
            total_added_in_file += added
            pending_normalized = []
    if pending_normalized:
        added = process_batch(conn, pending_normalized)
        total_added_in_file += added

    jlog({"event":"file_done","key":key,"added_in_file": total_added_in_file, "skipped_schema": total_skipped_schema})
    return total_added_in_file, total_skipped_schema

def process_batch(conn, normalized_list: List[Dict[str, Any]]):
    chunk_ids = [n["chunk_id"] for n in normalized_list]
    existing = pg_existing_chunk_ids(conn, chunk_ids)
    to_process = [n for n in normalized_list if n["chunk_id"] not in existing]
    skipped_count = len(normalized_list) - len(to_process)
    if skipped_count:
        jlog({"event":"batch_skipped_existing","total": len(normalized_list), "skipped": skipped_count})
    if not to_process:
        return 0

    docs_for_insert = []
    for n in to_process:
        try:
            emb = get_embedding_from_bedrock(n["content"])
        except Exception as e:
            jlog({"level":"ERROR","event":"embedding_failed","chunk_id": n["chunk_id"], "error": str(e)})
            continue
        doc = {
            "chunk_id": n["chunk_id"],
            "document_id": n["document_id"],
            "chunk_index": n["chunk_index"],
            "content": n["content"],
            "embedding": emb,
            "source_url": n.get("source_url"),
            "page_number": n.get("page_number"),
            "parser_version": n.get("parser_version"),
            "meta": n.get("meta", {})
        }
        docs_for_insert.append(doc)

    if not docs_for_insert:
        jlog({"event":"batch_all_embeddings_failed_or_empty"})
        return 0

    inserted = 0
    try:
        inserted = pg_insert_batch(conn, docs_for_insert)
        jlog({"event":"batch_inserted","attempted":len(docs_for_insert),"inserted": inserted})
    except Exception as e:
        jlog({"level":"ERROR","event":"batch_insert_failed","error": str(e)})
        raise
    return inserted

def main() -> int:
    jlog({"event":"startup","s3_bucket": S3_BUCKET, "s3_prefix": S3_PREFIX})
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception as e:
        jlog({"level":"CRITICAL","event":"s3_head_bucket_failed","error": str(e)})
        return 2

    conn = init_pg_connection()
    ensure_schema(conn)

    total_indexed = 0
    total_skipped_schema = 0
    try:
        for obj in list_chunk_objects(S3_BUCKET, S3_PREFIX):
            key = obj["Key"]
            try:
                added, skipped = process_s3_file(conn, S3_BUCKET, key)
                total_indexed += added
                total_skipped_schema += skipped
            except Exception as e:
                jlog({"level":"ERROR","event":"file_processing_error","key":key,"error": str(e)})
            if SLEEP_BETWEEN_FILES > 0:
                time.sleep(SLEEP_BETWEEN_FILES)
    except Exception as e:
        jlog({"level":"CRITICAL","event":"listing_or_iteration_failed","error": str(e)})
        return 4

    jlog({"event":"complete","total_indexed": total_indexed, "total_skipped_schema": total_skipped_schema})
    if total_skipped_schema > 0:
        return 50
    return 0

if __name__ == "__main__":
    try:
        rc = main()
    except Exception as e:
        jlog({"level":"CRITICAL","event":"unhandled_exception","error": str(e)})
        rc = 1
    sys.exit(rc)
