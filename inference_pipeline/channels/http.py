# inference_pipeline/channels/http.py
from __future__ import annotations
import os
import sys
import json
import logging
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_log = logging.getLogger("channels.http")

def jlog(msg: str) -> None:
    _log.info(msg)

try:
    from inference_pipeline.core.core import handle as core_handle
except Exception:
    try:
        pkg_root = __import__("os").path.abspath(__import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
        if pkg_root not in sys.path:
            sys.path.insert(0, pkg_root)
        from inference_pipeline.core.core import handle as core_handle
    except Exception as e:
        jlog(f"critical: core import failed: {e}")
        raise

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ALLOWED_LANGS = {"en", "ta", "hi", "bn", "te", "ml", "kn", "mr", "gu"}

def _normalize_language(lang: Optional[str]) -> str:
    if not lang:
        return "en"
    val = str(lang).strip().lower()
    return val if val in ALLOWED_LANGS else "en"

@app.post("/v1/query")
async def http_query(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    language = _normalize_language(body.get("language") or req.query_params.get("language"))
    query_text = body.get("query") or body.get("question") or req.query_params.get("q") or ""
    if not query_text:
        return Response(content=json.dumps({"error":"empty_query"}), status_code=400, media_type="application/json")
    ev = {
        "channel": "web",
        "language": language,
        "query": str(query_text),
        "session_id": body.get("session_id"),
        "request_id": body.get("request_id"),
        "top_k": body.get("top_k"),
        "raw_k": body.get("raw_k"),
        "filters": body.get("filters", {}),
    }
    jlog(f"http request | lang={language} | qlen={len(ev['query'])}")
    try:
        core_res = core_handle(ev)
    except Exception as e:
        jlog(f"error: core.handle exception: {e}")
        return Response(content=json.dumps({"error":"core_failure"}), status_code=500, media_type="application/json")
    if isinstance(core_res, dict) and core_res.get("resolution") == "answer":
        out = {"request_id": core_res.get("request_id"), "resolution": "answer", "answer_lines": [l if isinstance(l, dict) else {"text": l} for l in core_res.get("answer_lines", [])], "citations": [{"source_url": c.get("source_url")} for c in core_res.get("citations", [])]}
    else:
        out = core_res
    return Response(content=json.dumps(out, ensure_ascii=False), status_code=200, media_type="application/json")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        body = event.get("body") or {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        language = _normalize_language(body.get("language") or (event.get("queryStringParameters") or {}).get("language"))
        query_text = body.get("query") or body.get("question") or (event.get("queryStringParameters") or {}).get("q") or ""
        if not query_text:
            return {"statusCode": 400, "body": json.dumps({"error":"empty_query"})}
        core_ev = {
            "channel": "web",
            "language": language,
            "query": str(query_text),
            "session_id": body.get("session_id"),
            "request_id": body.get("request_id"),
            "top_k": body.get("top_k"),
            "raw_k": body.get("raw_k"),
            "filters": body.get("filters", {}),
        }
        jlog(f"lambda http request | lang={language} | qlen={len(core_ev['query'])}")
        core_res = core_handle(core_ev)
        if isinstance(core_res, dict) and core_res.get("resolution") == "answer":
            out = {"request_id": core_res.get("request_id"), "resolution": "answer", "answer_lines": [l if isinstance(l, dict) else {"text": l} for l in core_res.get("answer_lines", [])], "citations": [{"source_url": c.get("source_url")} for c in core_res.get("citations", [])]}
        else:
            out = core_res
        return {"statusCode": 200, "body": json.dumps(out, ensure_ascii=False)}
    except Exception as e:
        jlog(f"lambda handler unexpected: {e}")
        return {"statusCode": 500, "body": json.dumps({'error':'handler_exception'})}
