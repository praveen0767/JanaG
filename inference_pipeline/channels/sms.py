from __future__ import annotations
import json
import logging
import sys
import hashlib
from typing import Any, Dict, Optional

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_log = logging.getLogger("channels.sms")

def jlog(msg: str) -> None:
    _log.info(msg)

try:
    from inference_pipeline.core.core import handle as core_handle
except Exception:
    try:
        pkg_root = __import__("os").path.abspath(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", ".."))
        if pkg_root not in sys.path:
            sys.path.insert(0, pkg_root)
        from inference_pipeline.core.core import handle as core_handle
    except Exception as e:
        jlog(f"critical: core import failed: {e}")
        raise

def _extract_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body") or {}
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            body = {}
    return body

def _shorten_source(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    try:
        parts = src.split("/")
        host = parts[2] if src.startswith("http") and len(parts) > 2 else src
        return host
    except Exception:
        return src

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    body = _extract_body(event)
    provider = body.get("provider")
    inbound_text = body.get("text") or body.get("query") or ""
    language = (body.get("language") or "en").strip().lower()
    session_id = body.get("session_id")
    request_id = body.get("request_id")
    to = body.get("to")
    if not inbound_text:
        jlog("sms handler: empty inbound text")
        return {"statusCode": 400, "body": json.dumps({"error": "empty_query"})}
    core_ev = {"channel": "sms", "language": language, "query": inbound_text, "session_id": session_id, "request_id": request_id}
    jlog(f"sms request | lang={language} | qlen={len(inbound_text)} | provider={provider}")
    try:
        core_res = core_handle(core_ev)
    except Exception as e:
        jlog(f"sms core.handle exception: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "core_failure"})}
    res_body = {}
    if core_res.get("resolution") == "answer":
        lines = core_res.get("answer_lines") or []
        first = ""
        if lines:
            el = lines[0]
            first = el.get("text") if isinstance(el, dict) else str(el)
        sources = core_res.get("citations") or []
        short_src = None
        if sources:
            s0 = sources[0].get("source_url")
            short_src = _shorten_source(s0)
        sms_text = first
        if short_src:
            sms_text = f"{sms_text}\nSource: {short_src}"
        res_body = {"statusCode": 200, "to": to, "message": sms_text}
        jlog(f"sms ready | to={to} | len_message={len(sms_text)}")
    else:
        reason = core_res.get("resolution") or "refusal"
        guidance = core_res.get("guidance_key") or reason
        msg = "Unable to provide information." if reason != "not_enough_info" else "No authoritative information found."
        res_body = {"statusCode": 200, "to": to, "message": msg, "reason": guidance}
        jlog(f"sms refusal | reason={guidance}")
    return {"statusCode": 200, "body": json.dumps(res_body, ensure_ascii=False)}
