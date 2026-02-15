# inference_pipeline/channels/voice.py
from __future__ import annotations
import os
import sys
import json
import time
import uuid
import logging
import urllib.request
from typing import Any, Dict, Optional
from fastapi import FastAPI, File, Form, UploadFile, Request, Response
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_log = logging.getLogger("channels.voice")

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

try:
    import boto3
except Exception:
    boto3 = None

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AWS_REGION = os.getenv("AWS_REGION")
AUDIO_TMP_BUCKET = os.getenv("AUDIO_TMP_BUCKET")

def _fetch_uri_text(uri: str, timeout: int = 30) -> str:
    try:
        with urllib.request.urlopen(uri, timeout=timeout) as r:
            body = r.read()
            try:
                j = json.loads(body)
                if isinstance(j, dict):
                    res = j.get("results", {}).get("transcripts", [])
                    if res and isinstance(res, list):
                        return res[0].get("transcript", "")
                return body.decode("utf-8", errors="ignore")
            except Exception:
                return body.decode("utf-8", errors="ignore")
    except Exception as e:
        jlog(f"failed to fetch transcript uri: {e}")
        return ""

def _transcribe_with_aws(s3_key: str, local_ext: str, language: str, timeout_sec: int = 90) -> Optional[Dict[str, Any]]:
    if boto3 is None or not AWS_REGION or not AUDIO_TMP_BUCKET:
        jlog("aws transcribe skipped: boto3 or env missing")
        return None
    s3 = boto3.client("s3", region_name=AWS_REGION)
    transcribe = boto3.client("transcribe", region_name=AWS_REGION)
    job_name = f"job-{uuid.uuid4().hex[:16]}"
    media_uri = f"s3://{AUDIO_TMP_BUCKET}/{s3_key}"
    try:
        transcribe.start_transcription_job(TranscriptionJobName=job_name, Media={"MediaFileUri": media_uri}, MediaFormat=local_ext.lstrip("."), LanguageCode=language)
    except Exception as e:
        jlog(f"start_transcription_job failed: {e}")
        return None
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            resp = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            st = resp.get("TranscriptionJob", {}).get("TranscriptionJobStatus")
            if st == "COMPLETED":
                uri = resp.get("TranscriptionJob", {}).get("Transcript", {}).get("TranscriptFileUri")
                text = _fetch_uri_text(uri) if uri else ""
                return {"transcript": text, "confidence": 0.9}
            if st == "FAILED":
                jlog("transcription job failed")
                return None
        except Exception as e:
            jlog(f"transcribe polling error: {e}")
            return None
        time.sleep(2)
    jlog("transcribe timeout")
    return None

@app.post("/v1/query")
async def http_voice_query(request: Request, audio: UploadFile = File(None), language: str = Form("en")):
    if audio is None:
        body = await request.json()
        path = body.get("path")
        if not path:
            return Response(content=json.dumps({"error":"no_audio"}), status_code=400, media_type="application/json")
        try:
            with open(path, "rb") as f:
                audio_bytes = f.read()
            filename = os.path.basename(path)
        except Exception as e:
            return Response(content=json.dumps({"error":"file_read_failed","detail":str(e)}), status_code=400, media_type="application/json")
    else:
        audio_bytes = await audio.read()
        filename = audio.filename or f"upload-{uuid.uuid4().hex}.wav"
    ext = os.path.splitext(filename)[1].lstrip(".").lower() or "wav"
    request_id = f"v-{int(time.time()*1000)}"
    if boto3 and AWS_REGION and AUDIO_TMP_BUCKET:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"tmp/{uuid.uuid4().hex}.{ext}"
        try:
            s3.put_object(Bucket=AUDIO_TMP_BUCKET, Key=key, Body=audio_bytes)
        except Exception as e:
            jlog(f"s3 upload failed: {e}")
            return Response(content=json.dumps({"error":"s3_upload_failed","detail":str(e)}), status_code=500, media_type="application/json")
        trans = _transcribe_with_aws(key, ext, language)
        if trans is None:
            return Response(content=json.dumps({"error":"transcribe_failed"}), status_code=500, media_type="application/json")
        transcript = trans.get("transcript", "").strip()
        asr_conf = float(trans.get("confidence", 0.9))
    else:
        try:
            import speech_recognition as sr
            r = sr.Recognizer()
            from io import BytesIO
            bio = BytesIO(audio_bytes)
            with sr.AudioFile(bio) as src:
                audio_data = r.record(src)
                transcript = r.recognize_google(audio_data, language=language)
                asr_conf = 0.9
        except Exception as e:
            jlog(f"local ASR failed: {e}")
            return Response(content=json.dumps({"error":"asr_unavailable","detail":str(e)}), status_code=500, media_type="application/json")
    core_ev = {"channel":"voice","language":language,"query":transcript,"session_id":None,"request_id":request_id,"asr_confidence":asr_conf}
    try:
        core_res = core_handle(core_ev)
    except Exception as e:
        jlog(f"core.handle error: {e}")
        return Response(content=json.dumps({"error":"core_failure","detail":str(e)}), status_code=500, media_type="application/json")
    if isinstance(core_res, dict) and core_res.get("resolution") == "answer":
        out = {"request_id": core_res.get("request_id"), "resolution": "answer", "answer_lines": [l if isinstance(l, dict) else {"text": l} for l in core_res.get("answer_lines", [])], "citations": [{"source_url": c.get("source_url")} for c in core_res.get("citations", [])], "transcript": transcript}
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
        s3_uri = body.get("s3_uri")
        language = body.get("language", "en")
        request_id = body.get("request_id") or f"v-{int(time.time()*1000)}"
        if not s3_uri:
            return {"statusCode":400, "body": json.dumps({"error":"missing_s3_uri"})}
        if boto3 is None:
            return {"statusCode":500, "body": json.dumps({"error":"boto3_missing"})}
        try:
            transcribe = boto3.client("transcribe", region_name=AWS_REGION)
            job_name = f"job-{uuid.uuid4().hex[:16]}"
            transcribe.start_transcription_job(TranscriptionJobName=job_name, Media={"MediaFileUri": s3_uri}, MediaFormat=s3_uri.split(".")[-1], LanguageCode=language)
        except Exception as e:
            return {"statusCode":500, "body": json.dumps({"error":"start_transcription_failed","detail":str(e)})}
        deadline = time.time() + 90
        transcript_text = ""
        while time.time() < deadline:
            resp = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            st = resp.get("TranscriptionJob", {}).get("TranscriptionJobStatus")
            if st == "COMPLETED":
                uri = resp.get("TranscriptionJob", {}).get("Transcript", {}).get("TranscriptFileUri")
                transcript_text = _fetch_uri_text(uri)
                break
            if st == "FAILED":
                return {"statusCode":500, "body": json.dumps({"error":"transcription_failed"})}
            time.sleep(2)
        core_ev = {"channel":"voice","language":language,"query":transcript_text,"session_id":None,"request_id":request_id,"asr_confidence":0.9}
        core_res = core_handle(core_ev)
        if isinstance(core_res, dict) and core_res.get("resolution") == "answer":
            out = {"request_id": core_res.get("request_id"), "resolution": "answer", "answer_lines": [l if isinstance(l, dict) else {"text": l} for l in core_res.get("answer_lines", [])], "citations": [{"source_url": c.get("source_url")} for c in core_res.get("citations", [])], "transcript": transcript_text}
        else:
            out = core_res
        return {"statusCode":200, "body": json.dumps(out, ensure_ascii=False)}
    except Exception as e:
        jlog(f"lambda handler unexpected: {e}")
        return {"statusCode":500, "body": json.dumps({'error':'handler_exception','detail':str(e)})}
