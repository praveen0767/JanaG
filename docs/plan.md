



```sh

indexing_pipeline/
├── Dockerfile                    # OCI image definition for indexing runtime (no infra tools, deterministic build)
├── ELT/                           # Extract–Load–Transform logic for raw civic data
│   ├── __init__.py                # Marks ELT as a Python package
│   ├── extract_load/              # Data acquisition layer (network + IO bound)
│   │   ├── __init__.py            # Package marker
│   │   └── web_scraper.py         # Deterministic web scraping + raw data ingestion
│   └── parse_chunk_store/         # Content normalization and chunking layer
│       ├── _html.py               # HTML parsing and cleanup utilities
│       ├── pdf.py                 # PDF parsing and text extraction logic
│       └── router.py              # Routes document types to correct parser
├── __init__.py                    # Marks indexing_pipeline as an importable module
├── build_and_push_image.sh        # Authoritative build contract (multi-arch buildx + push)
├── embed_and_index.py             # Embedding generation and vector index write path
├── main.py                        # Container entrypoint orchestrating indexing pipeline
└── requirements.txt               # Runtime-only Python dependencies (no build tools)
|
infra/
├── pulumi_aws/                          # Declarative infrastructure (AWS) – single source of truth
│   ├── Pulumi.prod.yaml                 # Production stack config (values only, no logic)
│   ├── Pulumi.yaml                      # Pulumi project definition (name, runtime, backend)
│   ├── __main__.py                      # Stack entrypoint; wires all infra components together
│   ├── indexing_pipeline.py             # ECS + scheduling + IAM for indexing workload
│   ├── inference_pipeline.py            # Inference runtime (service, scaling, networking)
│   ├── requirements.txt                 # Infra-only Python deps (Pulumi SDK, AWS providers)
│   └── run.sh                           # Thin wrapper to select stack and run Pulumi deterministically
└── scripts/                             # Imperative helper scripts (non-authoritative)
    ├── build_lambdas.sh                 # Builds and packages Lambda artifacts (zip-based)
    ├── local_bootstrap.sh               # One-time local env setup (dev convenience only)
    ├── s3.py                            # Ad-hoc S3 utilities (inspection / debugging)
    ├── sync_s3_with_local_fs.py          # One-way sync between S3 buckets and local FS
    ├── temp_db.sh                       # Temporary DB setup for experiments/tests
    ├── test_aurora_pgvector.sh           # Connectivity + extension validation for Aurora pgvector

```



# public-civic-info-system — refactored operational plan (channels + 4 Lambdas)

## One-line pitch

Deterministic, auditable civic knowledge delivered across web, SMS, and voice: authoritative content is ingested by an idempotent ECS/Fargate cronjob and served by a serverless inference stack composed of a single core Lambda (policy + retrieval + generation) and three thin channel adapters (HTTP, SMS, Voice) to maximize reach while minimizing operational surface and cost. The system is **not** only a Q/A chatbot — it’s a multi-channel civic information platform (web, SMS, voice input/output). The inference surface is refactored into **one core Lambda** that implements the business logic (retrieval + generation + policy) and **three thin channel Lambdas** (http, sms, voice) that normalize input and normalize/route responses. This yields **4 Lambdas total** that scale/operate independently and keep business rules centralized and auditable.

---

## Refactored high-level architecture (operational)

```
Indexing (ECS Fargate scheduled) ---> S3 manifests + Aurora (pgvector)
                                          ▲
                                          |
               Channel Lambdas (http,sms,voice) -> Core Lambda (query.handle)
                                                     ├─ embed + vector search (pgvector)
                                                     ├─ generator (Bedrock)
                                                     ├─ metadata hydration (S3)
                                                     └─ audit sink (S3) + metrics
```

Key principle: **channel adapters are thin** (no business logic). All policy, validation, and citation guarantees live in the core Lambda. This reduces duplication and centralizes safety.

---

## Components & responsibilities

### Indexing cronjob (ECS Fargate + EventBridge)

* **Schedule**: EventBridge cron (default nightly; critical feeds every 6h).
* **Compute**: Fargate task (2 vCPU, 4–8 GB RAM), parallel workers N=4.
* **Work**: crawl → OCR/parse → deterministic chunking → embed → index → write manifests to S3.
* **Idempotency**: `chunk_id = sha256(raw_sha256 + ":" + start_token + ":" + end_token)` and `INSERT ... ON CONFLICT DO NOTHING`.
* **Outputs**: vector DB (Aurora+pgvector), per-chunk metadata in S3, ingestion manifests, logs/metrics.

### Inference — 4 Lambdas (serverless)

* **Core Lambda** (single package `core/` with `query.py`, `retriever.py`, `generator.py`):

  * Responsibilities: request validation, policy/intent blocking, ASR gating, embed + vector search, re-ranking, deterministic generation (Bedrock), strict validator, metadata hydration, audit write.
  * Runtimes: Python 3.11, pinned libs (boto3 etc.).
  * Time budget: Lambda timeout = 10s. Internal micro-budgets enforced: embed+search ≤ 2.5s, generation ≤ 4.0s, metadata fetch ≤ 0.3s, buffer ≈ 3s.
  * Single place to tune thresholds: `MIN_SIMILARITY`, `ASR_CONF_THRESHOLD`, `RAW_K`, `FINAL_K`.
* **HTTP Adapter Lambda** (`channels/http/handler.py`):

  * Translates API Gateway JSON → normalized core request.
  * Handles auth (short-lived token), CORS, and returns core response as JSON to client.
* **SMS Adapter Lambda** (`channels/sms/handler.py`):

  * Normalizes incoming SMS (SNS/Twilio) to `{session_id, request_id, language, query}`.
  * Enforces message length (e.g., 1,600 chars or split-handling); maps core responses to SMS-friendly text (strip citations inline, attach `View source` short code via URL only when allowed).
  * Sends replies via configured SMS provider (SNS / Pinpoint / Twilio) with rate limits and cost-awareness.
* **Voice Adapter Lambda** (`channels/voice/handler.py`):

  * Two modes:

    * **Input pipeline**: accepts audio, stores to S3, starts Transcribe job (or uses streaming if supported), returns transcript + asr_confidence to core.
    * **Output pipeline**: plays TTS `tts_url` returned by core; optionally generate Polly TTS if requested.
  * ASR gating implemented in Core (adapter supplies `asr_confidence`).

> Operational note: adapters do not make policy decisions; they only normalize, authenticate, and map transport-specific errors to core request semantics.

---

## Runtime control flow (canonical, same for all channels)

1. Channel Lambda receives input (HTTP/SMS/Voice).
2. Channel normalizes to canonical request shape and forwards to **Core Lambda** (synchronous invoke).
3. Core Lambda:

   * Validate `language` (REQUIRED), `query`, and channel.
   * If `channel == voice`, check `asr_confidence >= ASR_CONF_THRESHOLD`.
   * Intent classification → refuse medical/legal.
   * Embed query (Bedrock Titan embed or mock) → vector search (`RAW_K=50`) → re-rank by `similarity × trust × freshness`.
   * If no passages or `top_similarity < MIN_SIMILARITY` → refusal.
   * Call generator (Bedrock) with **numbered passages only**; validate generator output (every sentence ends with `[n]`, cit. indexes valid, no URLs).
   * If decision != `ACCEPT` → refusal.
   * Hydrate citation metadata from S3 for UI/SMS/voice use.
   * Write audit record to S3 and emit metrics.
4. Core returns structured response to channel Lambda.
5. Channel maps response to transport:

   * HTTP: full JSON.
   * SMS: plain text optimized for brevity; possibly include short link to source page.
   * Voice: return `tts_url` or generate Polly audio; play via telephony provider.

---

## Data shapes & channel specifics

**Canonical request** (sent to Core):

```json
{
  "session_id":"uuid",
  "request_id":"uuid",
  "language":"en|hi|ta",    // REQUIRED
  "channel":"web|sms|voice",
  "query":"How to apply for X?",
  "region":"tn",            // optional
  "top_k":5,                // optional
  "asr_confidence":0.87     // only when channel=voice
}
```

**Core success response** (HTTP):

```json
{
  "request_id":"r-...",
  "resolution":"answer",
  "answer_lines":[{"text":"Apply online at portal. [1]"}],
  "citations":[{"citation":1,"chunk_id":"c_123","title":"PM-KISAN","source_url":"https://...","last_updated":"2024-06-01"}],
  "confidence":"high",
  "tts_url":"https://signed-tts" // optional
}
```

**SMS mapping rules**

* Max SMS length per message: 1600 chars (concatenate fragments on provider).
* Behavior: send a single short sentence answer (first `answer_line`) and a localized guidance string; append link only if `source_url` is allowed and short link is available.
* Avoid returning `[n]` tokens; instead append `See source: <shortlink>`.

**Voice mapping rules**

* Keep audio responses ≤ 20s; prioritize 1–2 sentences.
* Use `tts_url` (signed) returned by core; fallback to Polly generation if core doesn't provide TTS.
* Provide "repeat" and "speak slowly" commands via DTMF/voice menu.

---

## SLOs, knobs, and operational thresholds

* **SLOs**

  * Median latency (channel adapter + core): ≤ 2s
  * P95 latency: ≤ 6s
  * Deterministic refusal on invalid/insufficient evidence: 100%

* **Knobs (defaults)**

  * `RAW_K = 50`, `FINAL_K = 5`
  * `MIN_SIMILARITY = 0.60`
  * `ASR_CONF_THRESHOLD = 0.75`
  * Embedding batch size (indexing): `B = 32`
  * Query Router/Core Lambda timeout = 10s; internal budgets: embed/search 2.5s, generate 4.0s, fetch 0.3s.

* **Retries**

  * Adapters: retry network/transient errors twice with jitter.
  * Core: 1 retry for Bedrock calls with exponential backoff; otherwise fail-fast and audit.

---

## Safety and policy (centralized in Core)

* Intent blocklist (medical/legal/prohibited) — Core refuses with `guidance_key`.
* ASR gating — adapters pass `asr_confidence`; Core enforces `ASR_CONF_THRESHOLD`.
* PII handling — ingestion flags redactable sources; Core will not surface PII from chunks (router-level enforcement).
* Rate limiting & WAF — use CloudFront + WAF for web UI; SMS/voice providers rate-limit at provider level.
* Audit trail — every request written to S3 audit sink with `{session_id, request_id, language, used_chunk_ids, resolution, timing_ms}`.

---

## Monitoring & alerting

* Core metrics: `request.latency_ms`, `retrieval.top_similarity`, `generate.latency`, `decision_counts` (`ACCEPT`,`NOT_ENOUGH_INFORMATION`,`INVALID_OUTPUT`).
* Adapter metrics: `adapter.latency_ms`, `adapter.errors` (per transport), `sms_delivery_failures`, `voice_asr_failures`.
* Alerts:

  * `no_candidates_rate` spike → ingestion priority
  * `INVALID_OUTPUT` > threshold → prompt/validator review
  * p95 latency breach → increase provisioned concurrency or memory
  * SMS failure rate spike → carrier/provider failover

---

## Rollout & canary strategy (channel-aware)

1. **Indexing first** — run indexing cron, populate vector DB and S3 manifests.
2. **Core in staging (refusal-only)** — core returns passages but refuses to generate; validate recall and audit trails.
3. **Enable HTTP adapter → canary generation** — 10% traffic for 24–48h; monitor `INVALID_OUTPUT`.
4. **Enable SMS adapter (pilot region)** — test message flows, delivery, and cost.
5. **Enable voice adapter** — test ASR gates, audio limits, TTS behavior.
6. **Full roll** — enable all channels when metrics stable.

---

## Operational checklist (ready-to-run)

* Build and publish indexing image to ECR; set `IMAGE_TAG` (not `latest`).
* Configure EventBridge schedule and ECS task env (`ENV_VARS_JSON`) with S3 bucket ARNs.
* Deploy Core Lambda and channels using IaC; provide `RETRIEVAL`/`GEN` envs inside Core as internal module references.
* Populate ingestion with sample authoritative docs (voter id, ration card).
* Run smoke tests per-channel and verify audit records in S3.

---

## Why this refactor matters (concise rationale)

* **Single source of truth for policy and citation** — core centralizes safety and validation so adding channels does not increase risk surface.
* **Thin, replaceable adapters** — makes it straightforward to add new transports (WhatsApp, IVR provider) without touching business logic.
* **Cost & scaling efficiency** — serverless adapters + single core Lambda avoid idle costs while enabling independent scaling per channel.
* **Accessibility impact** — SMS and voice adapters materially increase reach to low-literacy, feature-phone, and offline-first populations.

---






