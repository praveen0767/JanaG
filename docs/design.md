# Design Document: Public Civic Information System

## 1. System Architecture

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INDEXING PIPELINE                         │
│                     (ECS Fargate + EventBridge)                  │
│                                                                   │
│  ┌──────────┐    ┌──────────┐    ┌──────────────┐              │
│  │ Extract  │───▶│  Parse   │───▶│ Embed & Index│              │
│  │  & Load  │    │ & Chunk  │    │              │              │
│  └──────────┘    └──────────┘    └──────────────┘              │
│       │               │                   │                      │
└───────┼───────────────┼───────────────────┼──────────────────────┘
        │               │                   │
        ▼               ▼                   ▼
    ┌───────┐      ┌───────┐         ┌──────────┐
    │  S3   │      │  S3   │         │ Aurora   │
    │  Raw  │      │Chunks │         │pgvector  │
    └───────┘      └───────┘         └──────────┘
                                           ▲
                                           │
┌──────────────────────────────────────────┼──────────────────────┐
│                   INFERENCE PIPELINE     │                       │
│                   (4 Lambda Functions)   │                       │
│                                          │                       │
│  ┌──────────┐   ┌──────────┐   ┌───────┴────┐                 │
│  │   HTTP   │   │   SMS    │   │   Voice    │                 │
│  │ Adapter  │   │ Adapter  │   │  Adapter   │                 │
│  └────┬─────┘   └────┬─────┘   └─────┬──────┘                 │
│       │              │               │                          │
│       └──────────────┼───────────────┘                          │
│                      ▼                                           │
│              ┌───────────────┐                                  │
│              │  Core Lambda  │                                  │
│              │               │                                  │
│              │ • Validation  │                                  │
│              │ • Retrieval   │                                  │
│              │ • Generation  │                                  │
│              │ • Validation  │                                  │
│              │ • Audit       │                                  │
│              └───────────────┘                                  │
│                      │                                           │
└──────────────────────┼───────────────────────────────────────────┘
                       ▼
                  ┌────────┐
                  │   S3   │
                  │ Audits │
                  └────────┘
```

### 1.2 Design Principles

1. **Separation of Concerns**: Indexing (batch) and inference (real-time) are independent
2. **Thin Adapters**: Channel-specific logic isolated from business rules
3. **Centralized Policy**: All safety and validation in core Lambda
4. **Deterministic Behavior**: Content-addressed storage, idempotent operations
5. **Auditability**: Complete request/response trail with source attribution

## 2. Component Design

### 2.1 Indexing Pipeline

#### 2.1.1 Extract & Load (`web_scraper.py`)

**Purpose**: Acquire raw content from authoritative sources

**Design**:
- Uses `trafilatura` for deterministic HTML extraction
- Stores raw content in S3 with hash-based paths: `by-hash/{sha256}.html`
- Generates manifest files with metadata:
  ```json
  {
    "url": "https://source.gov.in/page",
    "fetched_at": "2024-01-15T10:30:00Z",
    "content_hash": "a0b668ab...",
    "trust_level": "gov",
    "content_type": "text/html"
  }
  ```
- Implements rate limiting and respectful crawling
- Supports both HTML and PDF content types

**Key Classes/Functions**:
- `fetch_and_store()`: Download content and compute hash
- `generate_manifest()`: Create metadata record
- `is_duplicate()`: Check if content already indexed

#### 2.1.2 Parse & Chunk (`router.py`, `_html.py`, `pdf.py`)

**Purpose**: Normalize content and create semantic chunks

**Design**:
- **Router** (`router.py`): Dispatches to appropriate parser based on content type
- **HTML Parser** (`_html.py`):
  - Uses BeautifulSoup for DOM parsing
  - Extracts clean text, preserving structure
  - Removes navigation, ads, boilerplate
- **PDF Parser** (`pdf.py`):
  - Uses pdfminer.six for text extraction
  - Falls back to pytesseract OCR for scanned documents
  - Preserves page numbers for citation

**Chunking Strategy**:
- Fixed token windows (512 tokens) with overlap (128 tokens)
- Deterministic chunk boundaries for reproducibility
- Chunk ID: `sha256(document_hash + ":" + start_token + ":" + end_token)`
- Output format (JSONL):
  ```json
  {
    "chunk_id": "6d970d8c...",
    "document_id": "a0b668ab...",
    "chunk_index": 0,
    "content": "Text content...",
    "meta": {"trust_level": "gov", "source_url": "..."},
    "page_number": 1
  }
  ```

**Key Functions**:
- `route_document()`: Select parser based on content type
- `chunk_text()`: Split text into overlapping windows
- `compute_chunk_id()`: Generate deterministic identifier

#### 2.1.3 Embed & Index (`embed_and_index.py`)

**Purpose**: Generate embeddings and populate vector database

**Design**:
- Reads chunked JSONL files from S3
- Batches chunks (B=32) for efficient embedding
- Calls AWS Bedrock Titan Embed v2 (1024 dimensions)
- Inserts into Aurora PostgreSQL with pgvector:
  ```sql
  CREATE TABLE civic_chunks (
    chunk_id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1024) NOT NULL,
    meta JSONB,
    source_url TEXT,
    page_number INTEGER,
    indexed_at TIMESTAMP DEFAULT NOW()
  );
  
  CREATE INDEX ON civic_chunks 
  USING hnsw (embedding vector_cosine_ops);
  ```
- Idempotent: `INSERT ... ON CONFLICT (chunk_id) DO NOTHING`

**Key Functions**:
- `batch_embed()`: Call Bedrock with batch of texts
- `insert_chunks()`: Write to database with conflict handling
- `verify_index()`: Check HNSW index exists and is healthy

#### 2.1.4 Orchestration (`main.py`)

**Purpose**: Execute pipeline stages in sequence

**Design**:
- Sequential execution: extract → parse → embed
- Subprocess-based stage execution for isolation
- Structured logging with run_id for traceability
- Exit on first failure (fail-fast)
- Environment-based configuration

**Execution Flow**:
```python
1. Set RUN_ID (timestamp-based)
2. For each stage:
   a. Log stage_start
   b. Execute stage script as subprocess
   c. Check exit code
   d. Log stage_complete or pipeline_stage_failed
3. Log pipeline_complete
```

### 2.2 Inference Pipeline

#### 2.2.1 Core Lambda (`core.py`)

**Purpose**: Centralized business logic for all channels

**Design**: Single `handle()` function with six phases:

**Phase 1: Request Validation**
```python
def _validate_request_shape(event):
    # Required: language, channel, query
    # Voice channel: also requires asr_confidence
    # Returns: (validated_request, error)
```

**Phase 2: Policy Enforcement**
```python
def _intent_blocked(query):
    # Check against INTENT_BLOCKLIST_PATTERNS
    # Medical: diagnos, prescrib, symptom, treatment
    # Legal: lawsuit, attorney, legal advice
    
def _enforce_asr(asr_confidence):
    # Voice only: check >= ASR_CONF_THRESHOLD (0.75)
```

**Phase 3: Retrieval**
```python
def retrieve(event):
    # 1. Embed query (Bedrock Titan)
    # 2. Vector search (pgvector, RAW_K=50)
    # 3. Deduplicate by normalized text hash
    # 4. Re-rank: similarity × trust_weight
    # 5. Return top FINAL_K=5 passages
```

**Similarity Calculation**:
```python
def compute_similarity_from_distance(distance):
    return 1.0 / (1.0 + distance)  # L2 distance to similarity

def trust_weight_for(meta):
    mapping = {
        "gov": 1.0,
        "implementing_agency": 0.95,
        "ngo": 0.8,
        "news": 0.6
    }
    return mapping.get(meta.get("trust_level"), 1.0)

final_score = similarity × trust_weight
```

**Phase 4: Generation**
```python
def call_bedrock(prompt_text):
    # Use Bedrock Converse API
    # System: "You are a government assistant..."
    # Prompt: "LANGUAGE: {lang}\n\nPASSAGES:\n{passages}\n\nQUESTION:\n{query}"
    # Retry: max 1 retry with exponential backoff
```

**Phase 5: Validation**
```python
def _validate_generator_output_and_extract_lines_raw(raw_text, passages):
    # 1. Check for "NOT_ENOUGH_INFORMATION"
    # 2. Verify every sentence has citation [n]
    # 3. Validate citation indices: 1 <= n <= len(passages)
    # 4. Reject URLs and disallowed substrings
    # 5. Return: (decision, validated_lines)
    #    decision: "ACCEPT" | "NOT_ENOUGH_INFORMATION" | "INVALID_OUTPUT"
```

**Citation Validation Rules**:
- Explicit citations: `[1]`, `[2]`, etc. at sentence end
- Placeholder citations: `[n]` replaced with sequential numbers
- Implicit citations: Numbers in text mapped to passages
- Fallback: Sequential assignment if sentences ≤ passages

**Phase 6: Audit**
```python
def _write_audit(record):
    # Hash query text (PII protection)
    # Write to S3: {AUDIT_PREFIX}/{YYYY-MM-DD}/{request_id}.json
    # Include: session_id, language, channel, chunk_ids, resolution, timing
```

**Response Format**:
```json
{
  "request_id": "r-1234567890",
  "resolution": "answer",
  "answer_lines": [
    {"text": "Apply online at portal. [1]"}
  ],
  "citations": [
    {
      "citation": 1,
      "chunk_id": "c_123",
      "source_url": "https://...",
      "meta": {"trust_level": "gov"}
    }
  ],
  "confidence": "high"
}
```

**Error Responses**:
- `resolution: "refusal"` + `guidance_key`: Policy violation
- `resolution: "not_enough_info"`: Low similarity or no candidates
- `resolution: "invalid_output"`: Validation failure (strict mode)

#### 2.2.2 HTTP Adapter (`http.py`)

**Purpose**: Web/API interface

**Design**:
- FastAPI application with CORS middleware
- Endpoint: `POST /v1/query`
- Request normalization:
  ```python
  {
    "channel": "web",
    "language": normalize_language(body.get("language")),
    "query": body.get("query"),
    "session_id": body.get("session_id"),
    "top_k": body.get("top_k"),
    "filters": body.get("filters", {})
  }
  ```
- Response mapping: Full JSON with citations
- Lambda handler: Adapts API Gateway event format

**Key Features**:
- Language fallback: defaults to "en" if invalid
- Query parameter support: `?language=hi&q=query`
- Error handling: 400 for empty query, 500 for core failure

#### 2.2.3 SMS Adapter (`sms.py`)

**Purpose**: SMS interface for feature phones

**Design**:
- Accepts webhook from SNS/Twilio
- Request normalization:
  ```python
  {
    "channel": "sms",
    "language": body.get("language", "en"),
    "query": body.get("text"),
    "session_id": body.get("session_id")
  }
  ```
- Response optimization:
  - Extract first answer sentence only
  - Shorten source URL (domain only)
  - Format: `{answer}\nSource: {short_url}`
  - Limit: 1600 characters
- Refusal messages: Localized, concise

**Key Functions**:
- `_shorten_source()`: Extract domain from URL
- `lambda_handler()`: Webhook entry point

#### 2.2.4 Voice Adapter (`voice.py`)

**Purpose**: Voice interface for low-literacy users

**Design** (Conceptual - not fully implemented):
- **Input Pipeline**:
  1. Accept audio via telephony provider
  2. Store audio to S3
  3. Start AWS Transcribe job
  4. Extract transcript + asr_confidence
  5. Forward to core with `channel: "voice"`
- **Output Pipeline**:
  1. Receive answer from core
  2. Generate TTS using AWS Polly
  3. Store audio to S3 with signed URL
  4. Return `tts_url` to telephony provider
- **ASR Gating**: Core enforces `asr_confidence >= 0.75`
- **Response Limits**: Keep audio ≤20 seconds

## 3. Data Models

### 3.1 Database Schema

```sql
-- Main vector index table
CREATE TABLE civic_chunks (
    chunk_id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1024) NOT NULL,
    meta JSONB,
    source_url TEXT,
    page_number INTEGER,
    indexed_at TIMESTAMP DEFAULT NOW()
);

-- HNSW index for fast similarity search
CREATE INDEX civic_chunks_embedding_idx 
ON civic_chunks 
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Metadata index for filtering
CREATE INDEX civic_chunks_meta_idx 
ON civic_chunks 
USING gin (meta);
```

### 3.2 S3 Structure

```
bucket/
├── raw/
│   ├── {domain}_{hash}/
│   │   ├── by-hash/
│   │   │   ├── {content_hash}.html
│   │   │   └── {content_hash}.html.manifest.json
│   │   └── latest/
│   │       └── {fetch_hash}.manifest.json
│   └── {filename}.pdf
│       └── {filename}.pdf.manifest.json
├── chunked/
│   └── chunked_v1/
│       └── {document_hash}.chunks.jsonl
└── audits/
    └── {YYYY-MM-DD}/
        └── {request_id}.json
```

### 3.3 Manifest Format

```json
{
  "url": "https://www.india.gov.in/schemes",
  "fetched_at": "2024-01-15T10:30:00Z",
  "content_hash": "a0b668ab7bf39376...",
  "trust_level": "gov",
  "content_type": "text/html",
  "http_status": 200,
  "content_length": 45678
}
```

### 3.4 Audit Record Format

```json
{
  "session_id": "sess-abc123",
  "request_id": "r-1234567890",
  "language": "hi",
  "channel": "sms",
  "query_hash": "sha256_of_query",
  "used_chunk_ids": ["c_123", "c_456"],
  "top_similarity": 0.87,
  "resolution": "answer",
  "generator_attempts": 1,
  "timing_ms": 1850
}
```

## 4. Algorithms & Logic

### 4.1 Chunking Algorithm

```python
def chunk_text(text, chunk_size=512, overlap=128):
    tokens = tokenize(text)
    chunks = []
    start = 0
    
    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = detokenize(chunk_tokens)
        
        chunk_id = sha256(
            f"{document_hash}:{start}:{end}".encode()
        ).hexdigest()
        
        chunks.append({
            "chunk_id": chunk_id,
            "start_token": start,
            "end_token": end,
            "content": chunk_text
        })
        
        start += (chunk_size - overlap)
    
    return chunks
```

### 4.2 Re-ranking Algorithm

```python
def re_rank_and_select(candidates, final_k):
    scored = []
    
    for c in candidates:
        # Convert L2 distance to similarity
        similarity = 1.0 / (1.0 + c["distance"])
        
        # Apply trust weight
        trust = trust_weight_for(c["meta"])
        
        # Compute final score
        final_score = similarity * trust
        
        c["similarity"] = similarity
        c["trust_weight"] = trust
        c["final_score"] = final_score
        scored.append(c)
    
    # Sort by final_score (desc), then similarity (desc)
    scored.sort(
        key=lambda x: (-x["final_score"], -x["similarity"])
    )
    
    return scored[:final_k]
```

### 4.3 Deduplication Algorithm

```python
def dedupe_candidates_keep_nearest(candidates, max_keep):
    seen_keys = set()
    deduped = []
    
    for c in candidates:
        # Normalize text and hash
        normalized = unicodedata.normalize("NFKC", c["text"])
        normalized = normalized.lower().strip()
        key = sha256(normalized.encode()).hexdigest()
        
        if key in seen_keys:
            continue
        
        seen_keys.add(key)
        deduped.append(c)
        
        if len(deduped) >= max_keep:
            break
    
    return deduped
```

### 4.4 Citation Validation Algorithm

```python
def validate_citations(text, num_passages):
    sentences = split_sentences(text)
    
    for sent in sentences:
        # Check for citation marker
        match = re.search(r'\[(\d+)\]$', sent)
        
        if not match:
            return "INVALID_OUTPUT"
        
        citation_num = int(match.group(1))
        
        # Validate range
        if citation_num < 1 or citation_num > num_passages:
            return "INVALID_OUTPUT"
        
        # Check for disallowed content
        if any(sub in sent.lower() for sub in DISALLOWED_SUBSTRINGS):
            return "INVALID_OUTPUT"
    
    return "ACCEPT"
```

## 5. Configuration & Tuning

### 5.1 Retrieval Knobs

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `RAW_K` | 50 | Initial candidates from vector search |
| `FINAL_K` | 5 | Passages sent to generator |
| `MIN_SIMILARITY` | 0.60 | Threshold for refusal |
| `EMBED_DIM` | 1024 | Titan Embed v2 dimension |

### 5.2 Generation Knobs

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `BEDROCK_MODEL_ID` | qwen.qwen3-next-80b-a3b | Generation model |
| `BEDROCK_MAX_TOKENS` | 256 | Response length limit |
| `BEDROCK_TEMPERATURE` | 0.2 | Determinism (lower = more deterministic) |
| `BEDROCK_TOP_P` | 0.95 | Nucleus sampling |
| `BEDROCK_MAX_RETRIES` | 1 | Retry attempts |

### 5.3 Safety Knobs

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `ASR_CONF_THRESHOLD` | 0.75 | Voice confidence gate |
| `VALIDATION_STRICT` | 0 (false) | Reject invalid citations |
| `INTENT_BLOCKLIST` | medical, legal | Blocked query types |

### 5.4 Performance Budgets

| Budget | Default | Purpose |
|--------|---------|---------|
| `EMBED_SEARCH_BUDGET_SEC` | 2.5 | Retrieval timeout |
| `GEN_BUDGET_SEC` | 4.0 | Generation timeout |
| Lambda timeout | 10.0 | Total request timeout |

## 6. Deployment Architecture

### 6.1 Indexing Pipeline

```
EventBridge Rule (cron: 0 2 * * *)
    ↓
ECS Task Definition
    ├── Image: {ECR_REPO}:{IMAGE_TAG}
    ├── CPU: 2 vCPU
    ├── Memory: 4-8 GB
    ├── Environment:
    │   ├── AWS_REGION
    │   ├── PG_HOST, PG_PASSWORD
    │   ├── S3_RAW_BUCKET
    │   └── S3_CHUNKED_BUCKET
    └── IAM Role:
        ├── S3 read/write
        ├── Bedrock invoke
        └── Aurora connect
```

### 6.2 Inference Pipeline

```
API Gateway / ALB
    ↓
┌─────────────────────────────────────┐
│  HTTP Lambda                        │
│  ├── Runtime: Python 3.11           │
│  ├── Memory: 512 MB                 │
│  ├── Timeout: 10s                   │
│  └── Concurrency: 100               │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│  Core Lambda                        │
│  ├── Runtime: Python 3.11           │
│  ├── Memory: 1024 MB                │
│  ├── Timeout: 10s                   │
│  ├── Environment:                   │
│  │   ├── PG_HOST, PG_PASSWORD       │
│  │   ├── BEDROCK_MODEL_ID           │
│  │   ├── AUDIT_S3_BUCKET            │
│  │   └── Tuning knobs               │
│  └── IAM Role:                      │
│      ├── Aurora connect             │
│      ├── Bedrock invoke             │
│      └── S3 write (audits)          │
└─────────────────────────────────────┘
```

### 6.3 Infrastructure as Code

**Pulumi Stack** (`infra/pulumi_aws/`):
- `__main__.py`: Orchestrates all resources
- `indexing_pipeline.py`: ECS task, EventBridge, IAM
- `inference_pipeline.py`: Lambda functions, API Gateway, IAM
- `Pulumi.prod.yaml`: Production configuration values

## 7. Monitoring & Observability

### 7.1 Key Metrics

**Indexing**:
- `indexing.chunks_processed`: Count of chunks indexed
- `indexing.duration_seconds`: Pipeline execution time
- `indexing.errors`: Failed stages

**Inference**:
- `request.latency_ms`: End-to-end latency (p50, p95, p99)
- `retrieval.top_similarity`: Similarity of best match
- `retrieval.candidates_count`: Passages returned
- `generation.latency_ms`: Bedrock call duration
- `decision_counts`: ACCEPT, NOT_ENOUGH_INFO, INVALID_OUTPUT
- `channel.{http|sms|voice}.requests`: Per-channel traffic

### 7.2 Alerts

| Alert | Condition | Action |
|-------|-----------|--------|
| No candidates spike | `candidates_count=0` >10% | Check indexing pipeline |
| Invalid output rate | `INVALID_OUTPUT` >5% | Review prompt/validator |
| High latency | p95 >6s for 5min | Increase Lambda memory |
| ASR failures | Voice errors >20% | Check Transcribe config |
| SMS delivery fail | Delivery rate <95% | Check provider status |

### 7.3 Logging

**Structured JSON format**:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "component": "core",
  "event": "request_complete",
  "request_id": "r-1234567890",
  "resolution": "answer",
  "timing_ms": 1850
}
```

## 8. Security Considerations

### 8.1 Data Protection
- Query hashing in audit logs (PII protection)
- Encryption at rest: S3 (SSE-S3), Aurora (KMS)
- Encryption in transit: TLS 1.2+ for all connections
- IAM roles with least-privilege policies

### 8.2 Access Control
- No user authentication (public access)
- Rate limiting via API Gateway throttling
- WAF rules for web channel (SQL injection, XSS)
- SMS/voice rate limits at provider level

### 8.3 Content Safety
- Intent blocklist (medical, legal)
- URL filtering in generated responses
- Source trust levels for ranking
- Audit trail for accountability

## 9. Testing Strategy

### 9.1 Unit Tests
- Chunking algorithm correctness
- Citation validation logic
- Re-ranking calculations
- Request normalization

### 9.2 Integration Tests
- End-to-end indexing pipeline
- Core Lambda with mock Bedrock
- Channel adapters with mock core
- Database operations (pgvector)

### 9.3 Performance Tests
- Latency under load (100 concurrent requests)
- Vector search performance (RAW_K=50)
- Generation timeout handling
- Lambda cold start impact

### 9.4 Validation Tests
- Citation accuracy (100% coverage)
- Refusal on blocked intents (100% precision)
- Multi-language support (9 languages)
- Channel-specific formatting

## 10. Future Enhancements

### 10.1 Short-term
- Voice adapter full implementation
- Multi-turn conversation support
- Regional filtering (state/district)
- Freshness scoring in re-ranking

### 10.2 Long-term
- Real-time indexing (streaming updates)
- Personalized recommendations
- WhatsApp channel adapter
- Offline mode (cached responses)
- Analytics dashboard for administrators
