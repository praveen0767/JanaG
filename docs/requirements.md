# Requirements Document: Public Civic Information System

## 1. Executive Summary

The Public Civic Information System is a multi-channel platform designed to deliver authoritative, verifiable civic information to citizens across India through web, SMS, and voice interfaces. The system prioritizes accessibility, auditability, and deterministic behavior to serve diverse populations including low-literacy and feature-phone users.

## 2. System Overview

### 2.1 Purpose
Provide citizens with reliable, cited answers to civic questions about government schemes, services, and procedures using authoritative sources.

### 2.2 Core Principles
- **Deterministic & Auditable**: Every response is traceable to source documents with full audit trails
- **Multi-channel Access**: Web, SMS, and voice interfaces for maximum reach
- **Safety-first**: Strict validation, intent blocking, and refusal mechanisms
- **Accessibility**: Support for 9 Indian languages and low-bandwidth channels

## 3. Functional Requirements

### 3.1 Indexing Pipeline (Batch Processing)

#### FR-1.1: Data Acquisition
- **FR-1.1.1**: Crawl authoritative government websites (india.gov.in, myscheme.gov.in, csc.gov.in)
- **FR-1.1.2**: Ingest PDF documents from official sources
- **FR-1.1.3**: Store raw content with cryptographic hashes for integrity verification
- **FR-1.1.4**: Generate manifest files tracking source metadata (URL, timestamp, trust level)

#### FR-1.2: Content Processing
- **FR-1.2.1**: Parse HTML content and extract clean text
- **FR-1.2.2**: Extract text from PDF documents using OCR when necessary
- **FR-1.2.3**: Chunk documents deterministically (fixed token windows with overlap)
- **FR-1.2.4**: Generate unique chunk IDs: `sha256(document_hash + start_token + end_token)`

#### FR-1.3: Embedding & Indexing
- **FR-1.3.1**: Generate embeddings using AWS Bedrock Titan Embed (1024 dimensions)
- **FR-1.3.2**: Store vectors in Aurora PostgreSQL with pgvector extension
- **FR-1.3.3**: Create HNSW indexes for efficient similarity search
- **FR-1.3.4**: Support idempotent re-indexing with `INSERT ... ON CONFLICT DO NOTHING`

#### FR-1.4: Scheduling
- **FR-1.4.1**: Run indexing pipeline on configurable schedule (default: nightly)
- **FR-1.4.2**: Support priority feeds with higher frequency (e.g., every 6 hours)
- **FR-1.4.3**: Execute as ECS Fargate task with parallel workers (N=4)

### 3.2 Inference Pipeline (Real-time)

#### FR-2.1: Request Handling
- **FR-2.1.1**: Accept requests from three channels: web (HTTP), SMS, voice
- **FR-2.1.2**: Validate required fields: language (REQUIRED), channel, query
- **FR-2.1.3**: Generate unique request_id and track session_id
- **FR-2.1.4**: Support 9 languages: en, hi, ta, bn, te, ml, kn, mr, gu

#### FR-2.2: Safety & Policy Enforcement
- **FR-2.2.1**: Block medical queries (diagnoses, prescriptions, treatments)
- **FR-2.2.2**: Block legal queries (lawsuits, contracts, legal advice)
- **FR-2.2.3**: Enforce ASR confidence threshold (≥0.75) for voice channel
- **FR-2.2.4**: Refuse queries with insufficient evidence (similarity <0.60)

#### FR-2.3: Retrieval
- **FR-2.3.1**: Embed user query using same model as indexing
- **FR-2.3.2**: Perform vector similarity search (RAW_K=50 candidates)
- **FR-2.3.3**: Deduplicate semantically identical chunks
- **FR-2.3.4**: Re-rank by: similarity × trust_weight × freshness
- **FR-2.3.5**: Return top FINAL_K=5 passages

#### FR-2.4: Generation
- **FR-2.4.1**: Construct prompt with numbered passages and user query
- **FR-2.4.2**: Call AWS Bedrock (default: qwen.qwen3-next-80b-a3b)
- **FR-2.4.3**: Retry failed generation calls (max 1 retry with exponential backoff)
- **FR-2.4.4**: Enforce generation timeout budget (4.0 seconds)

#### FR-2.5: Validation
- **FR-2.5.1**: Verify every sentence ends with citation marker [n]
- **FR-2.5.2**: Validate citation indices are within valid range [1..FINAL_K]
- **FR-2.5.3**: Reject responses containing URLs or disallowed substrings
- **FR-2.5.4**: Handle "NOT_ENOUGH_INFORMATION" responses appropriately
- **FR-2.5.5**: Support strict and relaxed validation modes

#### FR-2.6: Citation Hydration
- **FR-2.6.1**: Map citation numbers to source metadata
- **FR-2.6.2**: Convert S3 URLs to HTTPS URLs for public access
- **FR-2.6.3**: Include chunk_id, source_url, and metadata in response

#### FR-2.7: Audit Trail
- **FR-2.7.1**: Write audit record to S3 for every request
- **FR-2.7.2**: Hash sensitive query text (store hash, not plaintext)
- **FR-2.7.3**: Record: session_id, request_id, language, channel, chunk_ids, resolution, timing
- **FR-2.7.4**: Organize audit logs by date prefix (YYYY-MM-DD)

### 3.3 Channel Adapters

#### FR-3.1: HTTP/Web Channel
- **FR-3.1.1**: Expose REST API endpoint: POST /v1/query
- **FR-3.1.2**: Accept JSON request body with query, language, filters
- **FR-3.1.3**: Return full JSON response with answer_lines and citations
- **FR-3.1.4**: Support CORS for browser-based clients
- **FR-3.1.5**: Implement as Lambda function or FastAPI service

#### FR-3.2: SMS Channel
- **FR-3.2.1**: Accept SMS messages via SNS/Twilio webhook
- **FR-3.2.2**: Normalize inbound message to canonical request format
- **FR-3.2.3**: Limit response to 1600 characters (SMS-friendly)
- **FR-3.2.4**: Return first answer sentence + shortened source URL
- **FR-3.2.5**: Handle delivery via configured SMS provider

#### FR-3.3: Voice Channel
- **FR-3.3.1**: Accept audio input and store to S3
- **FR-3.3.2**: Transcribe using AWS Transcribe (streaming or batch)
- **FR-3.3.3**: Pass asr_confidence to core for gating
- **FR-3.3.4**: Generate TTS audio using AWS Polly
- **FR-3.3.5**: Keep audio responses ≤20 seconds
- **FR-3.3.6**: Support "repeat" and "speak slowly" commands

## 4. Non-Functional Requirements

### 4.1 Performance
- **NFR-1.1**: Median latency (channel + core): ≤2 seconds
- **NFR-1.2**: P95 latency: ≤6 seconds
- **NFR-1.3**: Embedding + search budget: ≤2.5 seconds
- **NFR-1.4**: Generation budget: ≤4.0 seconds
- **NFR-1.5**: Metadata fetch budget: ≤0.3 seconds

### 4.2 Scalability
- **NFR-2.1**: Support serverless auto-scaling for inference workload
- **NFR-2.2**: Handle concurrent requests via Lambda concurrency limits
- **NFR-2.3**: Scale indexing workers horizontally (parallel ECS tasks)
- **NFR-2.4**: Support database connection pooling for pgvector

### 4.3 Reliability
- **NFR-3.1**: Deterministic refusal on invalid/insufficient evidence: 100%
- **NFR-3.2**: Idempotent indexing operations (safe to re-run)
- **NFR-3.3**: Graceful degradation when external services fail
- **NFR-3.4**: Retry transient errors with exponential backoff

### 4.4 Security
- **NFR-4.1**: Hash sensitive query data in audit logs
- **NFR-4.2**: Use IAM roles for AWS service authentication
- **NFR-4.3**: Encrypt data at rest (S3, Aurora)
- **NFR-4.4**: Encrypt data in transit (TLS/HTTPS)
- **NFR-4.5**: Implement WAF rules for web channel
- **NFR-4.6**: Rate limiting at provider level (SMS/voice)

### 4.5 Observability
- **NFR-5.1**: Structured JSON logging for all components
- **NFR-5.2**: Emit CloudWatch metrics: latency, similarity, decision counts
- **NFR-5.3**: Alert on: no_candidates spike, INVALID_OUTPUT threshold, latency breach
- **NFR-5.4**: Track per-channel error rates and delivery failures

### 4.6 Maintainability
- **NFR-6.1**: Infrastructure as Code using Pulumi (AWS)
- **NFR-6.2**: Deterministic Docker builds for indexing pipeline
- **NFR-6.3**: Pinned dependency versions in requirements.txt
- **NFR-6.4**: Separate runtime and infrastructure dependencies

### 4.7 Accessibility
- **NFR-7.1**: Support 9 Indian languages with proper Unicode handling
- **NFR-7.2**: SMS channel for feature-phone users
- **NFR-7.3**: Voice channel for low-literacy users
- **NFR-7.4**: Low-bandwidth optimization for SMS/voice

## 5. Data Requirements

### 5.1 Input Data Sources
- Government websites: india.gov.in, myscheme.gov.in, csc.gov.in
- Official PDF documents: scheme guidelines, application procedures
- Trust levels: gov (1.0), implementing_agency (0.95), ngo (0.8), news (0.6)

### 5.2 Data Storage
- **Raw content**: S3 bucket with hash-based organization
- **Manifests**: JSON files tracking source metadata
- **Chunked content**: JSONL files with chunk metadata
- **Vector index**: Aurora PostgreSQL with pgvector extension
- **Audit logs**: S3 bucket with date-partitioned structure

### 5.3 Data Retention
- Raw content: Indefinite (immutable, content-addressed)
- Audit logs: Minimum 90 days, configurable retention policy
- Vector index: Updated incrementally, old versions pruned

## 6. Integration Requirements

### 6.1 AWS Services
- **Bedrock**: Embedding (Titan) and generation (Qwen/Claude)
- **Aurora PostgreSQL**: Vector storage with pgvector extension
- **S3**: Raw content, manifests, audit logs
- **ECS Fargate**: Indexing pipeline execution
- **Lambda**: Inference channel adapters and core logic
- **EventBridge**: Scheduling for indexing cron
- **Transcribe**: Speech-to-text for voice channel
- **Polly**: Text-to-speech for voice channel
- **CloudWatch**: Logging and metrics

### 6.2 External Services
- SMS provider: SNS, Pinpoint, or Twilio
- Voice provider: Telephony integration (IVR)

## 7. Constraints & Assumptions

### 7.1 Constraints
- AWS region availability for Bedrock models
- Lambda timeout limits (10 seconds for core)
- SMS message length limits (1600 characters)
- Voice response duration limits (20 seconds)
- Database connection limits for Aurora

### 7.2 Assumptions
- Authoritative sources are accessible and stable
- Government websites allow automated crawling
- Users have basic familiarity with their chosen channel
- Network connectivity available for real-time inference
- AWS services maintain published SLAs

## 8. Success Criteria

### 8.1 Technical Metrics
- Median latency <2s, P95 <6s
- Citation accuracy: 100% (every sentence cited)
- Refusal rate on blocked intents: 100%
- Indexing pipeline success rate: >99%

### 8.2 User Impact
- Multi-channel accessibility (web + SMS + voice)
- Support for 9 Indian languages
- Verifiable, authoritative responses with source citations
- Audit trail for transparency and accountability

## 9. Out of Scope

- Real-time content updates (batch indexing only)
- User authentication/authorization (public access)
- Personalized recommendations
- Conversational multi-turn dialogue
- Content creation or editing by users
- Integration with government backend systems
- Payment processing or transactional operations
