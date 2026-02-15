#!/usr/bin/env bash
# scripts/build_and_push.sh
# Idempotent multi-arch build + push to Docker Hub or ECR.
# - Builds multi-arch images with docker buildx
# - Skips push when remote tag already exists (best-effort)
# - Keeps logic testable and runnable locally
# -------------------------
# Configurable inputs (via env or defaults)
# -------------------------
IMAGE_NAME="${IMAGE_NAME:-civic-indexing}"
IMAGE_TAG="${IMAGE_TAG:-amd64-arm64-v6}"
BUILD_CONTEXT="${BUILD_CONTEXT:-./indexing_pipeline}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-${BUILD_CONTEXT}/Dockerfile}"

# Platform configuration
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
LOCAL_PLATFORM="${LOCAL_PLATFORM:-linux/amd64}"

OCR_LANGUAGES="${OCR_LANGUAGES:-eng,tam,hin}"
INDIC_OCR_SIZE="${INDIC_OCR_SIZE:-best}"

PUSH="$(printf '%s' "${PUSH:-true}" | tr '[:upper:]' '[:lower:]')"
REGISTRY_TYPE="${REGISTRY_TYPE:-dockerhub}"   # dockerhub | ecr
ECR_REPO="${ECR_REPO:-}"                      # full ECR repo if using ecr (e.g. 123.dkr.ecr.region.amazonaws.com/repo)
AWS_REGION="${AWS_REGION:-ap-south-1}"

# Docker Hub creds (expected via environment/repo secrets)
DOCKER_USERNAME="${DOCKER_USERNAME:-}"
DOCKER_PASSWORD="${DOCKER_PASSWORD:-}"

# Retries and timeouts
RETRY_ATTEMPTS="${RETRY_ATTEMPTS:-5}"
RETRY_DELAY="${RETRY_DELAY:-2}"   # exponential backoff base (seconds)

# Smoke test (optional)
SMOKE_TEST="$(printf '%s' "${SMOKE_TEST:-false}" | tr '[:upper:]' '[:lower:]')"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-60}"
HEALTH_PORT="${HEALTH_PORT:-8080}"
HEALTH_PATH="${HEALTH_PATH:-/health}"
CONTAINER_SHM="${CONTAINER_SHM:-1g}"

# Internal names
BUILDER_NAME="${BUILDER_NAME:-ci-buildx}"
LOCAL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

# -------------------------
# Utilities
# -------------------------
log(){ printf '\033[0;34m[INFO]\033[0m %s\n' "$*"; }
warn(){ printf '\033[0;33m[WARN]\033[0m %s\n' "$*" >&2; }
err(){ printf '\033[0;31m[ERROR]\033[0m %s\n' "$*" >&2; }

retry_cmd(){
  local -i attempts=0 rc=0
  local cmd=( "$@" )
  while :; do
    attempts+=1
    "${cmd[@]}" && { rc=0; break; } || rc=$?
    if [ "$attempts" -ge "$RETRY_ATTEMPTS" ]; then break; fi
    sleep $((RETRY_DELAY ** (attempts - 1)))
    log "Retrying (${attempts}/${RETRY_ATTEMPTS})..."
  done
  return $rc
}

# normalize boolean
is_true(){ case "${1:-}" in true|1|yes|y) return 0 ;; *) return 1 ;; esac; }

# cleanup helper for smoke container
cleanup_container(){
  set +e
  if docker ps -a --format '{{.Names}}' | grep -xq "${1:-}"; then
    docker rm -f "${1:-}" >/dev/null 2>&1 || true
  fi
  set -e
}

# -------------------------
# Preconditions
# -------------------------
if ! command -v docker >/dev/null 2>&1; then err "docker CLI required"; exit 2; fi
if ! command -v docker-buildx >/dev/null 2>&1 2>/dev/null; then
  # docker-buildx may not be a separate binary; rely on docker buildx
  true
fi
if [ ! -f "${DOCKERFILE_PATH}" ]; then err "Dockerfile not found at ${DOCKERFILE_PATH}"; exit 3; fi
if [ ! -d "${BUILD_CONTEXT}" ]; then err "Build context not found: ${BUILD_CONTEXT}"; exit 3; fi

# Make PUSH boolean
if is_true "$PUSH"; then PUSH=true; else PUSH=false; fi

# -------------------------
# Compute stable build hash (idempotence hint)
# - Prefer git commit if available, else hash Dockerfile + args.
# - This hash is added as image label so remote inspection can correlate builds.
# -------------------------
GIT_COMMIT="$(git rev-parse --verify --short HEAD 2>/dev/null || true)"
if [ -n "$GIT_COMMIT" ]; then
  BUILD_HASH_INPUT="$GIT_COMMIT|${OCR_LANGUAGES}|${INDIC_OCR_SIZE}"
else
  BUILD_HASH_INPUT="$(sha256sum "${DOCKERFILE_PATH}" | awk '{print $1}')|${OCR_LANGUAGES}|${INDIC_OCR_SIZE}"
fi
BUILD_HASH="$(printf '%s' "$BUILD_HASH_INPUT" | sha256sum | awk '{print $1}')"
LABEL_BUILD_HASH="civic.build.hash=${BUILD_HASH}"

log "IMAGE_NAME=${IMAGE_NAME} IMAGE_TAG=${IMAGE_TAG} PLATFORMS=${PLATFORMS} PUSH=${PUSH} REGISTRY_TYPE=${REGISTRY_TYPE}"
log "Using build hash: ${BUILD_HASH}"

# -------------------------
# Ensure buildx builder exists and qemu registered (best-effort)
# -------------------------
log "Ensuring buildx builder '${BUILDER_NAME}' exists and is bootstrapped"
if ! docker buildx inspect "${BUILDER_NAME}" >/dev/null 2>&1; then
  docker buildx create --name "${BUILDER_NAME}" --use >/dev/null
fi
# bootstrap builder
docker buildx inspect --bootstrap >/dev/null

# register qemu user static (best-effort; ignorable on some runners)
if ! docker run --rm --privileged tonistiigi/binfmt --version >/dev/null 2>&1; then
  # try multiarch qemu image (some runners already have binfmt)
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes >/dev/null 2>&1 || true
fi

# -------------------------
# Compute full image names
# -------------------------
if [ "${REGISTRY_TYPE}" = "ecr" ]; then
  if [ -z "${ECR_REPO}" ]; then err "ECR_REPO must be set for ECR pushes (full repo URL)"; exit 4; fi
  IMAGE_FULL="${ECR_REPO}:${IMAGE_TAG}"
  IMAGE_FULL_SHA="${ECR_REPO}:$(printf '%s' "${IMAGE_TAG}-${GIT_COMMIT:-local}" | sha256sum | cut -d' ' -f1)"
else
  if [ -z "${DOCKER_USERNAME:-}" ]; then
    warn "DOCKER_USERNAME not set; pushing will likely fail unless using local-only mode"
  fi
  IMAGE_FULL="${DOCKER_USERNAME:-}${DOCKER_USERNAME:+/}${IMAGE_NAME}:${IMAGE_TAG}"
fi

# Also prepare immutable sha tag
if [ -n "${GIT_COMMIT}" ]; then
  SHA_TAG="${IMAGE_FULL%:*}:${GIT_COMMIT}"
else
  SHA_TAG="${IMAGE_FULL%:*}:$(date +%s)"
fi

# -------------------------
# Helper: remote manifest existence (best-effort)
# returns 0 if remote tag exists (manifest present), 1 otherwise
# Uses buildx imagetools inspect -> tolerant of transient failures
# -------------------------
remote_tag_exists(){
  local image_ref="$1"
  local attempts=0
  while [ $attempts -lt "$RETRY_ATTEMPTS" ]; do
    if docker buildx imagetools inspect "$image_ref" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts+1))
    sleep $((RETRY_DELAY ** (attempts - 1)))
  done
  return 1
}

# -------------------------
# If pushing and remote exists with same tag -> skip build/push (idempotent)
# Note: this is best-effort and may be subject to registry eventual consistency.
# -------------------------
if [ "$PUSH" = true ]; then
  log "Checking whether remote image already exists: ${IMAGE_FULL}"
  if remote_tag_exists "${IMAGE_FULL}"; then
    log "Remote image ${IMAGE_FULL} already exists. Skipping build+push."
    # Optionally perform smoke test by pulling remote image if requested
    if is_true "$SMOKE_TEST"; then
      log "Pulling ${IMAGE_FULL} for smoke test"
      retry_cmd docker pull "${IMAGE_FULL}" || { warn "docker pull failed; skipping smoke test"; true; }
      # run smoke
      CONTAINER_NAME="ci-smoke-${IMAGE_NAME//[^a-zA-Z0-9]/}-${$}"
      cleanup_container "${CONTAINER_NAME}"
      docker run -d --name "${CONTAINER_NAME}" -p "${HEALTH_PORT}:$HEALTH_PORT" --shm-size="${CONTAINER_SHM}" "${IMAGE_FULL}"
      start_ts=$(date +%s); ok=false
      while :; do
        if curl -fsS --max-time 2 "http://127.0.0.1:${HEALTH_PORT}${HEALTH_PATH}" >/dev/null 2>&1; then ok=true; break; fi
        now=$(date +%s)
        if [ $((now-start_ts)) -ge "${SMOKE_TIMEOUT}" ]; then break; fi
        sleep 1
      done
      if [ "$ok" != true ]; then docker logs --tail 200 "${CONTAINER_NAME}" || true; cleanup_container "${CONTAINER_NAME}"; err "Smoke test failed"; exit 5; fi
      cleanup_container "${CONTAINER_NAME}"
      log "Smoke test passed against remote image"
    fi
    exit 0
  fi
fi

# -------------------------
# Build (multi-arch) and push or load
# - When PUSH=true we do a single buildx invocation with --push
# - When PUSH=false we do a single-platform buildx invocation with --load
# -------------------------
buildx_build(){
  local push_flag="$1"   # "push" or "load"
  local tags=("$@")      # tags passed from caller
  # construct tag args
  local tag_args=()
  for t in "${tags[@]:1}"; do
    tag_args+=(--tag "$t")
  done

  log "Running buildx build (mode=${push_flag})"
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${PLATFORMS}" \
    "${tag_args[@]}" \
    --label "${LABEL_BUILD_HASH}" \
    --build-arg "OCR_LANGUAGES=${OCR_LANGUAGES}" \
    --build-arg "INDIC_OCR_SIZE=${INDIC_OCR_SIZE}" \
    --file "${DOCKERFILE_PATH}" \
    ${push_flag} \
    "${BUILD_CONTEXT}"
}

# Prepare tag list
TAGS_LIST=( "${IMAGE_FULL}" "${SHA_TAG}" )
# include latest only if explicitly requested
if is_true "${PUSH_LATEST:-false}"; then
  TAGS_LIST+=( "${IMAGE_FULL%:*}:latest" )
fi

if [ "$PUSH" != true ]; then
  # Local single-arch load
  log "PUSH is false -> building single-arch and loading to local daemon: ${LOCAL_PLATFORM}"
  # rebuild local tag name
  LOCAL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
  # Build and load single-arch
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${LOCAL_PLATFORM}" \
    --tag "${LOCAL_TAG}" \
    --label "${LABEL_BUILD_HASH}" \
    --build-arg "OCR_LANGUAGES=${OCR_LANGUAGES}" \
    --build-arg "INDIC_OCR_SIZE=${INDIC_OCR_SIZE}" \
    --file "${DOCKERFILE_PATH}" \
    --load \
    "${BUILD_CONTEXT}"
  log "Built local image ${LOCAL_TAG}"
  exit 0
fi

# -------------------------
# Authenticate for pushes
# -------------------------
if [ "${REGISTRY_TYPE}" = "ecr" ]; then
  if ! command -v aws >/dev/null 2>&1; then err "aws CLI required for ECR push"; exit 6; fi
  log "Logging into ECR (via aws)"
  AWS_REGION="${AWS_REGION:-$(aws configure get region || true)}"
  if [ -z "${AWS_REGION}" ]; then err "AWS_REGION is required for ECR push"; exit 7; fi
  # ensure repository exists (idempotent)
  ECR_REPO_PATH="$(echo "${ECR_REPO}" | sed 's|.*/||')"
  aws ecr describe-repositories --repository-names "${ECR_REPO_PATH}" --region "${AWS_REGION}" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${ECR_REPO_PATH}" --region "${AWS_REGION}" >/dev/null
  log "ECR repository ensured"
  # login
  retry_cmd aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "$(echo "${ECR_REPO}" | sed 's|/.*||')"
else
  if [ -z "${DOCKER_USERNAME:-}" ]; then warn "DOCKER_USERNAME not set"; fi
  if [ -n "${DOCKER_PASSWORD:-}" ]; then
    log "Logging into Docker Hub as ${DOCKER_USERNAME}"
    printf '%s\n' "${DOCKER_PASSWORD}" | docker login -u "${DOCKER_USERNAME}" --password-stdin
  else
    warn "DOCKER_PASSWORD not provided; attempting anonymous push (may fail)"
  fi
fi

# -------------------------
# Build & push (multi-arch) - tags: IMAGE_FULL and SHA_TAG (and optional latest)
# -------------------------
# expand tags into arguments
tag_args=()
for t in "${TAGS_LIST[@]}"; do
  tag_args+=(--tag "${t}")
done

log "Building and pushing tags: ${TAGS_LIST[*]}"
# Use retry wrapper for build/push command
retry_cmd docker buildx build \
  --builder "${BUILDER_NAME}" \
  --platform "${PLATFORMS}" \
  "${tag_args[@]}" \
  --label "${LABEL_BUILD_HASH}" \
  --build-arg "OCR_LANGUAGES=${OCR_LANGUAGES}" \
  --build-arg "INDIC_OCR_SIZE=${INDIC_OCR_SIZE}" \
  --file "${DOCKERFILE_PATH}" \
  --push \
  "${BUILD_CONTEXT}" || { err "buildx build/push failed"; exit 10; }

log "Buildx push completed"

# -------------------------
# Optional smoke test: pull image and run
# -------------------------
if is_true "${SMOKE_TEST}"; then
  log "Running smoke test: pull ${IMAGE_FULL}"
  retry_cmd docker pull "${IMAGE_FULL}" || { warn "docker pull failed; continuing"; }
  CONTAINER_NAME="ci-smoke-${IMAGE_NAME//[^a-zA-Z0-9]/}-${$}"
  cleanup_container "${CONTAINER_NAME}"
  docker run -d --name "${CONTAINER_NAME}" -p "${HEALTH_PORT}:${HEALTH_PORT}" --shm-size="${CONTAINER_SHM}" "${IMAGE_FULL}"
  start_ts=$(date +%s); ok=false
  while :; do
    if curl -fsS --max-time 2 "http://127.0.0.1:${HEALTH_PORT}${HEALTH_PATH}" >/dev/null 2>&1; then ok=true; break; fi
    now=$(date +%s)
    if [ $((now-start_ts)) -ge "${SMOKE_TIMEOUT}" ]; then break; fi
    sleep 1
  done
  if [ "$ok" != true ]; then docker logs --tail 200 "${CONTAINER_NAME}" || true; cleanup_container "${CONTAINER_NAME}"; err "Smoke test failed"; exit 11; fi
  cleanup_container "${CONTAINER_NAME}"
  log "Smoke test passed"
fi

log "Done. Image(s) available at: ${TAGS_LIST[*]}"
exit 0
