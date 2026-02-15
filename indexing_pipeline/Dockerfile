# Build-time args:
#   OCR_LANGUAGES  - comma-separated list (default "eng,tam,hin")
#   INDIC_OCR_SIZE - best|standard|fast (default "best")
#
# Assumptions & invariants (fail-fast when violated):
# - Docker build runs as root during build stage (no sudo required).
# - Primary canonical traineddata source is tesseract-ocr/tessdata_best (stable).
# - indic-ocr is only consulted for Indic languages and only as a fallback (not primary).
# - No timestamps or non-deterministic names are used in persistent paths.
# - Script is idempotent: existing traineddata files are skipped.
# - Network fallbacks are deterministic and limited to a predictable ordered list.
# - Build must not fail if a particular traineddata is not available — we warn instead.
#
# External contracts:
# - Tesseract in runtime expects traineddata files under the tessdata directory reported by `tesseract --print-tessdata-dir`.
# - OCR_LANGUAGES must be comma-separated language codes (no spaces required).
#
# Deterministic behavior: curl retries + failure checks; non-fatal when file missing.

ARG OCR_LANGUAGES="eng,tam,hin"
ARG INDIC_OCR_SIZE="best"

FROM python:3.11-slim AS build

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    OCR_LANGUAGES="${OCR_LANGUAGES}" \
    INDIC_OCR_SIZE="${INDIC_OCR_SIZE}"

WORKDIR /app

# Install system packages required for Tesseract + curl/unzip + build tools
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      unzip \
      build-essential \
      pkg-config \
      libleptonica-dev \
      libtesseract-dev \
      tesseract-ocr \
      tesseract-ocr-eng \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python deps (do not install awscli via pip here)
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
 && python -m pip install --no-cache-dir -r /app/requirements.txt

# Copy application
COPY . /app

# Install tessdata for requested languages in a deterministic, limited-fallback order.
# Primary canonical source = tessdata_best (preferred). Then indic-ocr only for Indic langs,
# then standard and fast repos as additional fallbacks.
RUN cat > /tmp/install_tessdata.sh <<'SH'
#!/bin/sh
set -euo pipefail

# Deterministic candidate lists
PRIMARY_BEST="https://github.com/tesseract-ocr/tessdata_best/raw/main"
PRIMARY_STANDARD="https://github.com/tesseract-ocr/tessdata/raw/main"
PRIMARY_FAST="https://github.com/tesseract-ocr/tessdata_fast/raw/main"
INDIC_BASE="https://github.com/indic-ocr/tessdata/raw/main"

# Deterministic Indic languages set (space-separated)
INDIC_LANGS="asm ben guj hin kan mal mar nep ori pan san tam tel mni sat"

# Determine tessdata dir (use tesseract reported dir if available)
TESSDIR=""
if command -v tesseract >/dev/null 2>&1; then
  # If tesseract prints a path, use it; else fallback to common locations.
  if tesseract --print-tessdata-dir >/dev/null 2>&1; then
    TESSDIR="$(tesseract --print-tessdata-dir | tr -d '[:space:]')"
  fi
fi

# fallback dirs
if [ -z "$TESSDIR" ]; then
  for d in /usr/share/tessdata /usr/share/tesseract-ocr/4.00/tessdata /usr/share/tesseract-ocr/tessdata /usr/local/share/tessdata; do
    if [ -d "$d" ]; then
      TESSDIR="$d"
      break
    fi
  done
fi
if [ -z "$TESSDIR" ]; then
  TESSDIR="/usr/share/tessdata"
  mkdir -p "$TESSDIR"
fi

printf '%s\n' "$TESSDIR" > /tmp/tessdir
chmod 0644 /tmp/tessdir

# Choose primary base according to INDIC_OCR_SIZE
case "${INDIC_OCR_SIZE:-best}" in
  best) PRIMARY_BASE="$PRIMARY_BEST" ;;
  standard) PRIMARY_BASE="$PRIMARY_STANDARD" ;;
  fast) PRIMARY_BASE="$PRIMARY_FAST" ;;
  *) PRIMARY_BASE="$PRIMARY_BEST" ;;
esac
printf '%s\n' "$PRIMARY_BASE" > /tmp/primary_tessbase
chmod 0644 /tmp/primary_tessbase

# If no languages requested, exit cleanly
if [ -z "${OCR_LANGUAGES:-}" ]; then
  echo "No OCR_LANGUAGES specified; skipping tessdata downloads."
  exit 0
fi

# Normalize comma-separated list -> space-separated (strip whitespace)
langs=$(printf '%s' "${OCR_LANGUAGES}" | tr -d '[:space:]' | tr ',' ' ')
# Helper: check if a language is in Indic set
is_indic() {
  target="$1"
  for i in $INDIC_LANGS; do
    if [ "$i" = "$target" ]; then
      return 0
    fi
  done
  return 1
}

# Deterministic curl options
CURL_OPTS="-fsSL --retry 3 --retry-delay 2 --max-time 30"

for lang in $langs; do
  [ -z "$lang" ] && continue
  target="$TESSDIR/${lang}.traineddata"
  if [ -f "$target" ]; then
    echo "SKIP: ${lang} already present at ${target}"
    continue
  fi

  # Build deterministic candidate URL list:
  # 1) PRIMARY_BASE (canonical)
  # 2) indic-ocr (only if lang is indic)  <-- secondary, best-effort
  # 3) PRIMARY_STANDARD (if different)
  # 4) PRIMARY_FAST (if different)
  urls="${PRIMARY_BASE}/${lang}.traineddata"
  if is_indic "$lang"; then
    urls="${urls} ${INDIC_BASE}/${lang}.traineddata"
  fi
  # always include standard and fast as last-tier fallbacks if not duplicate
  urls="${urls} ${PRIMARY_STANDARD}/${lang}.traineddata ${PRIMARY_FAST}/${lang}.traineddata"

  installed=0
  # Try each URL in order; ensure file non-empty after download
  for u in $urls; do
    echo "Trying $u"
    # download to deterministic temp path
    TMP="/tmp/${lang}.traineddata"
    rm -f "$TMP"
    if curl $CURL_OPTS "$u" -o "$TMP"; then
      # ensure non-empty and reasonable size
      if [ -s "$TMP" ]; then
        mv "$TMP" "$target"
        chmod 0644 "$target"
        echo "Installed $target from $u"
        installed=1
        break
      else
        echo "Downloaded file empty from $u; skipping"
        rm -f "$TMP"
      fi
    else
      echo "Failed to download $u"
      rm -f "$TMP" || true
    fi
  done

  if [ "$installed" -ne 1 ]; then
    echo "WARNING: could not obtain traineddata for $lang from any source; OCR for this language may fail."
  fi
done

# Ensure tessdata dir readable at runtime
chmod -R a+rX "$TESSDIR" || true
SH

# Run the installer script deterministically during build
RUN chmod +x /tmp/install_tessdata.sh && /tmp/install_tessdata.sh

# Create non-root runtime user and chown app dir
RUN useradd -m -u 1000 app || true && chown -R app:app /app

USER app
WORKDIR /app
ENV PATH="/home/app/.local/bin:$PATH"

# Default command (override on docker run if necessary)
CMD ["python", "main.py"]
