export OCR_LANGUAGES="eng+tam+hin"
export INDIC_OCR_SIZE="best"   # best | standard | fast


#!/usr/bin/env bash
# infra/scripts/local_bootstrap.sh
# Idempotent bootstrap for Tesseract + traineddata (including Indic) + awscli
# Expects env:
#   OCR_LANGUAGES   e.g. "eng+tam+hin"  (required)
#   INDIC_OCR_SIZE  best|standard|fast   (optional, default=best)
#
# Behavior:
#  - Installs tesseract (via package manager) if missing
#  - Installs awscli (pkg or official installer fallback) if missing
#  - Detects tessdata dir and downloads traineddata files for requested languages
#  - Tries indic-ocr first for Indic languages, then tessdata_best/tessdata/tessdata_fast
#  - Is idempotent: skips files already present
#  - Retries network downloads, reports per-language results and exits non-zero if any requested language failed
set -euo pipefail

# --- Config / env
OCR_LANGUAGES="${OCR_LANGUAGES:-}"
INDIC_OCR_SIZE="${INDIC_OCR_SIZE:-best}"
RETRY_COUNT=3
RETRY_DELAY=2

if [ -z "$OCR_LANGUAGES" ]; then
  echo "ERROR: OCR_LANGUAGES must be set. Example: export OCR_LANGUAGES='eng+tam+hin'"
  exit 2
fi

case "$INDIC_OCR_SIZE" in
  best|standard|fast) ;;
  *)
    echo "WARN: INDIC_OCR_SIZE='$INDIC_OCR_SIZE' not recognised; defaulting to 'best'"
    INDIC_OCR_SIZE="best"
    ;;
esac

IFS='+' read -r -a LANGS <<< "$OCR_LANGUAGES"
echo "Requested languages: ${LANGS[*]}"
echo "Indic model size: ${INDIC_OCR_SIZE}"

# --- Sources (raw file URLs)
TESSDATA_BEST="https://github.com/tesseract-ocr/tessdata_best/raw/main"
TESSDATA_STANDARD="https://github.com/tesseract-ocr/tessdata/raw/main"
TESSDATA_FAST="https://github.com/tesseract-ocr/tessdata_fast/raw/main"
INDIC_OCR_BASE="https://github.com/indic-ocr/tessdata/raw/main"

# choose default tessdata base according to INDIC_OCR_SIZE
case "$INDIC_OCR_SIZE" in
  best)   PRIMARY_TESSBASE="$TESSDATA_BEST" ;;
  standard) PRIMARY_TESSBASE="$TESSDATA_STANDARD" ;;
  fast)   PRIMARY_TESSBASE="$TESSDATA_FAST" ;;
esac

# Indic language set to prefer indic-ocr (space-separated)
INDIC_LANGS_SET="asm ben guj hin kan mal mar nep ori pan san tam tel mni sat"

# --- helpers
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  fi
fi

command_exists() { command -v "$1" >/dev/null 2>&1; }

log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"; }

# choose package manager
PKG_INSTALL=""
PKG_UPDATE=""
INSTALLER=""
if command_exists apt-get; then
  INSTALLER="apt"
  PKG_UPDATE="$SUDO apt-get update -y"
  PKG_INSTALL="$SUDO apt-get install -y --no-install-recommends"
elif command_exists dnf; then
  INSTALLER="dnf"
  PKG_UPDATE="$SUDO dnf makecache -y"
  PKG_INSTALL="$SUDO dnf install -y"
elif command_exists yum; then
  INSTALLER="yum"
  PKG_UPDATE="$SUDO yum makecache -y"
  PKG_INSTALL="$SUDO yum install -y"
else
  log "ERROR: No supported package manager found (apt, dnf, yum). Install tesseract and awscli manually."
  exit 3
fi

# update repos (best-effort)
log "Updating package indexes..."
$PKG_UPDATE

# install common tools
log "Installing curl, unzip, jq if missing..."
$PKG_INSTALL curl ca-certificates unzip jq || log "Package manager install returned non-zero; continuing"

# install tesseract (idempotent)
if ! command_exists tesseract; then
  log "Tesseract not found, installing via ${INSTALLER}..."
  if [ "$INSTALLER" = "apt" ]; then
    $PKG_INSTALL tesseract-ocr libleptonica-dev libtesseract-dev pkg-config || true
  else
    # RHEL/CentOS/Amazon variants: enable EPEL if available, then install
    if [ "$INSTALLER" != "apt" ]; then
      # try to install basic tesseract package; may fail on some distros but package managers will pick best candidate
      $PKG_INSTALL tesseract || true
    fi
  fi
else
  log "Tesseract already installed: $(tesseract --version | head -n1 | tr -s ' ')"
fi

if ! command_exists tesseract; then
  log "ERROR: tesseract is still not available after install attempt. Please install tesseract manually. Aborting."
  exit 4
fi

# install aws cli if missing (try package manager first, then official installer)
if command_exists aws; then
  log "aws CLI already installed: $(aws --version 2>&1 | tr -d '\n')"
else
  log "aws CLI not found; attempting package manager install..."
  if [ "$INSTALLER" = "apt" ]; then
    if $PKG_INSTALL awscli >/dev/null 2>&1; then
      log "Installed awscli from apt"
    else
      log "apt install awscli failed; falling back to official installer"
    fi
  else
    if $PKG_INSTALL awscli >/dev/null 2>&1; then
      log "Installed awscli from package manager"
    else
      log "Package manager install of awscli failed; falling back to official installer"
    fi
  fi

  if ! command_exists aws; then
    # Official AWS CLI v2 installer (x86_64)
    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR"' EXIT
    log "Downloading official AWS CLI v2 installer..."
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$TMPDIR/awscliv2.zip"
    unzip -q "$TMPDIR/awscliv2.zip" -d "$TMPDIR"
    log "Running AWS CLI installer (may require sudo)..."
    $SUDO "$TMPDIR"/aws/install --update || $SUDO "$TMPDIR"/aws/install
    if command_exists aws; then
      log "Installed aws: $(aws --version 2>&1 | tr -d '\n')"
    else
      log "WARNING: aws still not available after installer. Please install awscli manually."
    fi
  fi
fi

# Determine tessdata dir
TESSDATA_DIR=""
if tesseract --print-tessdata-dir >/dev/null 2>&1; then
  TESSDATA_DIR="$(tesseract --print-tessdata-dir | tr -d '[:space:]')"
fi

# fallback dirs to probe
fallbacks=( \
  "/usr/share/tessdata" \
  "/usr/share/tesseract-ocr/4.00/tessdata" \
  "/usr/share/tesseract-ocr/tessdata" \
  "/usr/local/share/tessdata" \
)

for d in "${fallbacks[@]}"; do
  if [ -z "$TESSDATA_DIR" ] && [ -d "$d" ]; then
    TESSDATA_DIR="$d"
    break
  fi
done

if [ -z "$TESSDATA_DIR" ]; then
  TESSDATA_DIR="/usr/share/tessdata"
  log "Creating tessdata dir: $TESSDATA_DIR"
  $SUDO mkdir -p "$TESSDATA_DIR"
fi

log "Using tessdata directory: $TESSDATA_DIR"

# function: try download from a list of candidate URLs for a language
download_with_retries() {
  local lang="$1"
  shift
  local urls=("$@")
  local tmp="/tmp/${lang}.traineddata.$$"
  for url in "${urls[@]}"; do
    log "Trying $url"
    # curl retry. -f to fail on HTTP >=400
    if curl --fail -L --retry "$RETRY_COUNT" --retry-delay "$RETRY_DELAY" -sS "$url" -o "$tmp"; then
      # ensure file non-empty
      if [ -s "$tmp" ]; then
        $SUDO mv "$tmp" "${TESSDATA_DIR}/${lang}.traineddata"
        $SUDO chmod 0644 "${TESSDATA_DIR}/${lang}.traineddata"
        log "Installed ${lang}.traineddata -> ${TESSDATA_DIR}/${lang}.traineddata"
        return 0
      else
        log "WARN: downloaded file is empty for $url"
        rm -f "$tmp" || true
      fi
    else
      log "WARN: download failed for $url"
      rm -f "$tmp" || true
    fi
  done
  return 1
}

# Install requested languages
FAILED_LANGS=()
for lang in "${LANGS[@]}"; do
  lang="${lang// /}"   # trim
  if [ -z "$lang" ]; then
    continue
  fi

  target="${TESSDATA_DIR}/${lang}.traineddata"
  if [ -f "$target" ]; then
    log "SKIP: ${lang}.traineddata already present"
    continue
  fi

  # build candidate URLs in order:
  # 1) indic-ocr (only for indic languages)
  # 2) primary tessdata (best/standard/fast)
  # 3) other tessdata variants
  urls=()

  # check if language is in indic set
  want_indic=false
  for i in $INDIC_LANGS_SET; do
    if [ "$i" = "$lang" ]; then
      want_indic=true
      break
    fi
  done

  if [ "$want_indic" = true ]; then
    urls+=("${INDIC_OCR_BASE}/${lang}.traineddata")
  fi

  # primary tessdata chosen by INDIC_OCR_SIZE
  urls+=("${PRIMARY_TESSBASE}/${lang}.traineddata")

  # fallback variants
  if [ "$PRIMARY_TESSBASE" != "$TESSDATA_BEST" ]; then
    urls+=("${TESSDATA_BEST}/${lang}.traineddata")
  fi
  if [ "$PRIMARY_TESSBASE" != "$TESSDATA_STANDARD" ]; then
    urls+=("${TESSDATA_STANDARD}/${lang}.traineddata")
  fi
  if [ "$PRIMARY_TESSBASE" != "$TESSDATA_FAST" ]; then
    urls+=("${TESSDATA_FAST}/${lang}.traineddata")
  fi

  log "Attempting to install traineddata for '$lang'..."
  if download_with_retries "$lang" "${urls[@]}"; then
    log "SUCCESS: $lang installed"
  else
    log "ERROR: Failed to obtain traineddata for language '$lang' from any source"
    FAILED_LANGS+=("$lang")
  fi
done

# Set permissions on tessdata directory
$SUDO chmod -R a+rX "$TESSDATA_DIR" || true

# List installed languages
log "Verifying installed languages via 'tesseract --list-langs'..."
if tesseract --list-langs >/tmp/tess_langs.txt 2>/dev/null; then
  log "Available languages:"
  sed -n '1,200p' /tmp/tess_langs.txt || true
  rm -f /tmp/tess_langs.txt
else
  log "WARNING: 'tesseract --list-langs' failed or returned nothing"
fi

# Final summary and exit code
if [ "${#FAILED_LANGS[@]}" -gt 0 ]; then
  log "The following languages failed to install: ${FAILED_LANGS[*]}"
  log "You can re-run this script after fixing network access or adding more traineddata sources."
  exit 5
else
  log "Bootstrap complete. All requested traineddata installed or already present."
  exit 0
fi
