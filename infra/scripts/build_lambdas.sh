#!/usr/bin/env bash
# packages a Python lambda directory into a linux-compatible zip using the lambci build image
# Usage: ./scripts/package_lambda.sh <src_dir> [artifacts_dir] [zip_name]
# Example: ./scripts/package_lambda.sh inference/retriever ./artifacts retriever.zip
set -euo pipefail

SRC_DIR="${1:-}"
ARTIFACTS_DIR="${2:-./artifacts}"
ZIP_NAME="${3:-$(basename "${SRC_DIR%/}").zip}"

if [ -z "$SRC_DIR" ]; then
  echo "Usage: $0 <src_dir> [artifacts_dir] [zip_name]"
  exit 2
fi

# normalize to relative path if user supplied absolute
SRC_DIR="${SRC_DIR%/}"   # strip trailing slash if any

# ensure artifacts dir exists on host
mkdir -p "$ARTIFACTS_DIR"

echo "Packaging lambda from: $SRC_DIR"
echo "Artifacts dir: $ARTIFACTS_DIR"
echo "Output zip: $ZIP_NAME"

# Build inside the official lambci build image which matches Lambda environment
# The host working dir ($PWD) is mounted at /var/task inside container.
docker run --rm -v "$PWD":/var/task -w /var/task lambci/lambda:build-python3.11 bash -lc "\
set -euo pipefail; \
echo 'Preparing /tmp/build'; \
rm -rf /tmp/build; mkdir -p /tmp/build; \
# If requirements.txt exists in the source, install into /tmp/build
if [ -f \"/var/task/${SRC_DIR}/requirements.txt\" ]; then \
  echo 'Installing Python dependencies (no-cache)'; \
  python3 -m pip install --upgrade pip==23.2.1 setuptools wheel >/dev/null; \
  pip install --no-cache-dir -r \"/var/task/${SRC_DIR}/requirements.txt\" -t /tmp/build; \
fi; \
# Copy source code into /tmp/build (tar preserves symlinks and permissions)
echo 'Copying source files'; \
tar -C \"/var/task/${SRC_DIR}\" -cf - . | tar -C /tmp/build -xf -; \
# Remove common dev artifacts if present
rm -rf /tmp/build/.venv /tmp/build/venv /tmp/build/__pycache__ /tmp/build/build /tmp/build/dist; \
# Zip everything
cd /tmp/build; \
zip -r -q \"/var/task/${ARTIFACTS_DIR}/${ZIP_NAME}\" .; \
echo 'Wrote /var/task/${ARTIFACTS_DIR}/${ZIP_NAME}' \
"

# Ensure file exists and report size
if [ ! -f "${ARTIFACTS_DIR}/${ZIP_NAME}" ]; then
  echo "ERROR: packaging failed; zip not found: ${ARTIFACTS_DIR}/${ZIP_NAME}" >&2
  exit 3
fi

ls -lh "${ARTIFACTS_DIR}/${ZIP_NAME}"
echo "Done."
