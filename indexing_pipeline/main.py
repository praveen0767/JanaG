import os
import sys
import time
import logging
import subprocess

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "indexing-pipeline")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RUN_ID = os.getenv("RUN_ID", str(int(time.time())))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(PIPELINE_NAME)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STAGES = [
    ("extract_load", os.path.join(BASE_DIR, "ELT", "extract_load", "web_scraper.py")),
    ("parse_chunk_store", os.path.join(BASE_DIR, "ELT", "parse_chunk_store", "router.py")),
    ("embed_and_index", os.path.join(BASE_DIR, "embed_and_index.py")),
]


def run_stage(name: str, path: str):
    logger.info(f"stage_start {name} run_id={RUN_ID} path={path}")
    if not os.path.isfile(path):
        logger.error(f"pipeline_stage_failed stage={name} error=missing_file path={path} run_id={RUN_ID}")
        sys.exit(1)

    result = subprocess.run(
        [sys.executable, path],
        env=os.environ.copy(),
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    if result.returncode != 0:
        logger.error(f"pipeline_stage_failed stage={name} exit_code={result.returncode} run_id={RUN_ID}")
        sys.exit(result.returncode)

    logger.info(f"stage_complete {name} run_id={RUN_ID}")


def main():
    logger.info(f"pipeline_start name={PIPELINE_NAME} run_id={RUN_ID}")
    for name, path in STAGES:
        run_stage(name, path)
    logger.info(f"pipeline_complete name={PIPELINE_NAME} run_id={RUN_ID}")


if __name__ == "__main__":
    main()
