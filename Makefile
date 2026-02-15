# Makefile to package all Lambdas with scripts/package_lambda.sh
ARTIFACTS_DIR ?= artifacts

RETRIEVER_SRC ?= inference/retriever
GENERATE_SRC  ?= inference/generate
QUERY_SRC     ?= inference/query_router

RETRIEVER_ZIP := $(ARTIFACTS_DIR)/retriever.zip
GENERATE_ZIP  := $(ARTIFACTS_DIR)/generate.zip
QUERY_ZIP     := $(ARTIFACTS_DIR)/query_router.zip

.PHONY: all build-all build-retriever build-generate build-query clean

all: build-all

build-all: $(RETRIEVER_ZIP) $(GENERATE_ZIP) $(QUERY_ZIP)

$(RETRIEVER_ZIP):
	@echo "Building retriever lambda..."
	@bash infra/scripts/package_lambda.sh "$(RETRIEVER_SRC)" "$(ARTIFACTS_DIR)" "retriever.zip"

$(GENERATE_ZIP):
	@echo "Building generate lambda..."
	@bash infra/scripts/package_lambda.sh "$(GENERATE_SRC)" "$(ARTIFACTS_DIR)" "generate.zip"

$(QUERY_ZIP):
	@echo "Building query_router lambda..."
	@bash infra/scripts/package_lambda.sh "$(QUERY_SRC)" "$(ARTIFACTS_DIR)" "query_router.zip"

build-retriever: $(RETRIEVER_ZIP)
build-generate: $(GENERATE_ZIP)
build-query: $(QUERY_ZIP)


tree:
	tree -I 'repos|.venv|tmp'

bootstrap:
	bash infra/scripts/local_bootstrap.sh
	

.PHONY: run-scraper
run-scraper:
	cd indexing_pipeline && \
	PYTHONPATH="$$(pwd)" \
	python3 ELT/extract_load/web_scraper.py

create-s3:
	python3 infra/scripts/s3.py --create

delete-s3:
	python3 infra/scripts/s3.py --delete --force

PYTHON := python3
SCRIPT := infra/scripts/sync_s3_with_local_fs.py

.PHONY: upload-force download-force upload-merge download-merge

upload-force:
	$(PYTHON) $(SCRIPT) --upload

download-force:
	$(PYTHON) $(SCRIPT) --download

upload-merge:
	$(PYTHON) $(SCRIPT) --merge-upload

download-merge:
	$(PYTHON) $(SCRIPT) --merge-download


push:
	git add .
	git commit -m "new"
	git push origin main --force


clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" ! -path "./.git/*" -delete
	find . -type f -name "*.pulumi-logs" ! -path "./.git/*" -delete
	clear

SHELL := /bin/bash
.ONESHELL:

ls:
	set -e
	echo "bucket=$${S3_BUCKET}"
	aws s3 ls s3://$${S3_BUCKET}/data/chunked/


ELT:
	make run-scraper
	python3 indexing_pipeline/ELT/parse_chunk_store/router.py

push-indexing-image:
	bash indexing_pipeline/build_and_push_image.sh

pulumi-destroy:
	bash infra/pulumi_aws/run.sh delete

pulumi-up:
	bash infra/pulumi_aws/run.sh create

pi:
	git add .github/workflows/indexing.yaml indexing_pipeline
	git commit -m "indexing pipeline update"
	git push origin main
