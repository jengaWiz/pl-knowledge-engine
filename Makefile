.PHONY: setup install ingest clean-data embed store pipeline test lint

setup:
	python -m venv .venv
	.venv/bin/pip install -e ".[dev]"
	cp .env.example .env
	@echo "Edit .env with your API keys before running anything"

install:
	pip install -e ".[dev]"

ingest:
	python scripts/run_ingest.py

clean-data:
	python scripts/run_clean.py

embed:
	python scripts/run_embed.py

store:
	python scripts/run_store.py

pipeline:
	python scripts/run_pipeline.py

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/
	ruff format src/ tests/
