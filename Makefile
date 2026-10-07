.PHONY: setup install check-sources collect-matches collect-players check-corpus mvp ingest clean-data embed store pipeline test lint

setup: install
	@test -e .env || cp .env.example .env

install:
	uv sync --locked --extra dev

check-sources:
	uv run --locked python scripts/check_sources.py

collect-matches:
	uv run --locked python scripts/collect_matches.py

collect-players:
	uv run --locked python scripts/collect_players.py

check-corpus:
	uv run --locked python scripts/check_corpus.py

mvp:
	uv run --locked python scripts/run_mvp.py

collect-commentary:
	uv run --locked python scripts/collect_commentary.py

load-mvp:
	uv run --locked python scripts/load_mvp.py

ingest:
	uv run --locked python scripts/run_ingest.py

clean-data:
	uv run --locked python scripts/run_clean.py

embed:
	uv run --locked python scripts/run_embed.py

store:
	uv run --locked python scripts/run_store.py

pipeline:
	uv run --locked python scripts/run_pipeline.py

test:
	uv run --locked --extra dev pytest tests/ -q

lint:
	uv run --locked --extra dev ruff check src config backend scripts tests --select E9,F
	uv run --locked --extra dev ruff check config/season.py config/sources.py config/settings.py scripts/check_sources.py tests/test_config tests/test_ingest/test_stats_api.py src/ingest/historical_matches.py src/ingest/source_download.py scripts/collect_matches.py tests/test_ingest/test_historical_matches.py tests/test_ingest/test_source_download.py src/ingest/historical_players.py scripts/collect_players.py tests/test_ingest/test_historical_players.py src/clean/corpus_quality.py scripts/check_corpus.py tests/test_clean/test_corpus_quality.py scripts/run_mvp.py src/utils/checkpoint.py tests/test_utils src/store/mvp_graph.py src/store/mvp_index.py scripts/load_mvp.py tests/test_store/test_mvp_stores.py src/ingest/commentary.py scripts/collect_commentary.py tests/test_ingest/test_commentary.py src/analysis tests/test_analysis backend/graph.py scripts/build_reference_questions.py scripts/accept_mvp.py scripts/accept_stores.py backend/readiness.py scripts/demo.py src/corpus tests/test_corpus src/ingest/commons_media.py scripts/collect_media.py scripts/extract_media.py scripts/prepare_text.py scripts/accept_corpus.py src/ingest/match_history.py scripts/collect_history.py src/store/evidence_index.py scripts/index_evidence.py tests/test_store/test_evidence_index.py src/store/evidence_graph.py src/store/evidence_queries.py scripts/load_evidence_graph.py tests/test_store/test_evidence_graph.py tests/test_store/test_evidence_queries.py tests/test_ingest/test_match_history.py tests/test_ingest/test_commons_media.py
	uv run --locked --extra dev ruff format --check config/season.py config/sources.py config/settings.py scripts/check_sources.py tests/test_config tests/test_ingest/test_stats_api.py src/ingest/historical_matches.py src/ingest/source_download.py scripts/collect_matches.py tests/test_ingest/test_historical_matches.py tests/test_ingest/test_source_download.py src/ingest/historical_players.py scripts/collect_players.py tests/test_ingest/test_historical_players.py src/clean/corpus_quality.py scripts/check_corpus.py tests/test_clean/test_corpus_quality.py scripts/run_mvp.py src/utils/checkpoint.py tests/test_utils src/store/mvp_graph.py src/store/mvp_index.py scripts/load_mvp.py tests/test_store/test_mvp_stores.py src/ingest/commentary.py scripts/collect_commentary.py tests/test_ingest/test_commentary.py src/analysis tests/test_analysis backend/graph.py scripts/build_reference_questions.py scripts/accept_mvp.py scripts/accept_stores.py backend/readiness.py scripts/demo.py src/corpus tests/test_corpus src/ingest/commons_media.py scripts/collect_media.py scripts/extract_media.py scripts/prepare_text.py scripts/accept_corpus.py src/ingest/match_history.py scripts/collect_history.py src/store/evidence_index.py scripts/index_evidence.py tests/test_store/test_evidence_index.py src/store/evidence_graph.py src/store/evidence_queries.py scripts/load_evidence_graph.py tests/test_store/test_evidence_graph.py tests/test_store/test_evidence_queries.py tests/test_ingest/test_match_history.py tests/test_ingest/test_commons_media.py

.PHONY: demo demo-status demo-stop demo-backup
demo:
	python3 scripts/demo.py

demo-status:
	python3 scripts/demo.py status

demo-stop:
	python3 scripts/demo.py stop

demo-backup:
	python3 scripts/demo.py backup

.PHONY: collect-media
collect-media:
	uv run --locked python scripts/collect_media.py

.PHONY: extract-media
extract-media:
	uv run --locked python scripts/extract_media.py

.PHONY: prepare-text
prepare-text:
	uv run --locked python scripts/prepare_text.py

.PHONY: accept-corpus
accept-corpus:
	uv run --locked python scripts/accept_corpus.py

.PHONY: collect-history
collect-history:
	uv run --locked python scripts/collect_history.py

.PHONY: index-evidence
index-evidence:
	uv run --locked python scripts/index_evidence.py

.PHONY: load-evidence-graph
load-evidence-graph:
	uv run --locked python scripts/load_evidence_graph.py
