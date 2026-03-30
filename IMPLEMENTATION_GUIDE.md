# Premier League Knowledge Engine — Implementation Guide

> **Scope:** Aston Villa vs Liverpool FC, 2025-26 Premier League season
> **Modalities:** Text, Images, Audio, Video
> **Stack:** Python 3.11+, Gemini Embedding 2, ChromaDB, Neo4j, pandas

---

## Phase 0: Repository Setup

### 0.1 Initialize the Repository

```bash
mkdir pl-knowledge-engine
cd pl-knowledge-engine
git init
```

### 0.2 Create Directory Structure

```bash
# Config
mkdir -p config

# Source code — 4 pipeline stages + utils
mkdir -p src/ingest
mkdir -p src/clean
mkdir -p src/embed
mkdir -p src/store
mkdir -p src/utils

# Data directories (all gitignored — never committed)
mkdir -p data/raw/stats
mkdir -p data/raw/transcripts
mkdir -p data/raw/audio
mkdir -p data/raw/video
mkdir -p data/raw/images
mkdir -p data/cleaned/stats
mkdir -p data/cleaned/transcripts
mkdir -p data/cleaned/audio_segments
mkdir -p data/cleaned/metadata
mkdir -p data/embedded
mkdir -p data/checkpoints

# Notebooks for exploration
mkdir -p notebooks

# Tests mirror src structure
mkdir -p tests/test_ingest
mkdir -p tests/test_clean
mkdir -p tests/test_embed
mkdir -p tests/test_store

# Pipeline runner scripts
mkdir -p scripts

# Create all __init__.py files
touch src/__init__.py
touch src/ingest/__init__.py
touch src/clean/__init__.py
touch src/embed/__init__.py
touch src/store/__init__.py
touch src/utils/__init__.py
touch tests/__init__.py
touch tests/test_ingest/__init__.py
touch tests/test_clean/__init__.py
touch tests/test_embed/__init__.py
touch tests/test_store/__init__.py
```

### 0.3 Create pyproject.toml

```toml
[project]
name = "pl-knowledge-engine"
version = "0.1.0"
description = "Multimodal knowledge engine for Premier League analysis — Aston Villa vs Liverpool FC (2025-26)"
requires-python = ">=3.11"
dependencies = [
    # Core
    "python-dotenv>=1.0.0",
    "pydantic-settings>=2.0.0",
    "structlog>=24.0.0",

    # Data manipulation
    "pandas>=2.1.0",
    "requests>=2.31.0",
    "httpx>=0.27.0",
    "beautifulsoup4>=4.12.0",

    # YouTube & media
    "youtube-transcript-api>=1.0.0",
    "yt-dlp>=2024.0.0",
    "google-api-python-client>=2.100.0",

    # Audio processing
    "pydub>=0.25.1",

    # AI & embeddings
    "google-genai>=1.0.0",

    # Vector database
    "chromadb>=0.5.0",

    # Knowledge graph
    "neo4j>=5.14.0",

    # Testing
    "pytest>=7.4.0",
    "pytest-asyncio>=0.23.0",
]

[project.optional-dependencies]
dev = [
    "ipykernel>=6.0.0",
    "jupyter>=1.0.0",
    "ruff>=0.5.0",
    "mypy>=1.8.0",
]

[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

### 0.4 Create .env.example

```env
# Google AI Studio — for Gemini Embedding 2
# Get from: https://aistudio.google.com/apikey
GEMINI_API_KEY=your_gemini_api_key_here

# YouTube Data API v3 — for searching podcast channels and getting video IDs
# Get from: https://console.cloud.google.com/apis/credentials
# Enable "YouTube Data API v3" in your Google Cloud project first
YOUTUBE_API_KEY=your_youtube_api_key_here

# BallDontLie EPL API — for Premier League stats
# Get from: https://epl.balldontlie.io
BALLDONTLIE_API_KEY=your_balldontlie_api_key_here

# Neo4j — local Community Edition
# Default after installation; change password on first login
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password_here

# Anthropic API (optional) — for LLM-assisted transcript cleaning
# Get from: https://console.anthropic.com
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

### 0.5 Create .gitignore

```gitignore
# Data — never commit raw or processed data
data/

# Environment
.env
.venv/
venv/
env/

# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
dist/
build/

# ChromaDB local storage
.chroma/
chroma_data/

# Neo4j local data
neo4j/data/

# Jupyter
.ipynb_checkpoints/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
logs/
```

### 0.6 Create Makefile

```makefile
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
```

### 0.7 Initial Git Commit

```bash
git add .
git commit -m "feat: initialize project structure with dependencies and configuration"
```

---

## Phase 1: Configuration & Utilities

### 1.1 Create config/settings.py

This is the central configuration module. It loads environment variables from .env and validates them using pydantic-settings. Every other module imports settings from here — no module should read .env directly.

```python
"""
Central configuration loaded from environment variables.
All modules import from here — never read .env directly.
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    # API Keys
    gemini_api_key: str = Field(..., env="GEMINI_API_KEY")
    youtube_api_key: str = Field(..., env="YOUTUBE_API_KEY")
    balldontlie_api_key: str = Field(..., env="BALLDONTLIE_API_KEY")
    anthropic_api_key: str = Field(default="", env="ANTHROPIC_API_KEY")

    # Neo4j
    neo4j_uri: str = Field(default="bolt://localhost:7687", env="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", env="NEO4J_USER")
    neo4j_password: str = Field(..., env="NEO4J_PASSWORD")

    # Paths (relative to project root)
    data_dir: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    cleaned_dir: Path = Path("data/cleaned")
    embedded_dir: Path = Path("data/embedded")
    checkpoint_dir: Path = Path("data/checkpoints")

    # Embedding config
    gemini_model: str = "gemini-embedding-2-preview"
    embedding_dimensions: int = 3072
    embedding_batch_size: int = 5
    embedding_delay_seconds: float = 1.5

    # Chunking config
    chunk_size_words: int = 400
    chunk_overlap_words: int = 100
    audio_segment_seconds: int = 75
    audio_overlap_seconds: int = 10

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
```

### 1.2 Create config/teams.py

Centralized team metadata. Every module that needs team names, IDs, or filtering criteria imports from here.

```python
"""
Team metadata for the two focus teams.
All team-specific constants live here — player lists, API IDs, YouTube channels, etc.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TeamConfig:
    name: str
    short_name: str
    abbreviation: str
    balldontlie_id: int  # Look up from BallDontLie API on first run
    fbref_id: str
    stadium: str
    stadium_city: str


# These IDs need to be verified against the BallDontLie API on first run.
# Run: GET https://api.balldontlie.io/epl/v2/teams and find the correct IDs.
ASTON_VILLA = TeamConfig(
    name="Aston Villa",
    short_name="Villa",
    abbreviation="AVL",
    balldontlie_id=0,  # TODO: fill after first API call
    fbref_id="8602292d",
    stadium="Villa Park",
    stadium_city="Birmingham",
)

LIVERPOOL = TeamConfig(
    name="Liverpool",
    short_name="Liverpool",
    abbreviation="LFC",
    balldontlie_id=0,  # TODO: fill after first API call
    fbref_id="822bd0ba",
    stadium="Anfield",
    stadium_city="Liverpool",
)

FOCUS_TEAMS = [ASTON_VILLA, LIVERPOOL]
FOCUS_TEAM_NAMES = {t.name for t in FOCUS_TEAMS}
```

### 1.3 Create config/podcast_channels.py

YouTube channel IDs and metadata for target podcasts. These are the channels the ingestion pipeline will search for episodes.

```python
"""
YouTube channel metadata for target podcasts.
Each entry maps a channel to its YouTube channel ID and a description of its coverage.

To find a channel ID:
1. Go to the channel page on YouTube
2. View page source or use YouTube Data API: channels.list with forUsername parameter
3. Or use a tool like https://commentpicker.com/youtube-channel-id.php
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PodcastChannel:
    name: str
    youtube_channel_id: str  # The UC... ID from YouTube
    coverage: str  # What teams/topics this channel covers
    search_keywords: list[str]  # Keywords to filter relevant episodes


PODCAST_CHANNELS = [
    PodcastChannel(
        name="The Football Ramble",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="General Premier League analysis, covers all teams weekly",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The 2 Robbies - NBC Sports",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Match-by-match PL analysis from former professionals",
        search_keywords=["Premier League", "review", "preview"],
    ),
    PodcastChannel(
        name="Sky Sports Football Podcast",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Post-match analysis with Sky Sports pundits",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The Anfield Wrap",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Dedicated Liverpool FC podcast — deep tactical analysis",
        search_keywords=["Liverpool", "Anfield", "Premier League"],
    ),
    PodcastChannel(
        name="The Villa View",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Dedicated Aston Villa podcast",
        search_keywords=["Aston Villa", "Villa", "Premier League"],
    ),
    PodcastChannel(
        name="Official FPL Podcast",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Fantasy Premier League — player performance discussion",
        search_keywords=["FPL", "gameweek"],
    ),
]
```

### 1.4 Create src/utils/logger.py

Structured logging setup using structlog. All modules should use this logger, not print() or the default logging module.

```python
"""
Structured logging configuration.
Usage in any module:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("processing match", match_id=123, team="Liverpool")
"""
import structlog
import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Configure structlog with console output. Call once at startup."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a named logger instance."""
    return structlog.get_logger(name)
```

### 1.5 Create src/utils/retry.py

Exponential backoff decorator for API calls. Handles rate limits (HTTP 429) and transient failures.

```python
"""
Retry decorator with exponential backoff.
Use on any function that makes external API calls.

Usage:
    @retry(max_attempts=5, base_delay=1.0)
    def call_api():
        ...
"""
import time
import functools
from src.utils.logger import get_logger

logger = get_logger(__name__)


def retry(max_attempts: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts.
        base_delay: Initial delay in seconds (doubles each retry).
        max_delay: Maximum delay cap in seconds.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(
                            "max retries exceeded",
                            function=func.__name__,
                            attempts=max_attempts,
                            error=str(e),
                        )
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    logger.warning(
                        "retrying after error",
                        function=func.__name__,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        delay_seconds=delay,
                        error=str(e),
                    )
                    time.sleep(delay)
            raise last_exception  # Should never reach here
        return wrapper
    return decorator
```

### 1.6 Create src/utils/checkpoint.py

Checkpoint system for long-running pipeline stages. Saves progress to a JSON file so that if the script crashes, it can resume from where it left off instead of reprocessing everything.

```python
"""
Checkpoint system for resumable pipeline stages.

Usage:
    cp = Checkpoint("embed_text_chunks")
    if cp.is_completed("chunk_042"):
        continue  # Skip already-processed items
    # ... process chunk_042 ...
    cp.mark_completed("chunk_042")
"""
import json
from pathlib import Path
from src.utils.logger import get_logger
from config.settings import settings

logger = get_logger(__name__)


class Checkpoint:
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.filepath = settings.checkpoint_dir / f"{stage_name}.json"
        self.completed: set[str] = set()
        self._load()

    def _load(self) -> None:
        """Load existing checkpoint from disk."""
        if self.filepath.exists():
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self.completed = set(data.get("completed", []))
            logger.info(
                "checkpoint loaded",
                stage=self.stage_name,
                completed_count=len(self.completed),
            )

    def _save(self) -> None:
        """Persist checkpoint to disk."""
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump({"completed": sorted(self.completed)}, f, indent=2)

    def is_completed(self, item_id: str) -> bool:
        """Check if an item has already been processed."""
        return item_id in self.completed

    def mark_completed(self, item_id: str) -> None:
        """Mark an item as processed and save to disk."""
        self.completed.add(item_id)
        self._save()

    def reset(self) -> None:
        """Clear all checkpoint data."""
        self.completed.clear()
        if self.filepath.exists():
            self.filepath.unlink()
        logger.info("checkpoint reset", stage=self.stage_name)
```

### 1.7 Git Commit

```bash
git add .
git commit -m "feat: add configuration, logging, retry, and checkpoint utilities"
```

---

## Phase 2: Stats Data Ingestion

### 2.1 Create src/ingest/stats_api.py

BallDontLie EPL API client. This module handles all communication with the stats API: fetching teams, matches, standings, rosters, and lineups. It uses the retry decorator for resilience and returns typed dictionaries.

**Key implementation details:**
- Base URL: `https://api.balldontlie.io/epl/v2`
- Auth: API key passed as `Authorization` header
- Pagination: cursor-based — check `meta.next_cursor` in response, pass as `?cursor=` param
- Rate limits: respect the API's rate limit headers; use the retry decorator
- Filtering: after fetching, filter to only matches involving Aston Villa or Liverpool

**Endpoints to implement:**
1. `GET /teams` → get all teams, find IDs for Villa and Liverpool
2. `GET /standings?season=2025` → current standings
3. `GET /matches?season=2025` → all matches (paginate through all), filter to focus teams
4. `GET /match_lineups?match_id={id}` → lineups for each focus-team match
5. `GET /players?team_id={id}` → full roster for each focus team

**Output format:** Save each endpoint's response as JSON files in `data/raw/stats/`:
- `data/raw/stats/teams.json`
- `data/raw/stats/standings.json`
- `data/raw/stats/matches_villa.json`
- `data/raw/stats/matches_liverpool.json`
- `data/raw/stats/roster_villa.json`
- `data/raw/stats/roster_liverpool.json`
- `data/raw/stats/lineups/{match_id}.json` (one file per match)

**Important:** The BallDontLie API uses `season=2025` to refer to the 2025-26 season (the year the season starts).

### 2.2 Create src/ingest/fpl_data.py

Download and load pre-cleaned CSVs from the FPL-Core-Insights GitHub repository. These provide per-gameweek player performance data that supplements the BallDontLie match-level data.

**Key implementation details:**
- Repository: `https://github.com/olbauday/FPL-Core-Insights`
- Target directory in repo: `data/2025-2026/`
- Key files to download:
  - `players.csv` — all player metadata (name, team, position, price)
  - `gameweeks.csv` — gameweek dates and metadata
  - Per-gameweek directories containing `playerstats_gw.csv` — discrete per-gameweek stats (goals scored in that GW, not cumulative)
- Download raw CSV files to `data/raw/stats/fpl/`
- Filter to only players belonging to Aston Villa or Liverpool

**Note:** These CSVs use FPL-specific player IDs. You'll need to create a mapping between FPL player names and BallDontLie player names for cross-referencing. Use fuzzy string matching if exact names don't match (e.g., "Salah" vs "Mohamed Salah").

### 2.3 Create src/clean/stats_cleaner.py

Pandas pipeline that takes raw JSON/CSV from the ingestion stage and produces cleaned, normalized data files.

**Cleaning operations:**
1. Load raw JSON files from `data/raw/stats/`
2. Normalize all column names to snake_case
3. Parse date strings into Python datetime objects
4. Ensure numeric fields (goals, assists, minutes, etc.) are integers, not strings
5. Fill missing numeric values with 0 where appropriate
6. Remove duplicate records (use match_date + home_team + away_team as composite key)
7. Add derived fields:
   - `result`: "W", "L", or "D" from the perspective of each focus team
   - `points`: 3 for win, 1 for draw, 0 for loss
   - `cumulative_points`: running total through the season
   - `form`: last 5 match results as a string (e.g., "WWDLW")
8. Merge BallDontLie and FPL data where possible (match on player name + gameweek)

**Output:** Save cleaned dataframes as both CSV and JSON in `data/cleaned/stats/`:
- `data/cleaned/stats/matches.csv` — all matches for both teams
- `data/cleaned/stats/players_villa.csv` — per-gameweek player stats
- `data/cleaned/stats/players_liverpool.csv`
- `data/cleaned/stats/standings_history.csv` — week-by-week standings progression

### 2.4 Write Tests

Create `tests/test_ingest/test_stats_api.py`:
- Test that the API client correctly handles pagination (mock a multi-page response)
- Test that team filtering only returns Villa and Liverpool matches
- Test error handling for rate limit (429) responses

Create `tests/test_clean/test_stats_cleaner.py`:
- Test column name normalization
- Test duplicate removal
- Test derived field calculation (result, points, form)
- Provide a small sample dataset as a pytest fixture

### 2.5 Git Commit

```bash
git add .
git commit -m "feat: implement stats ingestion from BallDontLie API and FPL data"
```

---

## Phase 3: Knowledge Graph (Neo4j)

### 3.1 Neo4j Setup

**Install Neo4j Community Edition:**
- Download from https://neo4j.com/download/ (Community Edition is free)
- Or use Docker: `docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/your_password neo4j:community`
- Access the browser UI at http://localhost:7474
- Update your .env with the password you set

### 3.2 Create scripts/seed_graph.py

Script that initializes the Neo4j database with constraints and indexes. Run this once before loading any data.

**Constraints to create (Cypher):**
```cypher
CREATE CONSTRAINT team_name IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.player_id IS UNIQUE;
CREATE CONSTRAINT match_id IF NOT EXISTS FOR (m:Match) REQUIRE m.match_id IS UNIQUE;
CREATE CONSTRAINT gameweek_number IF NOT EXISTS FOR (gw:Gameweek) REQUIRE gw.number IS UNIQUE;
CREATE CONSTRAINT manager_name IF NOT EXISTS FOR (mg:Manager) REQUIRE mg.name IS UNIQUE;
CREATE CONSTRAINT stadium_name IF NOT EXISTS FOR (s:Stadium) REQUIRE s.name IS UNIQUE;
CREATE CONSTRAINT podcast_id IF NOT EXISTS FOR (ep:PodcastEpisode) REQUIRE ep.youtube_id IS UNIQUE;
CREATE CONSTRAINT season_year IF NOT EXISTS FOR (s:Season) REQUIRE s.year IS UNIQUE;
```

**Indexes for common queries:**
```cypher
CREATE INDEX player_name IF NOT EXISTS FOR (p:Player) ON (p.name);
CREATE INDEX match_date IF NOT EXISTS FOR (m:Match) ON (m.date);
CREATE INDEX podcast_date IF NOT EXISTS FOR (ep:PodcastEpisode) ON (ep.date);
```

### 3.3 Create src/store/neo4j_store.py

Neo4j data loading module. Uses the `neo4j` Python driver to create nodes and relationships from cleaned stats data.

**Key implementation details:**
- Use the `neo4j` Python driver: `from neo4j import GraphDatabase`
- Create a `Neo4jStore` class that manages the driver connection
- Implement `close()` method and use as context manager for proper cleanup
- Use `MERGE` instead of `CREATE` for all operations (MERGE creates if not exists, matches if it does — prevents duplicates)
- Use parameterized queries (never string-interpolate values into Cypher — it's a security risk and breaks on special characters)

**Methods to implement:**

1. `create_season()` — Create the Season node for 2025-26
2. `create_teams(teams_data)` — Create Team nodes for Villa and Liverpool with properties (name, short_name, abbreviation, league_position, points, etc.)
3. `create_stadiums()` — Create Stadium nodes for Villa Park and Anfield
4. `create_managers(managers_data)` — Create Manager nodes and MANAGES relationships
5. `create_players(roster_data)` — Create Player nodes and PLAYS_FOR relationships for each team
6. `create_gameweeks(matches_data)` — Create Gameweek nodes (1-38) from match dates
7. `create_matches(matches_data)` — Create Match nodes with:
   - HOME_TEAM and AWAY_TEAM relationships to Team nodes (with properties: score, possession, shots, etc.)
   - PART_OF relationship to Gameweek node
   - PLAYED_AT relationship to Stadium node
8. `create_player_appearances(lineups_data)` — Create PLAYED_IN relationships between Player and Match nodes (with properties: starter, minutes, goals, assists, yellow_cards, red_cards)
9. `create_podcast_episode(episode_data)` — Create PodcastEpisode nodes and DISCUSSES relationships (used later in Phase 4)

**Example Cypher for match creation:**
```cypher
MERGE (m:Match {match_id: $match_id})
SET m.date = date($date),
    m.gameweek = $gameweek,
    m.home_score = $home_score,
    m.away_score = $away_score,
    m.venue = $venue,
    m.attendance = $attendance

WITH m
MATCH (ht:Team {name: $home_team_name})
MERGE (ht)-[r1:HOME_TEAM]->(m)
SET r1.score = $home_score

WITH m
MATCH (at:Team {name: $away_team_name})
MERGE (at)-[r2:AWAY_TEAM]->(m)
SET r2.score = $away_score

WITH m
MATCH (gw:Gameweek {number: $gameweek})
MERGE (m)-[:PART_OF]->(gw)
```

### 3.4 Create scripts/load_graph.py

Script that orchestrates loading all cleaned stats data into Neo4j. Call the Neo4jStore methods in the correct order (season → teams → stadiums → managers → players → gameweeks → matches → player appearances).

**Order matters:** Teams and Players must exist before Matches can reference them.

### 3.5 Create Validation Queries

After loading, run these Cypher queries to verify the graph is correct:

```cypher
-- Count nodes by type
MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count ORDER BY count DESC;

-- Verify both teams exist and have players
MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
WHERE t.name IN ['Aston Villa', 'Liverpool']
RETURN t.name, count(p) AS player_count;

-- Verify matches have correct structure (exactly 2 team relationships)
MATCH (t:Team)-[r:HOME_TEAM|AWAY_TEAM]->(m:Match)
WITH m, count(t) AS team_count
WHERE team_count <> 2
RETURN m.match_id, team_count;
-- Should return 0 rows

-- Liverpool's results this season
MATCH (l:Team {name: 'Liverpool'})-[r:HOME_TEAM|AWAY_TEAM]->(m:Match)
RETURN m.date, m.home_score, m.away_score, m.venue
ORDER BY m.date;

-- Top scorers for each team
MATCH (p:Player)-[r:PLAYED_IN]->(m:Match)<-[:HOME_TEAM|AWAY_TEAM]-(t:Team)
WHERE t.name IN ['Aston Villa', 'Liverpool'] AND r.goals > 0
RETURN p.name, t.name AS team, SUM(r.goals) AS total_goals
ORDER BY total_goals DESC
LIMIT 10;

-- Head-to-head record
MATCH (v:Team {name: 'Aston Villa'})-[:HOME_TEAM|AWAY_TEAM]->(m:Match)<-[:HOME_TEAM|AWAY_TEAM]-(l:Team {name: 'Liverpool'})
RETURN m.date, m.venue, m.home_score, m.away_score;
```

### 3.6 Git Commit

```bash
git add .
git commit -m "feat: implement Neo4j knowledge graph with full match and player data"
```

---

## Phase 4: Podcast Ingestion — Transcripts + Audio

### 4.1 Create src/ingest/youtube_search.py

Uses the YouTube Data API v3 to search for relevant podcast episodes from the target channels.

**Key implementation details:**
- Use `googleapiclient.discovery.build("youtube", "v3", developerKey=key)`
- Method: `youtube.search().list(channelId=..., q=..., type="video", publishedAfter=..., maxResults=50)`
- Search each channel in `config/podcast_channels.py` for videos mentioning "Aston Villa" OR "Liverpool" OR "Premier League"
- Filter by date: only videos published after August 1, 2025 (season start)
- For each result, also call `youtube.videos().list(id=..., part="contentDetails,snippet")` to get duration and full title
- Filter out videos shorter than 10 minutes (likely not full podcast episodes) and longer than 3 hours (likely live streams)

**Output:** Save to `data/raw/transcripts/podcast_episodes.json` — a list of objects:
```json
[
  {
    "youtube_id": "abc123",
    "title": "Liverpool CRUMBLE vs Villa | Premier League Review GW22",
    "channel": "The Football Ramble",
    "published_at": "2026-01-21T14:00:00Z",
    "duration_seconds": 2640,
    "description": "..."
  }
]
```

### 4.2 Create src/ingest/transcripts.py

Extracts transcripts from YouTube videos using the youtube-transcript-api library.

**Key implementation details:**
- Library: `from youtube_transcript_api import YouTubeTranscriptApi`
- Create an instance: `ytt_api = YouTubeTranscriptApi()`
- Call: `ytt_api.fetch(video_id)` — returns a list of `{"text": "...", "start": 0.0, "duration": 3.5}` objects
- Try English first; fall back to auto-generated if manual captions aren't available
- Some videos may not have captions at all — catch `TranscriptsDisabled` and `NoTranscriptFound` exceptions and log them, don't crash
- Concatenate all segments into a single raw transcript string per episode
- Also preserve the timestamped segments for potential alignment with audio later

**Output:** For each episode, save two files:
- `data/raw/transcripts/{youtube_id}_raw.txt` — full concatenated text
- `data/raw/transcripts/{youtube_id}_segments.json` — timestamped segments

### 4.3 Create src/ingest/audio_download.py

Downloads audio tracks from YouTube podcast episodes using yt-dlp.

**Key implementation details:**
- Use yt-dlp as a Python library: `import yt_dlp`
- Configure to extract audio only: `{'format': 'bestaudio/best', 'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '128'}]}`
- Set output template: `{'outtmpl': 'data/raw/audio/%(id)s.%(ext)s'}`
- Download audio for each episode in the podcast_episodes.json list
- Use the checkpoint system to track which episodes have already been downloaded (audio downloads are slow)
- Respect rate limits — add a 5 second delay between downloads

**Output:** MP3 files in `data/raw/audio/{youtube_id}.mp3`

**Storage note:** Budget ~50MB per hour of audio at 128kbps. 30 podcast episodes averaging 45 minutes = ~1.1GB. Make sure there's enough disk space.

### 4.4 Create src/clean/transcript_cleaner.py

Cleans raw auto-generated YouTube transcripts using an LLM.

**Key implementation details:**
- Raw YouTube captions look like this: `"so yeah salah was was really poor yesterday i mean he had like three chances and he just couldnt finish them and you know van dijk was was struggling at the back too"`
- Send chunks of ~2000 words to Claude (or GPT) with this prompt structure:

```
You are cleaning an auto-generated YouTube transcript from a Premier League football podcast.

Fix the following issues:
1. Add proper punctuation and capitalization
2. Fix player/manager name spellings (reference list: Mohamed Salah, Virgil van Dijk, Bukayo Saka, Ollie Watkins, Unai Emery, Arne Slot, etc.)
3. Fix team name spellings
4. Remove filler words and false starts (um, uh, you know, like, etc.) only when they don't affect meaning
5. Add paragraph breaks at natural topic transitions
6. Do NOT change the meaning or add information that isn't there

Return ONLY the cleaned text, no commentary.

Raw transcript:
{raw_text}
```

- Process in chunks to stay within context window limits
- Save both the raw and cleaned versions for comparison
- If no Anthropic API key is configured, fall back to basic regex cleaning (capitalize sentences, fix common misspellings from a dictionary)

**Output:** `data/cleaned/transcripts/{youtube_id}_cleaned.txt`

### 4.5 Create src/clean/chunker.py

Splits cleaned text into chunks suitable for embedding.

**Key implementation details:**
- Target chunk size: 300-500 words (configurable in settings.py)
- Overlap: 100 words between consecutive chunks
- Split at sentence boundaries (use `. ` as delimiter, not mid-word)
- Each chunk gets a metadata dict attached (see metadata schema below)
- If a chunk is shorter than 50 words (e.g., the tail end), merge it with the previous chunk

**Output:** `data/cleaned/transcripts/{youtube_id}_chunks.json` — list of:
```json
[
  {
    "chunk_id": "podcast_{youtube_id}_chunk_001",
    "text": "The cleaned chunk text...",
    "source_type": "podcast_transcript",
    "source_name": "The Football Ramble - Episode Title",
    "source_id": "youtube_id",
    "date": "2026-01-21",
    "modality": "text",
    "chunk_index": 0,
    "total_chunks": 18,
    "word_count": 412,
    "teams_mentioned": ["Liverpool", "Aston Villa"],
    "players_mentioned": ["Salah", "Watkins"]
  }
]
```

**Team/player detection:** Use simple keyword matching against known player and team name lists. Don't overcomplicate this — exact string matching (case-insensitive) on a predefined list works well enough.

### 4.6 Create src/clean/audio_segmenter.py

Splits full podcast audio files into segments suitable for Gemini Embedding 2.

**Key implementation details:**
- Use `pydub` library: `from pydub import AudioSegment`
- Load MP3: `audio = AudioSegment.from_mp3("path/to/file.mp3")`
- Segment length: 60-90 seconds (configurable, use 75s default)
- Overlap: 10 seconds between segments
- Export segments as MP3: `segment.export("path/to/segment.mp3", format="mp3")`
- Create metadata for each segment linking it to the source episode and timestamp range

**Output:**
- Audio files: `data/cleaned/audio_segments/{youtube_id}_seg_{NNN}.mp3`
- Metadata: `data/cleaned/audio_segments/{youtube_id}_segments_meta.json`

**ffmpeg dependency:** pydub requires ffmpeg installed on the system. Install with:
- macOS: `brew install ffmpeg`
- Ubuntu: `sudo apt install ffmpeg`
- Windows: download from ffmpeg.org and add to PATH

### 4.7 Create src/clean/metadata_tagger.py

Utility module that attaches standardized metadata to any chunk (text, audio, image, video). Used by other cleaning modules. This ensures every chunk across all modalities has a consistent metadata structure.

**Standardized metadata schema:**
```python
@dataclass
class ChunkMetadata:
    chunk_id: str          # Unique identifier: "{type}_{source_id}_{index}"
    source_type: str       # "match_stats", "player_stats", "podcast_transcript", "podcast_audio", "image", "video_highlight"
    source_name: str       # Human-readable source name
    source_id: str         # Original source identifier (match_id, youtube_id, etc.)
    date: str              # ISO date string (YYYY-MM-DD)
    modality: str          # "text", "image", "audio", "video"
    teams: list[str]       # Teams mentioned/involved
    players: list[str]     # Players mentioned/involved
    gameweek: int | None   # Gameweek number if applicable
    chunk_index: int       # Position within source (0-indexed)
    total_chunks: int      # Total chunks from this source
```

### 4.8 Git Commit

```bash
git add .
git commit -m "feat: implement podcast transcript extraction, audio download, cleaning, and chunking"
```

---

## Phase 5: Image & Video Ingestion

### 5.1 Create src/ingest/images.py

Downloads player headshots and team badges.

**Key implementation details:**
- Player headshots: The Premier League website serves headshots at predictable URLs. Alternatively, use the player photo URLs returned by the BallDontLie API or FPL API if available.
- Team badges: Download from Wikipedia/Wikimedia Commons (check license — most club logos are trademarked but widely used in editorial/educational context)
- Use `requests.get(url)` to download each image
- Save as PNG/JPEG to `data/raw/images/players/{player_name}.png` and `data/raw/images/badges/{team_name}.png`
- Create a metadata JSON mapping each image to its player/team

**Output:**
- Image files in `data/raw/images/`
- Metadata: `data/raw/images/images_metadata.json`

### 5.2 Create src/ingest/video_download.py

Downloads match highlight clips from YouTube.

**Key implementation details:**
- Search the Premier League's official YouTube channel for highlight videos
- Use YouTube Data API: search for "{home_team} vs {away_team} highlights" within the channel
- Download using yt-dlp with video format: `{'format': 'best[height<=720]'}` (720p is sufficient for embedding; saves storage)
- Most PL highlight clips are 2-3 minutes — within Gemini's 120-second limit. If longer, trim to 120 seconds using ffmpeg
- Use checkpoint system for resumability

**Output:**
- Video files: `data/raw/video/{match_id}_highlights.mp4`
- Metadata: `data/raw/video/video_metadata.json`

### 5.3 Generate Text Summaries from Stats

Before embedding stats data, convert structured match/player data into natural language summaries. Embeddings work much better on prose than on raw numbers.

**Create src/clean/stats_summarizer.py:**

For each match, generate a text summary like:
```
"Premier League 2025-26, Gameweek 15: Liverpool 1-2 Aston Villa at Anfield.
Aston Villa secured a crucial away victory. Ollie Watkins scored twice (23', 67')
while Mohamed Salah pulled one back for Liverpool (78'). Villa dominated possession
with 56% and had 14 shots to Liverpool's 11. This result moves Villa to 4th in the
table with 34 points, while Liverpool drop to 6th."
```

For each player gameweek, generate:
```
"Mohamed Salah (Liverpool) - Gameweek 15 vs Aston Villa: Scored 1 goal (78'),
0 assists, 4 shots (2 on target), 87 minutes played. Rating: 6.8/10."
```

These text summaries become the chunks that get embedded in the vector DB for stats data.

**Output:** `data/cleaned/stats/match_summaries.json` and `data/cleaned/stats/player_summaries.json`

### 5.4 Git Commit

```bash
git add .
git commit -m "feat: implement image and video ingestion, stats text summarization"
```

---

## Phase 6: Embedding Pipeline (Gemini Embedding 2)

### 6.1 Create src/embed/gemini_client.py

Wrapper around the Gemini Embedding 2 API.

**Key implementation details:**
- Library: `from google import genai`
- Initialize: `client = genai.Client(api_key=settings.gemini_api_key)`
- For text: `client.models.embed_content(model="gemini-embedding-2-preview", contents="text here")`
- For images: load image as bytes, pass as `types.Part` with correct MIME type
- For audio: load audio file as bytes, pass with MIME type `audio/mpeg` (for MP3) or `audio/wav`
- For video: load video file as bytes, pass with MIME type `video/mp4`
- The response contains `embedding.values` — a list of 3,072 floats
- Wrap all API calls with the `@retry` decorator from src/utils/retry.py

**Methods:**
```python
def embed_text(self, text: str) -> list[float]
def embed_image(self, image_path: Path) -> list[float]
def embed_audio(self, audio_path: Path) -> list[float]
def embed_video(self, video_path: Path) -> list[float]
```

**Important:** Check the current Gemini API documentation for the exact SDK method names and parameters — the API may have been updated since this plan was written. The `google-genai` package is the newer SDK (not `google-generativeai` which is the older one).

### 6.2 Create src/embed/batch_embedder.py

Orchestrates embedding in batches with rate limit management and checkpointing.

**Key implementation details:**
- Process items in batches of 5 (configurable in settings)
- After each batch, sleep for 1.5 seconds (configurable)
- Use the Checkpoint class to track which items have been embedded
- After each batch, save the batch's vectors and metadata to `data/embedded/`
- If the script is interrupted, it resumes from the last checkpoint on next run
- Log progress: "Embedded 45/312 text chunks (14.4%)"

### 6.3 Create src/embed/embedder.py

Top-level orchestrator that calls batch_embedder for each modality in sequence.

**Embedding order:**
1. Text chunks (match summaries, player summaries, podcast transcript chunks)
2. Images (player photos, team badges)
3. Audio segments (podcast audio clips)
4. Video highlights (match highlight clips)

**Output:** For each item, save to `data/embedded/{modality}/`:
```json
{
  "chunk_id": "podcast_abc123_chunk_003",
  "vector": [0.023, -0.119, ...],
  "metadata": { ... }
}
```

Save vectors as JSON files (one per batch) or as a single JSONL file per modality. ChromaDB will ingest these in the next phase.

### 6.4 Git Commit

```bash
git add .
git commit -m "feat: implement Gemini Embedding 2 pipeline with batching and checkpointing"
```

---

## Phase 7: Vector Database (ChromaDB)

### 7.1 Create src/store/chroma_store.py

ChromaDB storage module. Creates collections and inserts embedded vectors.

**Key implementation details:**
- Initialize: `import chromadb; client = chromadb.PersistentClient(path="./chroma_data")`
- PersistentClient saves data to disk so it survives script restarts

**Collections to create:**
```python
text_chunks = client.get_or_create_collection("text_chunks", metadata={"hnsw:space": "cosine"})
images = client.get_or_create_collection("images", metadata={"hnsw:space": "cosine"})
audio_segments = client.get_or_create_collection("audio_segments", metadata={"hnsw:space": "cosine"})
video_highlights = client.get_or_create_collection("video_highlights", metadata={"hnsw:space": "cosine"})
unified = client.get_or_create_collection("unified", metadata={"hnsw:space": "cosine"})
```

**Note on "cosine" space:** This tells ChromaDB to use cosine similarity for comparing vectors, which is the standard distance metric for embedding models.

**Insertion method:**
```python
collection.add(
    ids=["chunk_001", "chunk_002"],
    embeddings=[[0.1, 0.2, ...], [0.3, 0.4, ...]],
    metadatas=[{"source_type": "podcast", "team": "Liverpool"}, ...],
    documents=["original text for text chunks", ...]  # Only for text collections
)
```

**Important:** ChromaDB's `add()` method is idempotent by ID — adding the same ID twice updates rather than duplicates. This makes re-runs safe.

**For the unified collection:** Insert ALL vectors from all modalities into this single collection. This enables cross-modal search (e.g., text query returning video results).

**Query methods to implement:**
```python
def search_text(self, query_text: str, n_results: int = 10, filters: dict = None) -> list
def search_by_vector(self, vector: list[float], collection_name: str, n_results: int = 10) -> list
def search_unified(self, query_vector: list[float], n_results: int = 10, filters: dict = None) -> list
```

**Filter syntax for ChromaDB:**
```python
# Filter by team
results = collection.query(
    query_embeddings=[query_vector],
    n_results=10,
    where={"teams": {"$contains": "Liverpool"}}
)

# Filter by date range
results = collection.query(
    query_embeddings=[query_vector],
    n_results=10,
    where={"$and": [
        {"date": {"$gte": "2026-01-01"}},
        {"date": {"$lte": "2026-01-31"}}
    ]}
)
```

**Note on metadata with ChromaDB:** ChromaDB metadata values must be strings, ints, floats, or bools. Lists (like `teams: ["Liverpool", "Villa"]`) are NOT supported as metadata values. Instead, either: (a) store as a comma-separated string (`teams: "Liverpool,Aston Villa"`) and use `$contains`, or (b) create separate boolean fields (`team_liverpool: true, team_villa: false`).

### 7.2 Git Commit

```bash
git add .
git commit -m "feat: implement ChromaDB vector storage with cross-modal search"
```

---

## Phase 8: Pipeline Orchestration & Integration

### 8.1 Create scripts/run_pipeline.py

End-to-end pipeline runner. Executes all stages in sequence with logging and error handling.

```python
"""
End-to-end pipeline: Ingest → Clean → Embed → Store

Usage:
    python scripts/run_pipeline.py                  # Run everything
    python scripts/run_pipeline.py --stage ingest   # Run only ingestion
    python scripts/run_pipeline.py --stage clean    # Run only cleaning
    python scripts/run_pipeline.py --stage embed    # Run only embedding
    python scripts/run_pipeline.py --stage store    # Run only storage
"""
```

Use `argparse` for command-line arguments. Each stage should be independently runnable.

### 8.2 Create Individual Stage Runners

- `scripts/run_ingest.py` — runs all ingestion modules
- `scripts/run_clean.py` — runs all cleaning modules
- `scripts/run_embed.py` — runs the embedding pipeline
- `scripts/run_store.py` — loads data into ChromaDB and Neo4j
- `scripts/seed_graph.py` — initializes Neo4j schema (run once)

### 8.3 Create notebooks/01_data_exploration.ipynb

Jupyter notebook that demonstrates the system's capabilities:

1. **Stats analysis:** Load cleaned match data, compare Villa vs Liverpool stats (goals, xG, possession, form)
2. **Semantic search demo:** Query ChromaDB with natural language questions, show results
3. **Cross-modal search demo:** Use a text query to find related video highlights or podcast audio
4. **Knowledge graph queries:** Run Cypher queries against Neo4j, display results
5. **Comparison analysis:** Show how the two teams differ across multiple dimensions

This notebook is a key portfolio artifact — it should be polished and well-commented.

### 8.4 Write README.md

The README is the first thing recruiters see. Structure it as:

1. **Project title and one-line description**
2. **Architecture diagram** (use Mermaid syntax — GitHub renders it natively)
3. **Key features** (multimodal embeddings, knowledge graph, semantic search)
4. **Tech stack** (with version numbers)
5. **Quick start** (setup in <5 commands)
6. **Project structure** (directory tree)
7. **Example queries** (with output screenshots)
8. **Data sources** (with links)
9. **What I learned** (brief reflection on technical challenges)

### 8.5 Git Commit

```bash
git add .
git commit -m "feat: add pipeline orchestration, demo notebook, and documentation"
```

---

## Appendix A: Metadata Schema Reference

Every chunk in the system (regardless of modality) must conform to this schema:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| chunk_id | string | Yes | Unique ID: `{source_type}_{source_id}_{chunk_index}` |
| source_type | string | Yes | One of: `match_stats`, `player_stats`, `podcast_transcript`, `podcast_audio`, `player_image`, `team_badge`, `video_highlight` |
| source_name | string | Yes | Human-readable name (e.g., "The Anfield Wrap - Ep 142") |
| source_id | string | Yes | Original identifier (match_id, youtube_id, player_name) |
| date | string | Yes | ISO date: YYYY-MM-DD |
| modality | string | Yes | One of: `text`, `image`, `audio`, `video` |
| teams | string | Yes | Comma-separated team names (ChromaDB limitation) |
| players | string | No | Comma-separated player names |
| gameweek | int | No | Gameweek number (1-38) if applicable |
| chunk_index | int | Yes | 0-indexed position within source |
| total_chunks | int | Yes | Total chunks from this source |
| word_count | int | No | For text chunks only |
| duration_seconds | int | No | For audio/video chunks only |

## Appendix B: Git Workflow

Use conventional commits throughout:
- `feat:` — new feature or module
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — code restructuring without behavior change
- `test:` — adding or updating tests
- `chore:` — dependency updates, config changes

Use feature branches for each phase:
```bash
git checkout -b feature/stats-ingestion    # Phase 2
git checkout -b feature/knowledge-graph    # Phase 3
git checkout -b feature/podcast-ingestion  # Phase 4
# etc.
```

Merge to main via squash merges to keep main history clean.

## Appendix C: Environment Setup Commands

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/pl-knowledge-engine.git
cd pl-knowledge-engine

# 2. Create virtual environment and install
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# 3. Copy environment template and fill in API keys
cp .env.example .env
# Edit .env with your actual API keys

# 4. Install system dependencies
# macOS:
brew install ffmpeg
# Ubuntu:
sudo apt install ffmpeg

# 5. Start Neo4j (Docker method)
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/your_password \
  neo4j:community

# 6. Initialize Neo4j schema
python scripts/seed_graph.py

# 7. Run the full pipeline
python scripts/run_pipeline.py

# 8. Or run individual stages
python scripts/run_ingest.py
python scripts/run_clean.py
python scripts/run_embed.py
python scripts/run_store.py
```
