"""Route natural-language requests and retrieve graph-constrained local evidence."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.retrieval.hybrid import retrieve


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query")
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season, help="Primary corpus season")
    parser.add_argument("--evidence-season", required=True)
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    settings.require_credentials("neo4j_password")
    result = retrieve(
        args.output,
        args.season,
        args.query,
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
        evidence_season=args.evidence_season,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
