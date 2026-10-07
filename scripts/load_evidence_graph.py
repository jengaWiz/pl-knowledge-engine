"""Load accepted evidence into owned Neo4j nodes without rewriting the default MVP."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.store.evidence_graph import load_graph


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    args = parser.parse_args()
    settings.require_credentials("neo4j_password")
    report = load_graph(
        args.output, args.season, settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password
    )
    print(
        f"Verified evidence graph: {report['counts']}; internal links: "
        f"{report['relationships']}; canonical MVP links: {report['canonical_links']}"
    )


if __name__ == "__main__":
    main()
