"""Load verified MVP graph and local text index, publishing readiness only on success."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.ingest.source_download import atomic_write
from src.store.mvp_graph import load_graph
from src.store.mvp_index import build_index


def load_stores(season: str, output: Path) -> dict:
    report = {"season": season, "loaded_at": datetime.now(UTC).isoformat(), "valid": False}
    path = output / "reports" / "mvp" / season / "stores.json"
    try:
        settings.require_credentials("neo4j_password")
        report["graph"] = load_graph(
            output, season, settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password
        )
        report["index"] = build_index(output, season)
        report["valid"] = True
    except Exception as exc:
        report["error"] = str(exc)
        atomic_write(path, (json.dumps(report, indent=2) + "\n").encode())
        raise
    atomic_write(path, (json.dumps(report, indent=2) + "\n").encode())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    args = parser.parse_args()
    report = load_stores(args.season, args.output)
    print(
        f"Verified graph: {report['graph']['counts']}; "
        f"local text documents: {report['index']['documents']}"
    )


if __name__ == "__main__":
    main()
