"""Start, prepare expanded evidence, inspect or back up the persistent local demo."""

import argparse
import json
import os
import secrets
import shutil
import subprocess
import time
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "pl-knowledge-engine-demo"
URL = "http://127.0.0.1:8010"


@contextmanager
def public_image_client():
    """Use the current Docker context/plugins without invoking a credential store."""
    previous = os.environ.get("DOCKER_CONFIG")
    source = Path(previous) if previous else Path.home() / ".docker"
    config_path = source / "config.json"
    config = json.loads(config_path.read_text()) if config_path.exists() else {}
    with TemporaryDirectory(prefix="pl-public-images-") as folder:
        target = Path(folder)
        public_config = {
            key: config[key] for key in ("currentContext", "cliPluginsExtraDirs") if key in config
        }
        (target / "config.json").write_text(json.dumps(public_config))
        for name in ("contexts", "cli-plugins"):
            if (source / name).is_dir():
                shutil.copytree(source / name, target / name, symlinks=True)
        os.environ["DOCKER_CONFIG"] = folder
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop("DOCKER_CONFIG", None)
            else:
                os.environ["DOCKER_CONFIG"] = previous


def prepare_secret(path: Path) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.exists():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write("NEO4J_PASSWORD=" + secrets.token_urlsafe(32) + "\n")
    path.chmod(0o600)
    values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line)
    if set(values) != {"NEO4J_PASSWORD"}:
        raise ValueError("Private demo file must contain only NEO4J_PASSWORD")
    if not values.get("NEO4J_PASSWORD"):
        raise ValueError("Private demo credentials are incomplete")
    return values


def status(path="/api/readiness", timeout=15) -> dict:
    try:
        with urlopen(URL + path, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        if exc.code in {429, 503}:
            return {**json.load(exc), "status": "not_ready"}
        raise
    except (URLError, TimeoutError):
        return {"status": "not_ready", "detail": "Local demo is not running"}


def run_demo(command="up", *, recollect=False):
    if command == "evidence-status":
        print(json.dumps(status("/api/evidence/status", timeout=120), indent=2))
        return
    if command == "status":
        print(json.dumps(status(), indent=2))
        return
    secret_file = ROOT / "data/private/demo.env"
    env = os.environ.copy()
    env.update(prepare_secret(secret_file))
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(secret_file),
        "-p",
        PROJECT,
        "-f",
        str(ROOT / "compose.yml"),
    ]

    def run(args):
        subprocess.run(args, cwd=ROOT, env=env, check=True)

    if command == "stop":
        run(compose + ["stop"])
        return
    if command == "backup":
        names = ("corpus", "index", "models", "graph")
        for name in names:
            subprocess.run(
                ["docker", "volume", "inspect", PROJECT + "_" + name],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        folder = ROOT / "data/backups" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        folder.mkdir(parents=True, mode=0o700)
        shutil.copyfile(secret_file, folder / "demo.env")
        (folder / "demo.env").chmod(0o600)
        run(compose + ["stop"])
        try:
            args = ["docker", "run", "--rm", "--user", "0"]
            for name in names:
                args += ["-v", f"{PROJECT}_{name}:/snapshot/{name}:ro"]
            args += [
                "-v",
                f"{folder}:/backup",
                PROJECT + ":local",
                "python",
                "-c",
                "import tarfile,os; "
                "archive=tarfile.open('/backup/volumes.tar.gz','w:gz'); "
                "archive.add('/snapshot',arcname='volumes'); archive.close(); "
                "os.chmod('/backup/volumes.tar.gz',0o600)",
            ]
            run(args)
        finally:
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "180"])
        print(f"Private offline backup: {folder}")
        return
    run(compose + ["up", "-d", "--build", "--wait", "--wait-timeout", "180"])
    if status()["status"] != "ready" or recollect:
        for script, flags in [
            ("run_mvp.py", ["--commentary"]),
            ("load_mvp.py", []),
            ("accept_mvp.py", []),
        ]:
            print(f"Running {script}", flush=True)
            run(
                compose + ["run", "--rm", "--no-deps", "app", "python", "scripts/" + script] + flags
            )
    for _ in range(6):
        report = status()
        if report["status"] == "ready":
            if command == "evidence":
                run(
                    compose
                    + ["run", "--rm", "--no-deps", "app", "python", "scripts/prepare_evidence.py"]
                )
                expanded = status("/api/evidence/status", timeout=120)
                if expanded["status"] != "ready":
                    raise RuntimeError("Evidence preparation finished but API readiness failed")
                print(
                    f"Source evidence ready: {expanded['index']['documents']} documents; "
                    f"seasons: {', '.join(expanded['seasons'])}"
                )
            print(f"Local demo ready: {URL}")
            print(
                f"Verified corpus: {report['counts']}; "
                f"index: {report['index']['documents']} summaries"
            )
            return
        time.sleep(2)
    raise RuntimeError("Demo is not ready; inspect /api/readiness and the dataset reports")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=["up", "evidence", "status", "evidence-status", "stop", "backup"],
        default="up",
        nargs="?",
    )
    parser.add_argument(
        "--recollect", action="store_true", help="Revalidate caches and reload all stores"
    )
    parser.add_argument(
        "--public-images",
        action="store_true",
        help="Build public images with an isolated client; preserve the existing Docker login",
    )
    args = parser.parse_args()
    with public_image_client() if args.public_images else nullcontext():
        run_demo(args.command, recollect=args.recollect)


if __name__ == "__main__":
    main()
