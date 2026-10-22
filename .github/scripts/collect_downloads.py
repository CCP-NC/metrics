"""Collect package download statistics (PyPI via pypistats, npm) for projects in projects.yml.

pypistats only keeps ~180 days, so each run merges what the API returns into a
long-lived tidy CSV: traffic-stats/downloads.csv
(date, registry, package, metric, category, downloads).
"""
import argparse
import logging
import os
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "traffic-stats" / "downloads.csv"
KEY = ["date", "registry", "package", "metric", "category"]
PYPI_METRICS = ["overall", "python_minor", "system"]
NPM_START = "2024-01-01"  # npm's range API allows at most ~18 months

session = requests.Session()
session.headers["User-Agent"] = "ccpnc-metrics (https://github.com/CCP-NC/metrics)"
log = logging.getLogger("collect_downloads")


def get_json(url: str, retries: int = 4):
    for attempt in range(retries):
        r = session.get(url, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            log.warning("404 for %s", url)
            return None
        time.sleep(2 * 2**attempt)  # 429s and transient errors
    r.raise_for_status()


def pypi_rows(package: str) -> list[dict]:
    rows = []
    for metric in PYPI_METRICS:
        data = get_json(f"https://pypistats.org/api/packages/{package}/{metric}")
        time.sleep(1)  # be polite; pypistats rate-limits
        if not data:
            continue
        rows += [
            dict(date=d["date"], registry="pypi", package=package, metric=metric,
                 category=d["category"], downloads=d["downloads"])
            for d in data["data"]
        ]
    return rows


def npm_rows(package: str) -> list[dict]:
    end = date.today().isoformat()
    data = get_json(f"https://api.npmjs.org/downloads/range/{NPM_START}:{end}/{package}")
    if not data:
        return []
    return [
        dict(date=d["day"], registry="npm", package=package, metric="overall",
             category="all", downloads=d["downloads"])
        for d in data["downloads"]
    ]


def warn_unlisted_repos(config: dict) -> None:
    """Warn if the org has public repos that are not in projects.yml."""
    token = os.environ.get("GH_TOKEN")
    if not token:
        return
    r = session.get(
        f"https://api.github.com/orgs/{config['org']}/repos",
        params={"type": "public", "per_page": 100},
        headers={"Authorization": f"token {token}"},
        timeout=30,
    )
    r.raise_for_status()
    known = set(config.get("ignore", []))
    for p in config["projects"]:
        known.update(p.get("repos", []))
    for repo in r.json():
        if not repo["archived"] and repo["name"] not in known:
            msg = f"Repo {repo['name']} is not listed in projects.yml"
            print(f"WARNING: {msg}")
            log.warning(msg)


def merge(new: pd.DataFrame) -> pd.DataFrame:
    if OUT.exists():
        old = pd.read_csv(OUT)
        new = pd.concat([old, new]).drop_duplicates(KEY, keep="last")
    return new.sort_values(KEY).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=ROOT / "projects.yml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    config = yaml.safe_load(Path(args.config).read_text())
    warn_unlisted_repos(config)

    rows = []
    for project in config["projects"]:
        for pkg in project.get("pypi", []):
            rows += pypi_rows(pkg)
        for pkg in project.get("npm", []):
            rows += npm_rows(pkg)

    if not rows:
        print("No download data collected")
        return
    df = merge(pd.DataFrame(rows))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"{len(df)} rows in {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
