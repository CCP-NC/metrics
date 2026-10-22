"""Load and aggregate GitHub traffic and package download data, joined through projects.yml."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
STATS = ROOT / "traffic-stats"


def load_projects() -> dict:
    return yaml.safe_load((ROOT / "projects.yml").read_text())


def _repo_map() -> dict[str, str]:
    return {r: p["name"] for p in load_projects()["projects"] for r in p.get("repos", [])}


def _package_map() -> dict[tuple[str, str], str]:
    return {
        (registry, pkg): p["name"]
        for p in load_projects()["projects"]
        for registry in ("pypi", "npm")
        for pkg in p.get(registry, [])
    }


def project_names() -> list[str]:
    return [p["name"] for p in load_projects()["projects"]]


def github_daily(kind: str) -> pd.DataFrame:
    """Daily GitHub 'views' or 'clones': columns date, repo, project, count, uniques.

    Days missing from the API had no traffic. Repos not in projects.yml are dropped.
    """
    repos = _repo_map()
    suffix = f"-{kind}-combined.json"
    rows = []
    for f in STATS.glob(f"*{suffix}"):
        repo = f.name[: -len(suffix)]
        if repo not in repos:
            continue
        for e in json.loads(f.read_text()):
            rows.append((e["timestamp"][:10], repo, repos[repo], e["count"], e["uniques"]))
    df = pd.DataFrame(rows, columns=["date", "repo", "project", "count", "uniques"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def downloads(registry: str | None = None) -> pd.DataFrame:
    """Package downloads: date, registry, package, metric, category, downloads, project."""
    df = pd.read_csv(STATS / "downloads.csv", parse_dates=["date"])
    pkgs = _package_map()
    df["project"] = [pkgs.get(k) for k in zip(df["registry"], df["package"])]
    df = df.dropna(subset=["project"])
    return df if registry is None else df[df["registry"] == registry]


def package_downloads_daily() -> pd.DataFrame:
    """Headline download series: PyPI without mirrors plus npm, one row per day and package."""
    d = downloads()
    keep = ((d["registry"] == "pypi") & (d["category"] == "without_mirrors")) | (d["registry"] == "npm")
    return d[keep & (d["metric"] == "overall")][["date", "registry", "package", "project", "downloads"]]


def referrers(start: date, end: date, step_days: int = 14) -> pd.DataFrame:
    """Referrer totals over a period: repo, project, referrer, count, uniques.

    GitHub reports referrers as rolling 14-day windows and we snapshot them daily, so
    adding every snapshot would count the same visits repeatedly. We take one snapshot
    every `step_days`, walking back from `end`, so the windows do not overlap.
    """
    repos = _repo_map()
    suffix = "-referrers-combined.json"
    rows = []
    for f in STATS.glob(f"*{suffix}"):
        repo = f.name[: -len(suffix)]
        if repo not in repos:
            continue
        snaps = {e["timestamp"][:10]: e["data"] for e in json.loads(f.read_text())}
        picks, cur = [], pd.Timestamp(end)
        while cur >= pd.Timestamp(start):
            avail = [d for d in snaps if pd.Timestamp(d) <= cur]
            if avail:
                picks.append(max(avail))
            cur -= timedelta(days=step_days)
        for d in set(picks):
            rows += [(repo, repos[repo], r["referrer"], r["count"], r["uniques"]) for r in snaps[d]]
    df = pd.DataFrame(rows, columns=["repo", "project", "referrer", "count", "uniques"])
    return df.groupby(["repo", "project", "referrer"], as_index=False)[["count", "uniques"]].sum()


def in_period(df: pd.DataFrame, start: date, end: date, col: str = "date") -> pd.DataFrame:
    return df[(df[col] >= pd.Timestamp(start)) & (df[col] <= pd.Timestamp(end))]


def previous_period(start: date, end: date) -> tuple[date, date]:
    length = (end - start) + timedelta(days=1)
    return start - length, start - timedelta(days=1)


def _totals(start: date, end: date, projects: list[str] | None = None) -> dict[str, int]:
    def sel(df):
        df = in_period(df, start, end)
        return df if projects is None else df[df["project"].isin(projects)]

    dl = sel(package_downloads_daily())
    views, clones = sel(github_daily("views")), sel(github_daily("clones"))
    return {
        "PyPI downloads (excl. mirrors)": int(dl.loc[dl["registry"] == "pypi", "downloads"].sum()),
        "npm downloads": int(dl.loc[dl["registry"] == "npm", "downloads"].sum()),
        "GitHub page views": int(views["count"].sum()),
        "GitHub visitor-days (sum of daily uniques)": int(views["uniques"].sum()),
        "GitHub clones": int(clones["count"].sum()),
        "GitHub cloner-days (sum of daily uniques)": int(clones["uniques"].sum()),
    }


def headline(start: date, end: date, projects: list[str] | None = None) -> pd.DataFrame:
    """Headline numbers for the period with the change against the previous equal-length period."""
    cur = _totals(start, end, projects)
    prev = _totals(*previous_period(start, end), projects)
    out = pd.DataFrame({"current": cur, "previous": prev})
    out["change_pct"] = ((out["current"] - out["previous"]) / out["previous"].where(out["previous"] > 0) * 100).round(1)
    # A comparison is only meaningful if we have data for the whole previous period.
    cov = data_coverage()
    first = {
        "PyPI": cov["PyPI downloads"], "npm": cov["npm downloads"], "GitHub": cov["GitHub traffic"],
    }
    prev_start = previous_period(start, end)[0]
    for metric in out.index:
        source = next(v for k, v in first.items() if metric.startswith(k))
        if prev_start < source:
            out.loc[metric, "change_pct"] = float("nan")
    return out


def project_table(start: date, end: date) -> pd.DataFrame:
    """One row per project, joining registry downloads and GitHub traffic."""
    dl = in_period(package_downloads_daily(), start, end)
    views = in_period(github_daily("views"), start, end)
    clones = in_period(github_daily("clones"), start, end)
    t = pd.DataFrame({"project": project_names()}).set_index("project")
    t["PyPI downloads"] = dl[dl["registry"] == "pypi"].groupby("project")["downloads"].sum()
    t["npm downloads"] = dl[dl["registry"] == "npm"].groupby("project")["downloads"].sum()
    t["GitHub views"] = views.groupby("project")["count"].sum()
    t["Visitor-days"] = views.groupby("project")["uniques"].sum()
    t["GitHub clones"] = clones.groupby("project")["count"].sum()
    t["Cloner-days"] = clones.groupby("project")["uniques"].sum()
    t = t.fillna(0).astype(int)
    return t.sort_values(["PyPI downloads", "npm downloads", "Visitor-days"], ascending=False).reset_index()


def data_coverage() -> dict[str, date]:
    """First date of each data source, for stating coverage in reports."""
    return {
        "GitHub traffic": github_daily("views")["date"].min().date(),
        "PyPI downloads": downloads("pypi")["date"].min().date(),
        "npm downloads": downloads("npm")["date"].min().date(),
        "Latest data": downloads()["date"].max().date(),
    }
