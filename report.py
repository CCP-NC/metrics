# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas", "pyyaml", "plotly", "kaleido", "jinja2", "weasyprint"]
# ///
"""Build a PDF usage report plus PNG figures and CSV tables for a date range.

    uv run report.py --from 2026-01-01 --to 2026-09-30

Output goes to reports/<from>_to_<to>/ (report.pdf, report-for-docs.html, png/, tables/).
On macOS, WeasyPrint needs Homebrew's pango: brew install pango
"""
import argparse
import base64
import os
import sys
from datetime import date, datetime
from pathlib import Path

if sys.platform == "darwin":
    os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from ccpnc_metrics import data, plots

ROOT = Path(__file__).resolve().parent


def pick_freq(start: date, end: date) -> str:
    days = (end - start).days + 1
    return "D" if days <= 14 else "W" if days <= 120 else "M"


def build_figures(start: date, end: date):
    freq = pick_freq(start, end)
    table = data.project_table(start, end)
    dl = data.in_period(data.package_downloads_daily(), start, end)
    views = data.in_period(data.github_daily("views"), start, end)
    clones = data.in_period(data.github_daily("clones"), start, end)
    all_dl = data.downloads()
    return table, {
        "downloads_by_project": (plots.downloads_by_project(table), (1000, 340)),
        "pypi": (plots.downloads_over_time(dl, start, end, freq, registry="pypi"), (1000, 360)),
        "npm": (plots.downloads_over_time(dl, start, end, freq, registry="npm"), (1000, 340)),
        "python": (plots.breakdown(all_dl, "python_minor", start, end), (500, 340)),
        "system": (plots.breakdown(all_dl, "system", start, end), (500, 340)),
        "github": (plots.github_traffic_over_time(views, clones, start, end, "W" if freq == "D" else freq), (1000, 340)),
        "github_projects": (plots.github_by_project(table), (1000, 420)),
        "referrers": (plots.top_referrers(data.referrers(start, end)), (1000, 420)),
    }


def headline_rows(start: date, end: date) -> list[dict]:
    rows = []
    for metric, r in data.headline(start, end).iterrows():
        if pd.isna(r["change_pct"]):
            change, cls = "n/a", "na"
        else:
            change = f"{r['change_pct']:+.1f}%"
            cls = "up" if r["change_pct"] > 0 else "down" if r["change_pct"] < 0 else "na"
        rows.append(dict(metric=metric, current=int(r["current"]), previous=int(r["previous"]), change=change, cls=cls))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    ap.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    ap.add_argument("--out", type=Path, default=ROOT / "reports")
    args = ap.parse_args()
    start, end = args.start, args.end
    if end < start:
        ap.error("--to must not be before --from")

    out = args.out / f"{start}_to_{end}"
    (out / "png").mkdir(parents=True, exist_ok=True)
    (out / "tables").mkdir(exist_ok=True)

    table, figs = build_figures(start, end)
    png = {}
    for name, (fig, (w, h)) in figs.items():
        path = out / "png" / f"{name}.png"
        plots.to_png(fig, path, width=w, height=h)
        png[name] = f"png/{name}.png"

    table.to_csv(out / "tables" / "projects.csv", index=False)
    data.headline(start, end).to_csv(out / "tables" / "headline.csv")
    data.in_period(data.package_downloads_daily(), start, end).to_csv(out / "tables" / "downloads_daily.csv", index=False)

    env = Environment(loader=FileSystemLoader(ROOT / "templates"))
    context = dict(
        start=start, end=end, generated=datetime.now().strftime("%Y-%m-%d"),
        headline=headline_rows(start, end), coverage=data.data_coverage(),
        table_cols=["Project"] + list(table.columns[1:]),
        table_rows=[[row[0]] + [f"{v:,}" if v else "–" for v in row[1:]] for row in table.itertuples(index=False)],
    )
    template = env.get_template("report.html")
    html = template.render(png=png, **context)
    (out / "report.html").write_text(html)

    # Self-contained copy with embedded images: open in a browser, select all, paste into Google Docs.
    embedded = {k: "data:image/png;base64," + base64.b64encode((out / v).read_bytes()).decode() for k, v in png.items()}
    (out / "report-for-docs.html").write_text(template.render(png=embedded, **context))

    from weasyprint import HTML
    HTML(string=html, base_url=str(out)).write_pdf(out / "report.pdf")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
