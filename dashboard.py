# /// script
# requires-python = ">=3.10"
# dependencies = ["marimo", "pandas", "pyyaml", "plotly"]
# ///
import marimo

app = marimo.App(width="full", app_title="CCP-NC usage metrics")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from datetime import date, timedelta
    from ccpnc_metrics import data, plots

    return data, date, mo, pd, plots, timedelta


@app.cell
def _(data, date, mo, timedelta):
    cov = data.data_coverage()
    latest = cov["Latest data"]
    dates = mo.ui.date_range(
        start=cov["GitHub traffic"], stop=latest,
        value=(latest - timedelta(days=89), latest), label="Period",
    )
    freq = mo.ui.dropdown({"Day": "D", "Week": "W", "Month": "M"}, value="Week", label="Bin by")
    projects = mo.ui.multiselect(data.project_names(), value=data.project_names(), label="Projects")
    mo.hstack([dates, freq, projects], justify="start", align="end", wrap=True)
    return cov, dates, freq, projects


@app.cell
def _(dates, data, mo, projects):
    start, end = dates.value
    selected = list(projects.value)
    h = data.headline(start, end, selected)
    cards = []
    for metric, r in h.iterrows():
        delta = None if r["change_pct"] != r["change_pct"] else f"{r['change_pct']:+.1f}% vs previous period"
        cards.append(mo.stat(value=f"{int(r['current']):,}", label=metric, caption=delta,
                             direction="increase" if (r["change_pct"] or 0) > 0 else "decrease"
                             if (r["change_pct"] or 0) < 0 else None, bordered=True))
    mo.vstack([mo.hstack(cards[:3], widths="equal"), mo.hstack(cards[3:], widths="equal")])
    return end, h, selected, start


@app.cell
def _(cov, mo):
    mo.callout(mo.md(
        "**Proxies, not people.** Downloads include CI and bots (PyPI excludes mirrors). "
        "*Visitor-days* add up GitHub's daily unique counts, so repeat visits and multiple repos count repeatedly. "
        f"Data starts: GitHub {cov['GitHub traffic']}, npm {cov['npm downloads']}, PyPI {cov['PyPI downloads']}."
    ), kind="warn")
    return


@app.cell
def _(data, end, freq, mo, plots, selected, start):
    dl = data.in_period(data.package_downloads_daily(), start, end)
    dl = dl[dl["project"].isin(selected)]
    tbl = data.project_table(start, end)
    tbl = tbl[tbl["project"].isin(selected)]
    views = data.in_period(data.github_daily("views"), start, end)
    views = views[views["project"].isin(selected)]
    clones = data.in_period(data.github_daily("clones"), start, end)
    clones = clones[clones["project"].isin(selected)]
    refs = data.referrers(start, end)
    refs = refs[refs["project"].isin(selected)]
    all_dl = data.downloads()
    all_dl = all_dl[all_dl["project"].isin(selected)]
    f = freq.value

    tabs = mo.ui.tabs({
        "Downloads": mo.vstack([
            mo.hstack([plots.downloads_over_time(dl, start, end, f, registry="pypi"),
                       plots.downloads_by_project(tbl)], widths="equal"),
            mo.hstack([plots.downloads_over_time(dl, start, end, f, registry="npm"),
                       plots.breakdown(all_dl, "python_minor", start, end),
                       plots.breakdown(all_dl, "system", start, end)], widths="equal"),
        ]),
        "GitHub": mo.vstack([
            plots.github_traffic_over_time(views, clones, start, end, f),
            mo.hstack([plots.github_by_project(tbl), plots.top_referrers(refs)], widths="equal"),
        ]),
        "Table": mo.ui.table(tbl, selection=None, page_size=20),
    })
    tabs
    return


if __name__ == "__main__":
    app.run()
