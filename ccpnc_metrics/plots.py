"""Plotly figures shared by the dashboard (interactive) and report (static PNG)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

FREQS = {"Day": "D", "Week": "W", "Month": "M"}
PALETTE = [
    "#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00", "#56B4E9", "#7A3E9D",
    "#8C510A", "#1B7837", "#B2182B", "#4D4D4D", "#2166AC", "#F4A582", "#66C2A5",
]
OTHER_COLOUR = "#b0b0b0"

pio.templates["ccpnc"] = go.layout.Template(
    layout=dict(
        font=dict(family="Helvetica Neue, Helvetica, Arial, sans-serif", size=13, color="#222"),
        colorway=PALETTE,
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, linecolor="#999", ticks="outside"),
        yaxis=dict(gridcolor="#e6e6e6", zeroline=False, automargin=True),
        legend=dict(orientation="h", y=-0.2, title_text=""),
        margin=dict(l=60, r=20, t=60, b=60),
        title=dict(x=0, xanchor="left", font=dict(size=16)),
    )
)
pio.templates.default = "ccpnc"


def _colours(names: list[str]) -> dict[str, str]:
    """Stable colour per project regardless of which subset is plotted."""
    from .data import project_names

    order = project_names()
    cmap = {n: PALETTE[i % len(PALETTE)] for i, n in enumerate(order)}
    cmap["Other"] = OTHER_COLOUR
    return {n: cmap.get(n, OTHER_COLOUR) for n in names}


def _bin(df: pd.DataFrame, value: str, freq: str, start, end, by: str | None = "project") -> pd.DataFrame:
    """Sum `value` into day/week/month bins, filling empty bins with zero."""
    df = df.copy()
    df["bin"] = df["date"].dt.to_period(freq).dt.start_time
    keys = ["bin"] + ([by] if by else [])
    g = df.groupby(keys)[value].sum().reset_index()
    bins = pd.period_range(start, end, freq=freq).start_time
    if by is None:
        return g.set_index("bin").reindex(bins, fill_value=0).rename_axis("bin").reset_index()
    full = pd.MultiIndex.from_product([bins, g[by].unique()], names=["bin", by])
    return g.set_index(keys).reindex(full, fill_value=0).reset_index()


def _top_n(df: pd.DataFrame, value: str, n: int, by: str = "project") -> pd.DataFrame:
    top = df.groupby(by)[value].sum().nlargest(n).index
    out = df.copy()
    out[by] = out[by].where(out[by].isin(top), "Other")
    return out


def _empty(title: str, msg: str = "No data in this period") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=msg, showarrow=False, font=dict(size=14, color="#888"))
    fig.update_layout(title=title, xaxis_visible=False, yaxis_visible=False)
    return fig


def downloads_over_time(df: pd.DataFrame, start, end, freq: str = "M", top_n: int = 6,
                        registry: str = "pypi") -> go.Figure:
    """Stacked bars of package downloads per bin, split by project."""
    label = {"pypi": "PyPI downloads (excluding mirrors)", "npm": "npm downloads"}[registry]
    df = df[df["registry"] == registry]
    if df.empty or df["downloads"].sum() == 0:
        return _empty(label)
    df = _top_n(df, "downloads", top_n)
    b = _bin(df, "downloads", freq, start, end)
    order = b.groupby("project")["downloads"].sum().sort_values(ascending=False).index
    colours = _colours(list(order))
    fig = go.Figure()
    for p in order:
        s = b[b["project"] == p]
        fig.add_bar(x=s["bin"], y=s["downloads"], name=p, marker_color=colours[p])
    fig.update_layout(barmode="stack", title=label, yaxis_title="Downloads")
    return fig


def downloads_by_project(table: pd.DataFrame) -> go.Figure:
    """Horizontal bars of total downloads per project (PyPI + npm)."""
    t = table.assign(total=table["PyPI downloads"] + table["npm downloads"])
    t = t[t["total"] > 0].sort_values("total")
    if t.empty:
        return _empty("Package downloads by project")
    colours = _colours(list(t["project"]))
    fig = go.Figure()
    fig.add_bar(x=t["total"], y=t["project"], orientation="h", marker_color=[colours[p] for p in t["project"]],
                text=t["total"].map("{:,}".format), textposition="outside", cliponaxis=False)
    fig.update_layout(title="Package downloads by project (PyPI excl. mirrors + npm)",
                      xaxis_title="Downloads", yaxis_title="", showlegend=False)
    fig.update_xaxes(showgrid=True, gridcolor="#e6e6e6")
    return fig


def github_traffic_over_time(views: pd.DataFrame, clones: pd.DataFrame, start, end, freq: str = "W",
                             metric: str = "uniques") -> go.Figure:
    """Organisation-wide GitHub visitors and cloners (sum of daily uniques, or raw counts)."""
    what = "Unique" if metric == "uniques" else "Total"
    fig = go.Figure()
    for name, df, colour in [(f"{what} visitors (views)", views, PALETTE[0]),
                             (f"{what} cloners (clones)", clones, PALETTE[1])]:
        if df.empty:
            continue
        b = _bin(df, metric, freq, start, end, by=None)
        fig.add_scatter(x=b["bin"], y=b[metric], name=name, mode="lines", line=dict(color=colour, width=2))
    if not fig.data:
        return _empty("GitHub traffic")
    unit = "visitor-days" if metric == "uniques" else "events"
    fig.update_layout(title="GitHub traffic across the organisation", yaxis_title=f"Sum of daily {unit}"
                      if metric == "uniques" else "Count")
    return fig


def github_by_project(table: pd.DataFrame, top_n: int = 12) -> go.Figure:
    """Horizontal bars of GitHub visitor-days per project."""
    t = table[table["Visitor-days"] > 0].nlargest(top_n, "Visitor-days").sort_values("Visitor-days")
    if t.empty:
        return _empty("GitHub visitors by project")
    colours = _colours(list(t["project"]))
    fig = go.Figure()
    fig.add_bar(x=t["Visitor-days"], y=t["project"], orientation="h", marker_color=[colours[p] for p in t["project"]],
                text=t["Visitor-days"].map("{:,}".format), textposition="outside", cliponaxis=False)
    fig.update_layout(title="GitHub visitors by project (sum of daily uniques)",
                      xaxis_title="Visitor-days", yaxis_title="", showlegend=False)
    fig.update_xaxes(showgrid=True, gridcolor="#e6e6e6")
    return fig


def top_referrers(df: pd.DataFrame, n: int = 12) -> go.Figure:
    """Where GitHub visitors came from."""
    if df.empty:
        return _empty("Top referrers")
    t = df.groupby("referrer")[["count", "uniques"]].sum().nlargest(n, "uniques").sort_values("uniques")
    fig = go.Figure()
    fig.add_bar(x=t["uniques"], y=t.index, orientation="h", marker_color=PALETTE[0],
                text=t["uniques"].map("{:,}".format), textposition="outside", cliponaxis=False)
    fig.update_layout(title="Top referrers to GitHub repositories", xaxis_title="Unique visitors (14-day windows, summed)",
                      yaxis_title="", showlegend=False)
    fig.update_xaxes(showgrid=True, gridcolor="#e6e6e6")
    return fig


def breakdown(downloads: pd.DataFrame, metric: str, start, end, top_n: int = 6) -> go.Figure:
    """Share of PyPI downloads by Python version ('python_minor') or OS ('system')."""
    title = {"python_minor": "PyPI downloads by Python version", "system": "PyPI downloads by operating system"}[metric]
    d = downloads[(downloads["metric"] == metric) & (downloads["registry"] == "pypi")]
    d = d[(d["date"] >= pd.Timestamp(start)) & (d["date"] <= pd.Timestamp(end))]
    d = d[d["category"] != "null"]
    if d.empty:
        return _empty(title)
    t = d.groupby("category")["downloads"].sum().sort_values(ascending=False)
    if len(t) > top_n:
        t = pd.concat([t.iloc[:top_n], pd.Series({"Other": t.iloc[top_n:].sum()})])
    fig = go.Figure(go.Pie(labels=t.index, values=t.values, hole=0.5, sort=False, textinfo="percent",
                           textposition="inside", marker_colors=PALETTE))
    fig.update_layout(title=title, showlegend=True, legend=dict(orientation="v", y=0.5, x=1.0),
                      uniformtext=dict(minsize=10, mode="hide"))
    return fig


def to_png(fig: go.Figure, path: Path, width: int = 1000, height: int = 520, scale: int = 2) -> None:
    fig.write_image(str(path), width=width, height=height, scale=scale)
