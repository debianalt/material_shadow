"""
02_analysis_figures.py — Generate all figures for the TFSC article
==================================================================
'The Material Shadow of the Digital Economy'

Reads computed ICT footprint/multiplier data and produces publication-quality
figures using matplotlib, seaborn, and geopandas.

Main-text figures:
  Fig 1 — Global ICT material shadow by MFA category, 1990–2029 (stacked area)
  Fig 2 — Metal ores subcategory composition, 2029 (horizontal bar)
  Fig 3 — Geographic distribution: butterfly chart — consumption vs extraction
  Fig 4 — Net importers vs exporters, 2029 (diverging bar)
  Fig 5 — Bilateral flows: chord diagram — top supply-chain corridors
  Fig 6 — USA ICT material shadow by extraction origin (bar)

Supplementary figures:
  Fig S1 — MFA category shares over time (100% stacked bar)
  Fig S2 — China vs USA temporal trajectory (line chart)
  Fig S3 — Copper extraction by country of origin (stacked area)
  Fig S4 — Choropleth: ICT material shadow net balance (world map)

Author: Raimundo Elías Gómez
"""

from pathlib import Path

import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from mpl_chord_diagram import chord_diagram

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_REPO = Path(__file__).resolve().parent.parent          # subirGitHubTFSC/
DATA_DIR = _REPO / "data"
FIG_DIR = _REPO / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

NE_PATH = f"zip://{_REPO / 'data' / 'ne_110m_admin_0_countries.zip'}"

# Colour palette
MFA_COLORS = {
    "Biomass": "#27ae60",
    "Metal ores": "#8e44ad",
    "Non-metallic minerals": "#f39c12",
    "Fossil fuels": "#2c3e50",
}
MFA_ORDER = ["Non-metallic minerals", "Metal ores", "Fossil fuels", "Biomass"]

# Plot style
plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["Times New Roman", "DejaVu Serif"],
    "font.size": 11,
    "axes.labelsize": 13,
    "axes.titlesize": 13,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11,
    "legend.fontsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.linewidth": 0.8,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
})

YEARS = [1990, 1995, 2000, 2005, 2010, 2015, 2022, 2029]
LATEST_YEAR = YEARS[-1]


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_data():
    """Load all footprint and multiplier data."""
    baseline = DATA_DIR / "baseline"
    foot = pd.concat(
        [pd.read_parquet(f) for f in sorted(baseline.glob("ict_footprint_*.parquet"))],
        ignore_index=True,
    )
    mult = pd.concat(
        [pd.read_parquet(f) for f in sorted(baseline.glob("ict_multipliers_*.parquet"))],
        ignore_index=True,
    )
    regions = pd.read_parquet(DATA_DIR / "region_order.parquet")
    return foot, mult, regions


# ---------------------------------------------------------------------------
# Helper: save figure in PNG + PDF
# ---------------------------------------------------------------------------
def _save(fig, name):
    """Save figure in both PNG and PDF formats."""
    fig.savefig(FIG_DIR / f"{name}.png")
    fig.savefig(FIG_DIR / f"{name}.pdf")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Fig 1 — Global ICT material shadow over time (stacked area)
# ---------------------------------------------------------------------------
def fig01_temporal_stacked(foot):
    """Stacked area chart: ICT material shadow by MFA category, 1990-2029."""
    pivot = (
        foot.groupby(["year", "mfa_category"])["footprint_tonnes"]
        .sum()
        .unstack(fill_value=0)
        / 1e9
    )
    pivot = pivot[MFA_ORDER]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.stackplot(
        pivot.index,
        [pivot[cat] for cat in MFA_ORDER],
        labels=MFA_ORDER,
        colors=[MFA_COLORS[cat] for cat in MFA_ORDER],
        alpha=0.85,
    )

    ax.set_xlabel("Year")
    ax.set_ylabel("ICT material shadow (Gt)")
    ax.set_xlim(1990, LATEST_YEAR)
    ax.set_ylim(0, None)
    ax.legend(loc="upper left", frameon=False, fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5, color="#cccccc")
    ax.set_axisbelow(True)

    fig.tight_layout()
    _save(fig, "fig01_temporal_stacked")
    print("  Fig 1: temporal stacked area")


# ---------------------------------------------------------------------------
# Fig 2 — Metal ores subcategory composition (horizontal bar)
# ---------------------------------------------------------------------------
def fig02_metal_ores(foot):
    """Horizontal bar: metal ores subcategory composition for ICT, latest year."""
    f22 = foot[(foot["year"] == LATEST_YEAR) & (foot["mfa_category"] == "Metal ores")]
    by_sub = f22.groupby("mfa_subcategory")["footprint_tonnes"].sum().sort_values(ascending=True)
    total = by_sub.sum()

    colors = {
        "Copper": "#c0392b",
        "Iron": "#34495e",
        "Other metals": "#8e44ad",
        "Gold": "#f39c12",
        "Aluminium": "#1abc9c",
    }
    bar_colors = [colors.get(s, "#95a5a6") for s in by_sub.index]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.barh(by_sub.index, by_sub.values / 1e6, color=bar_colors,
                   edgecolor="white", linewidth=0.5)

    for bar, (sub, val) in zip(bars, by_sub.items()):
        pct = 100 * val / total
        ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
                f"{val/1e6:,.0f} Mt ({pct:.1f}%)", va="center", fontsize=10)

    ax.set_xlabel("Material extraction (Mt)")
    ax.set_xlim(0, by_sub.max() / 1e6 * 1.25)

    fig.tight_layout()
    _save(fig, "fig02_metal_ores")
    print("  Fig 2: metal ores horizontal bar")


# ---------------------------------------------------------------------------
# Fig 3 — Geographic distribution: butterfly chart (consumption vs extraction)
# ---------------------------------------------------------------------------
def fig03_geographic_bars(foot):
    """Butterfly chart: consumption (left/blue) vs extraction (right/red), top 20, latest year."""
    f29 = foot[foot["year"] == LATEST_YEAR]
    by_cons = f29.groupby("consumption_region")["footprint_tonnes"].sum() / 1e6
    by_extr = f29.groupby("extraction_region")["footprint_tonnes"].sum() / 1e6

    # Union of top 20 by max(consumption, extraction)
    top_cons = set(by_cons.nlargest(20).index)
    top_extr = set(by_extr.nlargest(20).index)
    all_countries = top_cons | top_extr

    df = pd.DataFrame({"consumption": by_cons, "extraction": by_extr}).fillna(0)
    df = df.loc[df.index.isin(all_countries)].sort_values("consumption")

    fig, ax = plt.subplots(figsize=(11, 8.5))
    y = np.arange(len(df))
    bar_height = 0.7

    ax.barh(y, -df["consumption"], height=bar_height, color="#2980b9", edgecolor="white", lw=0.5)
    ax.barh(y, df["extraction"], height=bar_height, color="#c0392b", edgecolor="white", lw=0.5)

    ax.set_yticks(y)
    ax.set_yticklabels(df.index, fontsize=13)
    ax.axvline(0, color="black", lw=1)

    # Value labels at bar tips
    max_val = max(df["consumption"].max(), df["extraction"].max())
    for i, (iso, row) in enumerate(df.iterrows()):
        if row["consumption"] > 0:
            ax.text(-row["consumption"] - max_val * 0.02, i, f"{row['consumption']:,.0f}",
                    va="center", ha="right", fontsize=11, color="#2980b9")
        if row["extraction"] > 0:
            ax.text(row["extraction"] + max_val * 0.02, i, f"{row['extraction']:,.0f}",
                    va="center", ha="left", fontsize=11, color="#c0392b")

    # Panel labels
    ax.text(0.02, 0.99, "(a) Consumption", transform=ax.transAxes,
            fontsize=14, fontweight="bold", va="top", ha="left", color="#2980b9")
    ax.text(0.98, 0.99, "(b) Extraction", transform=ax.transAxes,
            fontsize=14, fontweight="bold", va="top", ha="right", color="#c0392b")

    ax.set_xlabel(
        "\u2190 ICT material shadow (Mt)  |  Material extraction for ICT (Mt) \u2192",
        fontsize=14,
    )

    # Symmetric x-axis
    x_max = max_val * 1.18
    ax.set_xlim(-x_max, x_max)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{abs(v):,.0f}"))
    ax.tick_params(axis="x", labelsize=12)

    ax.xaxis.grid(True, alpha=0.2, linewidth=0.5, color="#cccccc")
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)

    fig.tight_layout()
    _save(fig, "fig03_geographic_bars")
    print("  Fig 3: geographic butterfly chart")


# ---------------------------------------------------------------------------
# Fig 4 — Net importers vs exporters (diverging bar)
# ---------------------------------------------------------------------------
def fig04_net_balance(foot):
    """Diverging bar: top net importers and exporters, latest year."""
    f22 = foot[foot["year"] == LATEST_YEAR]
    cons = f22.groupby("consumption_region")["footprint_tonnes"].sum()
    extr = f22.groupby("extraction_region")["footprint_tonnes"].sum()
    all_reg = sorted(set(cons.index) | set(extr.index))
    balance = pd.Series(
        {r: cons.get(r, 0) - extr.get(r, 0) for r in all_reg}
    ) / 1e6

    top_imp = balance.nlargest(10)
    top_exp = balance.nsmallest(10)
    selected = pd.concat([top_imp, top_exp]).sort_values()

    fig, ax = plt.subplots(figsize=(8, 6))
    colors = ["#c0392b" if v > 0 else "#2980b9" for v in selected.values]
    ax.barh(selected.index, selected.values, color=colors, edgecolor="white", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=1.0)
    ax.set_xlabel("Net ICT material balance (Mt)")

    for i, (region, val) in enumerate(selected.items()):
        offset = 5 if val > 0 else -5
        ha = "left" if val > 0 else "right"
        ax.text(val + offset, i, f"{val:+,.0f}", va="center", ha=ha, fontsize=10)

    ax.xaxis.grid(True, alpha=0.2, linewidth=0.5, color="#cccccc")
    ax.set_axisbelow(True)
    fig.tight_layout()
    _save(fig, "fig04_net_balance")
    print("  Fig 4: net balance diverging bar")


# ---------------------------------------------------------------------------
# Fig 5 — Bilateral flows: chord diagram
# ---------------------------------------------------------------------------
def fig05_bilateral_flows(foot):
    """Chord diagram: top bilateral ICT material flows (extraction to consumption), latest year."""
    f29 = foot[foot["year"] == LATEST_YEAR]

    # Aggregate bilateral flows (extraction -> consumption), exclude intra-country
    bilateral = (
        f29.groupby(["extraction_region", "consumption_region"])["footprint_tonnes"]
        .sum()
        .reset_index()
    )
    bilateral = bilateral[
        bilateral["extraction_region"] != bilateral["consumption_region"]
    ].copy()
    bilateral["flow_mt"] = bilateral["footprint_tonnes"] / 1e6

    # Top 10 countries by total flow involvement (as source OR destination)
    total_as_source = bilateral.groupby("extraction_region")["flow_mt"].sum()
    total_as_dest = bilateral.groupby("consumption_region")["flow_mt"].sum()
    total_involvement = total_as_source.add(total_as_dest, fill_value=0).nlargest(10)
    top_countries = total_involvement.index.tolist()

    # Build directed matrix (rows = extraction, cols = consumption)
    n = len(top_countries)
    matrix = np.zeros((n, n))
    idx = {c: i for i, c in enumerate(top_countries)}
    for _, row in bilateral.iterrows():
        e, c = row["extraction_region"], row["consumption_region"]
        if e in idx and c in idx:
            matrix[idx[e], idx[c]] = row["flow_mt"]

    # Country name mapping for labels
    iso_to_name = {
        "CHN": "China", "USA": "United States", "IND": "India",
        "JPN": "Japan", "KOR": "South Korea", "DEU": "Germany",
        "AUS": "Australia", "BRA": "Brazil", "CHL": "Chile",
        "RUS": "Russia", "IDN": "Indonesia", "ZAF": "South Africa",
        "GBR": "United Kingdom", "FRA": "France", "MEX": "Mexico",
        "CAN": "Canada", "PER": "Peru", "TWN": "Taiwan",
    }
    names = [iso_to_name.get(c, c) for c in top_countries]

    # Qualitative colours (distinct, print-friendly)
    color_list = [
        "#c0392b", "#2980b9", "#e67e22", "#27ae60", "#8e44ad",
        "#f39c12", "#1abc9c", "#34495e", "#d35400", "#7f8c8d",
    ]
    colors = color_list[:n]

    fig, ax = plt.subplots(figsize=(9, 9))
    chord_diagram(
        matrix, names, ax=ax, colors=colors, fontsize=14,
        sort="size", rotate_names=True, fontcolor="black",
    )

    fig.tight_layout(pad=1.5)
    _save(fig, "fig05_bilateral_flows")
    print("  Fig 5: bilateral chord diagram")


# ---------------------------------------------------------------------------
# Fig 6 — USA extraction origins
# ---------------------------------------------------------------------------
def fig06_usa_origins(foot):
    """Horizontal bar: USA ICT material shadow by extraction origin, latest year."""
    usa = foot[(foot["year"] == LATEST_YEAR) & (foot["consumption_region"] == "USA")]
    by_origin = (
        usa.groupby("extraction_region")["footprint_tonnes"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        / 1e6
    )
    by_origin = by_origin.iloc[::-1]

    fig, ax = plt.subplots(figsize=(7, 5))

    color_map = {
        "CHN": "#c0392b", "USA": "#2980b9",
        "CHL": "#e67e22", "AUS": "#e67e22", "PER": "#e67e22",
        "BRA": "#e67e22", "ZAF": "#e67e22", "IDN": "#e67e22",
    }
    colors = [color_map.get(r, "#95a5a6") for r in by_origin.index]

    ax.barh(by_origin.index, by_origin.values, color=colors, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Material extraction (Mt)")

    for i, (region, val) in enumerate(by_origin.items()):
        ax.text(val + 1, i, f"{val:,.0f}", va="center", fontsize=10)

    ax.set_xlim(0, by_origin.max() * 1.15)
    ax.xaxis.grid(True, alpha=0.2, linewidth=0.5, color="#cccccc")
    ax.set_axisbelow(True)
    fig.tight_layout()
    _save(fig, "fig06_usa_origins")
    print("  Fig 6: USA extraction origins")


# ===========================================================================
# Supplementary figures
# ===========================================================================

# ---------------------------------------------------------------------------
# Fig S1 — MFA composition over time (100% stacked bar)
# ---------------------------------------------------------------------------
def figS1_mfa_shares(foot):
    """100% stacked bar: MFA composition of ICT shadow over time."""
    pivot = (
        foot.groupby(["year", "mfa_category"])["footprint_tonnes"]
        .sum()
        .unstack(fill_value=0)
    )
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
    pivot_pct = pivot_pct[MFA_ORDER]

    fig, ax = plt.subplots(figsize=(8, 4))
    bottom = np.zeros(len(pivot_pct))
    bar_width = 3.5

    for cat in MFA_ORDER:
        ax.bar(pivot_pct.index, pivot_pct[cat], bottom=bottom, width=bar_width,
               color=MFA_COLORS[cat], label=cat, edgecolor="white", linewidth=0.5)
        bottom += pivot_pct[cat].values

    ax.set_xlabel("Year")
    ax.set_ylabel("Share of ICT material shadow (%)")
    ax.set_ylim(0, 100)
    ax.set_xlim(1987, LATEST_YEAR + 3)
    ax.set_xticks(pivot_pct.index)
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize=10, frameon=False)

    fig.tight_layout()
    _save(fig, "figS1_mfa_shares")
    print("  Fig S1: MFA composition shares")


# ---------------------------------------------------------------------------
# Fig S2 — China vs USA trajectory
# ---------------------------------------------------------------------------
def figS2_china_usa(foot):
    """Line chart: China vs USA ICT material shadow, 1990-2029."""
    focus = foot[foot["consumption_region"].isin(["CHN", "USA"])]
    pivot = (
        focus.groupby(["year", "consumption_region"])["footprint_tonnes"]
        .sum()
        .unstack(fill_value=0)
        / 1e9
    )

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(pivot.index, pivot["CHN"], "o-", color="#c0392b", linewidth=2.5,
            markersize=7, label="China")
    ax.plot(pivot.index, pivot["USA"], "s-", color="#2980b9", linewidth=2.5,
            markersize=7, label="United States")

    ax.axvspan(2000, 2010, alpha=0.08, color="grey")
    ax.annotate("Crossover\n~2003\u20132005", xy=(2005, 0.55), fontsize=10,
                ha="center", style="italic", color="#555555")

    ax.set_xlabel("Year")
    ax.set_ylabel("ICT material shadow (Gt)")
    ax.set_xlim(1988, LATEST_YEAR + 2)
    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))

    fig.tight_layout()
    _save(fig, "figS2_china_usa")
    print("  Fig S2: China vs USA trajectory")


# ---------------------------------------------------------------------------
# Fig S3 — Copper extraction geography over time
# ---------------------------------------------------------------------------
def figS3_copper_trajectory(foot):
    """Stacked area: copper extraction for ICT by top 5 countries, 1990-2029."""
    copper = foot[foot["mfa_subcategory"] == "Copper"]
    by_yr_origin = (
        copper.groupby(["year", "extraction_region"])["footprint_tonnes"]
        .sum()
        .reset_index()
    )

    top5_latest = (
        by_yr_origin[by_yr_origin["year"] == LATEST_YEAR]
        .nlargest(5, "footprint_tonnes")["extraction_region"]
        .tolist()
    )

    pivot = by_yr_origin.pivot_table(
        index="year", columns="extraction_region", values="footprint_tonnes", fill_value=0
    ) / 1e6
    other = pivot.drop(columns=top5_latest, errors="ignore").sum(axis=1)
    pivot_top = pivot[top5_latest].copy()
    pivot_top["Other"] = other

    colors_cu = ["#c0392b", "#2c3e50", "#e67e22", "#2980b9", "#27ae60", "#95a5a6"]

    # Map ISO codes to full country names for the legend
    iso_to_name = {
        "CHL": "Chile", "CHN": "China", "MEX": "Mexico",
        "PER": "Peru", "COD": "DRC", "AUS": "Australia",
        "BRA": "Brazil", "IDN": "Indonesia", "ZAF": "South Africa",
        "IND": "India", "RUS": "Russia", "USA": "United States",
    }

    fig, ax = plt.subplots(figsize=(8, 4.5))
    cols = top5_latest + ["Other"]
    labels_full = [iso_to_name.get(c, c) for c in cols]
    ax.stackplot(
        pivot_top.index,
        [pivot_top[c] for c in cols],
        labels=labels_full,
        colors=colors_cu[:len(cols)],
        alpha=0.85,
    )
    ax.set_xlabel("Year")
    ax.set_ylabel("Copper ore extraction for ICT (Mt)")
    ax.set_xlim(1990, LATEST_YEAR)
    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10, frameon=False)

    for y in [1990, LATEST_YEAR]:
        total = pivot_top.loc[y].sum()
        ax.annotate(f"{total:.0f} Mt", xy=(y, total), xytext=(0, 8),
                    textcoords="offset points", ha="center", fontsize=9, fontweight="bold")

    fig.tight_layout()
    _save(fig, "figS3_copper_trajectory")
    print("  Fig S3: copper trajectory")


# ---------------------------------------------------------------------------
# Fig S4 — Choropleth: net material balance
# ---------------------------------------------------------------------------
def figS4_choropleth(foot):
    """World choropleth: net ICT material balance, latest year (Robinson projection)."""
    f22 = foot[foot["year"] == LATEST_YEAR]
    cons = f22.groupby("consumption_region")["footprint_tonnes"].sum()
    extr = f22.groupby("extraction_region")["footprint_tonnes"].sum()
    all_reg = sorted(set(cons.index) | set(extr.index))
    balance = pd.Series(
        {r: cons.get(r, 0) - extr.get(r, 0) for r in all_reg}
    ) / 1e6

    world = gpd.read_file(NE_PATH)
    world = world.merge(
        balance.rename("net_balance").reset_index().rename(columns={"index": "ISO_A3"}),
        on="ISO_A3",
        how="left",
    )

    # Robinson projection
    robinson = "ESRI:54030"
    world_proj = world.to_crs(robinson)

    fig, ax = plt.subplots(figsize=(12, 6))
    world_proj.plot(
        column="net_balance",
        ax=ax,
        legend=True,
        cmap="RdBu_r",
        vmin=-230,
        vmax=280,
        missing_kwds={"color": "lightgrey", "edgecolor": "white", "linewidth": 0.3},
        edgecolor="white",
        linewidth=0.3,
        legend_kwds={
            "label": "Net ICT material balance (Mt)",
            "orientation": "horizontal",
            "shrink": 0.6,
            "pad": 0.02,
        },
    )
    ax.set_axis_off()

    # Ensure colorbar label fontsize
    cbar = fig.axes[-1]  # colorbar is the last axes
    cbar.tick_params(labelsize=11)
    for text in cbar.get_xticklabels() + cbar.get_yticklabels():
        text.set_fontsize(11)

    fig.tight_layout()
    _save(fig, "figS4_choropleth")
    print("  Fig S4: choropleth map")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  GENERATING FIGURES")
    print("=" * 60)

    foot, mult, regions = load_data()
    print(f"  Data loaded: {len(foot):,} footprint rows, {len(mult):,} multiplier rows")
    print()

    # Main-text figures
    fig01_temporal_stacked(foot)
    fig02_metal_ores(foot)
    fig03_geographic_bars(foot)
    fig04_net_balance(foot)
    fig05_bilateral_flows(foot)
    fig06_usa_origins(foot)

    # Supplementary figures
    print()
    print("  --- Supplementary ---")
    figS1_mfa_shares(foot)
    figS2_china_usa(foot)
    figS3_copper_trajectory(foot)
    figS4_choropleth(foot)

    print()
    print(f"  All figures saved to {FIG_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
