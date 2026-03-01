"""
03_sensitivity_comparison.py — Compare baseline vs ex91 sensitivity results
===========================================================================
Reads the baseline and ex91 parquets and computes comparison metrics:
1. Global total by year: baseline vs ex91 (reduction %)
2. MFA category shares (2029): do proportions shift?
3. Top 10 consumer/extractor rankings: same countries? same order?
4. Net balance signs: preserved for all major economies?
5. Copper share of metal ores: change without sector 91?

Author: Raimundo Elías Gómez (CONICET / University of Porto)
"""

from pathlib import Path
import pandas as pd
import numpy as np

_REPO = Path(__file__).resolve().parent.parent
DATA_DIR = _REPO / "data"

YEARS = [1990, 1995, 2000, 2005, 2010, 2015, 2022, 2029]


def load_footprint(suffix=""):
    """Load footprint data by concatenating per-year parquets."""
    if suffix == "_ex91":
        subdir = DATA_DIR / "sensitivity_ex91"
        pattern = "ict_footprint_*_ex91.parquet"
    else:
        subdir = DATA_DIR / "baseline"
        pattern = "ict_footprint_*.parquet"
    files = sorted(subdir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No footprint parquets found in {subdir}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def compare_global_totals(base, ex91):
    """Compare global totals by year."""
    print("\n" + "=" * 70)
    print("  1. GLOBAL TOTALS BY YEAR (Gt)")
    print("=" * 70)

    base_yr = base.groupby("year")["footprint_tonnes"].sum() / 1e9
    ex91_yr = ex91.groupby("year")["footprint_tonnes"].sum() / 1e9

    print(f"{'Year':>6} {'Baseline':>10} {'Ex91':>10} {'Reduction':>10} {'% Red':>8}")
    print("-" * 50)
    for y in YEARS:
        b = base_yr.get(y, 0)
        e = ex91_yr.get(y, 0)
        red = b - e
        pct = 100 * red / b if b > 0 else 0
        print(f"{y:>6} {b:>10.2f} {e:>10.2f} {red:>10.2f} {pct:>7.1f}%")

    return base_yr, ex91_yr


def compare_mfa_shares(base, ex91, year=2029):
    """Compare MFA category shares for a given year."""
    print(f"\n{'=' * 70}")
    print(f"  2. MFA CATEGORY SHARES — {year} (Gt and %)")
    print("=" * 70)

    for label, df in [("Baseline", base), ("Ex91", ex91)]:
        yr_df = df[df["year"] == year]
        by_cat = yr_df.groupby("mfa_category")["footprint_tonnes"].sum() / 1e9
        total = by_cat.sum()
        print(f"\n  {label} (total: {total:.2f} Gt):")
        for cat in ["Non-metallic minerals", "Metal ores", "Fossil fuels", "Biomass"]:
            val = by_cat.get(cat, 0)
            pct = 100 * val / total if total > 0 else 0
            print(f"    {cat:<25} {val:>6.2f} Gt  ({pct:>5.1f}%)")


def compare_rankings(base, ex91, year=2029, top_n=10):
    """Compare top consumer and extractor rankings."""
    print(f"\n{'=' * 70}")
    print(f"  3. TOP {top_n} RANKINGS — {year}")
    print("=" * 70)

    for label, col_name in [("CONSUMERS", "consumption_region"), ("EXTRACTORS", "extraction_region")]:
        print(f"\n  {label}:")
        print(f"  {'Rank':>4} {'Baseline':>8} {'Mt':>8} {'Ex91':>8} {'Mt':>8}")
        print("  " + "-" * 42)

        base_yr = base[base["year"] == year]
        ex91_yr = ex91[ex91["year"] == year]

        base_rank = base_yr.groupby(col_name)["footprint_tonnes"].sum().sort_values(ascending=False) / 1e6
        ex91_rank = ex91_yr.groupby(col_name)["footprint_tonnes"].sum().sort_values(ascending=False) / 1e6

        for i in range(min(top_n, len(base_rank))):
            b_country = base_rank.index[i]
            b_val = base_rank.iloc[i]
            e_country = ex91_rank.index[i] if i < len(ex91_rank) else "—"
            e_val = ex91_rank.iloc[i] if i < len(ex91_rank) else 0
            print(f"  {i+1:>4} {b_country:>8} {b_val:>8.0f} {e_country:>8} {e_val:>8.0f}")


def compare_net_balances(base, ex91, year=2029):
    """Check whether net balance signs are preserved."""
    print(f"\n{'=' * 70}")
    print(f"  4. NET BALANCE SIGNS — {year}")
    print("=" * 70)

    for label, df in [("Baseline", base), ("Ex91", ex91)]:
        yr_df = df[df["year"] == year]
        cons = yr_df.groupby("consumption_region")["footprint_tonnes"].sum()
        extr = yr_df.groupby("extraction_region")["footprint_tonnes"].sum()

        all_regions = sorted(set(cons.index) | set(extr.index))
        net = pd.Series({r: cons.get(r, 0) - extr.get(r, 0) for r in all_regions})

        if label == "Baseline":
            base_net = net
        else:
            ex91_net = net

    # Check sign changes
    common = sorted(set(base_net.index) & set(ex91_net.index))
    sign_changes = []
    for r in common:
        b_sign = np.sign(base_net[r])
        e_sign = np.sign(ex91_net[r])
        if b_sign != e_sign and b_sign != 0 and e_sign != 0:
            sign_changes.append((r, base_net[r] / 1e6, ex91_net[r] / 1e6))

    if sign_changes:
        print(f"\n  Sign changes detected ({len(sign_changes)}):")
        for r, b, e in sign_changes:
            print(f"    {r}: baseline={b:+.0f} Mt -> ex91={e:+.0f} Mt")
    else:
        print("\n  No sign changes in net balance for any economy.")

    # Show top importers/exporters
    print("\n  Top 5 net importers (baseline -> ex91, Mt):")
    top_imp = base_net.sort_values(ascending=False).head(5)
    for r in top_imp.index:
        b = base_net[r] / 1e6
        e = ex91_net.get(r, 0) / 1e6
        print(f"    {r}: {b:>+8.0f} -> {e:>+8.0f}")

    print("\n  Top 5 net exporters (baseline -> ex91, Mt):")
    top_exp = base_net.sort_values().head(5)
    for r in top_exp.index:
        b = base_net[r] / 1e6
        e = ex91_net.get(r, 0) / 1e6
        print(f"    {r}: {b:>+8.0f} -> {e:>+8.0f}")


def compare_copper(base, ex91, year=2029):
    """Compare copper share of metal ores."""
    print(f"\n{'=' * 70}")
    print(f"  5. COPPER SHARE OF METAL ORES — {year}")
    print("=" * 70)

    for label, df in [("Baseline", base), ("Ex91", ex91)]:
        yr_df = df[df["year"] == year]
        metals = yr_df[yr_df["mfa_category"] == "Metal ores"]
        total_metals = metals["footprint_tonnes"].sum() / 1e6
        copper = metals[metals["mfa_subcategory"] == "Copper"]["footprint_tonnes"].sum() / 1e6
        pct = 100 * copper / total_metals if total_metals > 0 else 0
        print(f"  {label}: copper={copper:.0f} Mt / metals={total_metals:.0f} Mt ({pct:.1f}%)")


def main():
    print("Loading baseline footprint data...")
    base = load_footprint("")
    print(f"  Baseline: {len(base):,} rows")

    print("Loading ex91 footprint data...")
    ex91 = load_footprint("_ex91")
    print(f"  Ex91: {len(ex91):,} rows")

    compare_global_totals(base, ex91)
    compare_mfa_shares(base, ex91, year=2029)
    compare_rankings(base, ex91, year=2029)
    compare_net_balances(base, ex91, year=2029)
    compare_copper(base, ex91, year=2029)

    print("\n" + "=" * 70)
    print("  COMPARISON COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
