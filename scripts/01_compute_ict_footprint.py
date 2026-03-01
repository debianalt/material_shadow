"""
01_compute_ict_footprint.py — Material footprint of ICT via Leontief inverse
=============================================================================
Computes the full supply-chain material footprint of ICT sectors using GLORIA
MRIO Part I tables (Supply-Use Tables).

Strategy:
  1. Read T-matrix from GLORIA ZIP → extract SUT blocks (supply T_ip, use T_pi)
  2. Read Y-matrix (final demand) for product rows
  3. Read V-matrix (value added) for total output cross-check
  4. Read TQ satellite data (material extensions) for industry rows
  5. Compute Schur complement: S = I - B × D  (19,680 × 19,680)
     where B = input coefficients, D = market share matrix
  6. Solve Leontief system for ICT products → material multipliers
  7. Solve for consumption-based ICT footprints by origin region
  8. Save results as parquet

Dimensions: 164 regions × 120 sectors × 367 materials × 8 benchmark years

Author: Raimundo Elías Gómez (CONICET / University of Porto)
"""

import os
import re
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# --- USER CONFIG: set this to the root of your local GLORIA database ---
GLORIA_DIR = Path(os.environ.get("GLORIA_DIR", r"C:\Users\ant\OneDrive\mrio-2026"))
PART1_DIR = GLORIA_DIR / "part1_mrio_database"
TQ_DIR = GLORIA_DIR / "TQ"
META_DIR = GLORIA_DIR / "metadata"
_REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = _REPO / "data" / "baseline"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

N_REGIONS = 164
N_SECTORS = 120
N_PER_REGION = 240  # 120 industries + 120 products per region
N_TOTAL = N_REGIONS * N_PER_REGION  # 39,360
N_IND = N_REGIONS * N_SECTORS  # 19,680 (total industries)
N_FD_CATS = 6  # final demand categories per region
N_FD_COLS = N_REGIONS * N_FD_CATS  # 984

# ICT sector indices (1-based GLORIA sector numbers)
ICT_SECTORS = {
    90: "Computers, electronics, optical instruments",
    91: "Electrical equipment",
    109: "Publishing",
    110: "Telecommunications",
    111: "Information services",
}
ICT_SECTOR_IDS = sorted(ICT_SECTORS.keys())
N_ICT = len(ICT_SECTOR_IDS)

# Benchmark years
BENCHMARK_YEARS = [1990, 1995, 2000, 2005, 2010, 2015, 2022, 2029]


# ---------------------------------------------------------------------------
# Index mappings
# ---------------------------------------------------------------------------
def build_index_maps():
    """Build mappings between full T-matrix indices and subspace indices.

    In the T-matrix (39,360 × 39,360), each region occupies 240 rows/cols:
      - rows [r*240 .. r*240+119]: industries for region r
      - rows [r*240+120 .. r*240+239]: products for region r

    The subspace indices (19,680) re-index to [r*120 .. r*120+119].
    """
    ind_full = np.zeros(N_IND, dtype=np.int32)
    prod_full = np.zeros(N_IND, dtype=np.int32)
    for r in range(N_REGIONS):
        for s in range(N_SECTORS):
            sub_idx = r * N_SECTORS + s
            ind_full[sub_idx] = r * N_PER_REGION + s
            prod_full[sub_idx] = r * N_PER_REGION + N_SECTORS + s

    # Reverse mappings: full index → subspace index
    full_to_ind = np.full(N_TOTAL, -1, dtype=np.int32)
    full_to_prod = np.full(N_TOTAL, -1, dtype=np.int32)
    for k in range(N_IND):
        full_to_ind[ind_full[k]] = k
        full_to_prod[prod_full[k]] = k

    return ind_full, prod_full, full_to_ind, full_to_prod


def get_ict_product_indices():
    """Get subspace indices for ICT product rows across all regions.

    Returns dict: {(region_idx, sector_1based): subspace_idx}
    """
    indices = {}
    for r in range(N_REGIONS):
        for s1 in ICT_SECTOR_IDS:
            s0 = s1 - 1  # 0-based
            sub_idx = r * N_SECTORS + s0
            indices[(r, s1)] = sub_idx
    return indices


# ---------------------------------------------------------------------------
# Load metadata
# ---------------------------------------------------------------------------
def load_metadata():
    """Load GLORIA region, sector, and satellite metadata."""
    regions = pd.read_parquet(META_DIR / "regions.parquet")
    sectors = pd.read_parquet(META_DIR / "sectors.parquet")
    satellites = pd.read_parquet(META_DIR / "satellites.parquet")
    seq_labels = pd.read_parquet(META_DIR / "sequential_labels.parquet")

    # Extract region ordering from sequential labels
    region_order = []
    for i in range(0, len(seq_labels), N_PER_REGION):
        label = seq_labels.iloc[i]["sequential_regionsector_labels"]
        match = re.search(r"\(([A-Z]{3})\)", label)
        if match:
            region_order.append(match.group(1))

    # Material satellites (367 indicators, lfd_nr 1-367)
    mat_sats = satellites[satellites["sat_head_indicator"] == "Material"].copy()
    mat_sats[["mfa_category", "mfa_subcategory"]] = mat_sats.apply(
        lambda r: pd.Series(classify_material(r["sat_indicator"], r["lfd_nr"])),
        axis=1,
    )

    return regions, sectors, mat_sats, region_order


def classify_material(indicator, lfd_nr):
    """Classify a material indicator into MFA category and subcategory."""
    ind = indicator.lower()

    if lfd_nr >= 358:
        if any(k in ind for k in ("coal", "lignite", "anthracite", "bituminous")):
            return "Fossil fuels", "Coal"
        elif any(k in ind for k in ("oil", "petroleum", "tar")):
            return "Fossil fuels", "Oil"
        else:
            return "Fossil fuels", "Gas and peat"

    if 329 <= lfd_nr <= 343:
        if "iron" in ind:
            return "Metal ores", "Iron"
        elif "aluminium" in ind or "bauxite" in ind:
            return "Metal ores", "Aluminium"
        elif "copper" in ind:
            return "Metal ores", "Copper"
        elif "gold" in ind:
            return "Metal ores", "Gold"
        else:
            return "Metal ores", "Other metals"

    if 344 <= lfd_nr <= 357:
        if any(k in ind for k in ("sand", "gravel", "crushed", "stone")):
            return "Non-metallic minerals", "Construction minerals"
        else:
            return "Non-metallic minerals", "Industrial minerals"

    # Biomass (lfd_nr 1-328)
    if "residue" in ind:
        return "Biomass", "Crop residues"
    elif "timber" in ind or "wood" in ind:
        return "Biomass", "Wood"
    elif "fish" in ind or "aquatic" in ind:
        return "Biomass", "Fishery and aquatic"
    elif "grazed" in ind:
        return "Biomass", "Grazed biomass"
    else:
        return "Biomass", "Crops"


# ---------------------------------------------------------------------------
# Read matrices from GLORIA ZIP
# ---------------------------------------------------------------------------
def read_sut_blocks(zip_path, full_to_ind, full_to_prod):
    """Read Supply (T_ip) and Use (T_pi) blocks from T-matrix in GLORIA ZIP.

    T_ip (supply): industry → product flows (block diagonal, very sparse)
    T_pi (use):    product → industry flows (includes international trade)

    Both returned as sparse CSC matrices of shape (N_IND, N_IND).
    """
    print(f"  Reading T-matrix SUT blocks from {zip_path.name}...")
    t0 = time.time()

    with zipfile.ZipFile(zip_path) as zf:
        t_file = [n for n in zf.namelist()
                  if "T-Results" in n and "Markup001" in n][0]

        # Accumulate sparse data in chunks
        ip_rows, ip_cols, ip_vals = [], [], []
        pi_rows, pi_cols, pi_vals = [], [], []

        with zf.open(t_file) as f:
            for row_full, line in enumerate(f):
                arr = np.fromstring(line, sep=",", dtype=np.float64)
                within = row_full % N_PER_REGION

                if within < N_SECTORS:
                    # Industry row → extract product columns (supply block)
                    row_sub = (row_full // N_PER_REGION) * N_SECTORS + within
                    nz_full = np.nonzero(arr)[0]
                    if len(nz_full) > 0:
                        nz_sub = full_to_prod[nz_full]
                        valid = nz_sub >= 0
                        n_valid = valid.sum()
                        if n_valid > 0:
                            ip_rows.append(np.full(n_valid, row_sub, dtype=np.int32))
                            ip_cols.append(nz_sub[valid].astype(np.int32))
                            ip_vals.append(arr[nz_full[valid]])
                else:
                    # Product row → extract industry columns (use block)
                    row_sub = (row_full // N_PER_REGION) * N_SECTORS + (within - N_SECTORS)
                    nz_full = np.nonzero(arr)[0]
                    if len(nz_full) > 0:
                        nz_sub = full_to_ind[nz_full]
                        valid = nz_sub >= 0
                        n_valid = valid.sum()
                        if n_valid > 0:
                            pi_rows.append(np.full(n_valid, row_sub, dtype=np.int32))
                            pi_cols.append(nz_sub[valid].astype(np.int32))
                            pi_vals.append(arr[nz_full[valid]])

                if row_full % 10000 == 0:
                    elapsed = time.time() - t0
                    pct = 100 * row_full / N_TOTAL
                    print(f"    Row {row_full:,}/{N_TOTAL:,} ({pct:.0f}%) — {elapsed:.0f}s")

    T_ip = sparse.csc_matrix(
        (np.concatenate(ip_vals), (np.concatenate(ip_rows), np.concatenate(ip_cols))),
        shape=(N_IND, N_IND),
    )
    T_pi = sparse.csc_matrix(
        (np.concatenate(pi_vals), (np.concatenate(pi_rows), np.concatenate(pi_cols))),
        shape=(N_IND, N_IND),
    )

    elapsed = time.time() - t0
    print(f"  T_ip: {T_ip.nnz:,} non-zeros | T_pi: {T_pi.nnz:,} non-zeros | {elapsed:.0f}s")
    return T_ip, T_pi


def read_y_matrix(zip_path, prod_full):
    """Read Y-matrix (final demand) from GLORIA ZIP.

    Returns Y_prod: product rows only, shape (N_IND, N_FD_COLS).
    """
    print(f"  Reading Y-matrix...")
    t0 = time.time()

    Y_full = []
    with zipfile.ZipFile(zip_path) as zf:
        y_file = [n for n in zf.namelist()
                  if "Y-Results" in n and "Markup001" in n][0]
        with zf.open(y_file) as f:
            for line in f:
                arr = np.fromstring(line, sep=",", dtype=np.float64)
                Y_full.append(arr)

    Y_full = np.vstack(Y_full)
    # Extract product rows only
    Y_prod = Y_full[prod_full, :]
    elapsed = time.time() - t0
    print(f"  Y-matrix: {Y_full.shape} -> Y_prod: {Y_prod.shape} | {elapsed:.0f}s")
    return Y_prod


def read_v_matrix(zip_path):
    """Read V-matrix (value added) from GLORIA ZIP.

    Returns V: shape (n_va_categories, N_TOTAL).
    """
    print(f"  Reading V-matrix...")
    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        v_file = [n for n in zf.namelist()
                  if "V-Results" in n and "Markup001" in n][0]
        with zf.open(v_file) as f:
            for line in f:
                arr = np.fromstring(line, sep=",", dtype=np.float64)
                rows.append(arr)
    V = np.vstack(rows)
    print(f"  V-matrix: {V.shape}")
    return V


def read_tq_materials(year, mat_sat_ids, ind_full):
    """Read material satellite data from TQ parquets for a given year.

    Returns S_mat: shape (n_materials, N_IND) — material extraction by industry.
    """
    print(f"  Reading TQ material satellites for {year}...")
    tq_path = TQ_DIR / f"year={year}" / "data.parquet"
    tq = pd.read_parquet(tq_path)

    # Material rows: sat_id matches mat_sat_ids (1-based lfd_nr)
    tq_mat = tq[tq["sat_id"].isin(mat_sat_ids)].sort_values("sat_id")

    # Extract industry columns (using full-system column names)
    ind_col_names = [f"c{i}" for i in ind_full]
    S_mat = tq_mat[ind_col_names].values.astype(np.float64)

    print(f"  TQ materials: {S_mat.shape} ({S_mat.shape[0]} indicators × {S_mat.shape[1]} industries)")
    return S_mat


# ---------------------------------------------------------------------------
# Leontief computation via Schur complement
# ---------------------------------------------------------------------------
def neumann_solve(B_dense, D_sparse, Y_rhs, tol=1e-4, max_iter=200):
    """Solve (I - B*D) * Z = Y using Neumann series iteration.

    Z_{k+1} = Y + B * (D * Z_k)

    Convergence guaranteed when spectral radius of BD < 1 (Hawkins-Simon
    condition, satisfied for all viable Leontief systems).

    Parameters
    ----------
    B_dense : ndarray (N_IND, N_IND) — input coefficient matrix (dense, ~92% fill)
    D_sparse : sparse (N_IND, N_IND) — market share matrix (very sparse, ~19K nnz)
    Y_rhs : ndarray (N_IND,) or (N_IND, k) — right-hand side(s)
    tol : float — convergence tolerance (relative norm change)
    max_iter : int — maximum iterations

    Returns
    -------
    Z : ndarray — solution(s) with same shape as Y_rhs
    """
    Z = Y_rhs.copy().astype(np.float64)
    for k in range(max_iter):
        DZ = D_sparse @ Z         # sparse x dense -> fast
        BDZ = B_dense @ DZ        # dense x dense -> BLAS-optimised
        Z_new = Y_rhs + BDZ

        # Convergence check
        diff_norm = np.linalg.norm(Z_new - Z)
        z_norm = np.linalg.norm(Z_new) + 1e-30
        rel_diff = diff_norm / z_norm

        if rel_diff < tol:
            print(f"    Neumann converged in {k + 1} iterations (rel diff: {rel_diff:.2e})")
            return Z_new
        Z = Z_new
        if (k + 1) % 5 == 0:
            print(f"    Iteration {k + 1}: rel diff = {rel_diff:.2e}")

    print(f"    WARNING: Neumann did not converge in {max_iter} iterations "
          f"(rel diff: {rel_diff:.2e})")
    return Z


def compute_schur_and_solve(T_ip, T_pi, Y_prod, S_mat, region_order, mat_sats):
    """Compute material footprint of ICT using the Schur complement method.

    The full SUT system (I - A) has block structure:
        | I     -D  |   where D = T_ip / q  (market shares, very sparse)
        | -B     I  |         B = T_pi / g  (input coefficients, ~92% dense)

    Schur complement: S = I - B * D  (product-by-product Leontief system)
    For product demand y_p:  z_p = S^{-1} * y_p  (product outputs)
                             z_i = D * z_p        (industry outputs)

    Material footprint: MF = s_i * z_i  where s_i = TQ / g (material intensity)

    Uses Neumann series iteration (guaranteed convergence for Leontief systems)
    instead of sparse LU, which exceeds memory for this matrix size.
    """
    import gc

    print("\n[Leontief] Computing technical coefficients...")

    # Industry output: g = row sums of supply matrix
    g = np.array(T_ip.sum(axis=1)).flatten()
    # Product output: q = row sums of use matrix + total final demand per product
    q = np.array(T_pi.sum(axis=1)).flatten() + Y_prod.sum(axis=1)

    print(f"  Industry output: min={g.min():.0f}, max={g.max():.0f}, "
          f"zeros={np.sum(g == 0)}/{N_IND}")
    print(f"  Product output:  min={q.min():.0f}, max={q.max():.0f}, "
          f"zeros={np.sum(q == 0)}/{N_IND}")

    # Safe reciprocals (suppress warnings for known zeros)
    with np.errstate(divide="ignore"):
        g_inv = np.where(g > 0, 1.0 / g, 0.0)
        q_inv = np.where(q > 0, 1.0 / q, 0.0)

    # D = T_ip * diag(1/q): market share matrix (very sparse: ~19K nnz)
    D_sparse = T_ip @ sparse.diags(q_inv)
    # B = T_pi * diag(1/g): input coefficient matrix (~92% dense)
    B_sparse = T_pi @ sparse.diags(g_inv)

    print(f"  D (market shares): {D_sparse.nnz:,} non-zeros "
          f"({100 * D_sparse.nnz / N_IND**2:.2f}% fill)")
    print(f"  B (input coeffs):  {B_sparse.nnz:,} non-zeros "
          f"({100 * B_sparse.nnz / N_IND**2:.1f}% fill)")

    # Convert B to dense (92% fill rate -> dense is smaller and faster than sparse)
    print("[Leontief] Converting B to dense (faster than sparse at 92% fill)...")
    t0 = time.time()
    B_dense = B_sparse.toarray()
    del B_sparse, T_ip, T_pi  # free sparse matrices
    gc.collect()
    print(f"  B dense: {B_dense.nbytes / 1e9:.2f} GB | {time.time() - t0:.0f}s")

    # Material intensity: s[k, i] = TQ[k, i] / g[i]
    s_intensity = S_mat * g_inv[np.newaxis, :]

    # --- Phase 1: ICT multipliers (unit demand per product) ---
    print("\n[Phase 1] Computing ICT material multipliers via Neumann series...")
    ict_prod_idx = get_ict_product_indices()
    n_ict_total = len(ict_prod_idx)

    # Build RHS: identity columns for each ICT product
    E_ict = np.zeros((N_IND, n_ict_total))
    ict_keys = sorted(ict_prod_idx.keys())
    for k, key in enumerate(ict_keys):
        E_ict[ict_prod_idx[key], k] = 1.0

    t0 = time.time()
    Z_p = neumann_solve(B_dense, D_sparse, E_ict)
    print(f"  Phase 1 solve: {time.time() - t0:.0f}s")

    # Industry output induced by ICT product demand
    Z_i = D_sparse @ Z_p  # sparse x dense -> dense

    # Material multipliers: tonnes per unit of ICT product output
    M_ict = s_intensity @ Z_i  # (n_materials, n_ict_total)
    print(f"  Multiplier matrix: {M_ict.shape}")

    # --- Phase 2: Consumption-based ICT footprints ---
    print("\n[Phase 2] Computing consumption-based ICT footprints...")
    n_materials = S_mat.shape[0]

    # Build ICT-only final demand matrix: one column per consumption region
    # Y_prod: (N_IND, N_FD_COLS), columns c*6..(c+1)*6 = region c's FD
    # ICT product indices (all regions, ICT sectors)
    ict_product_mask = np.zeros(N_IND, dtype=bool)
    for s1 in ICT_SECTOR_IDS:
        for r in range(N_REGIONS):
            ict_product_mask[r * N_SECTORS + (s1 - 1)] = True

    Y_ict_by_region = np.zeros((N_IND, N_REGIONS))
    ict_fd_totals = np.zeros(N_REGIONS)
    for c in range(N_REGIONS):
        y_c = Y_prod[:, c * N_FD_CATS:(c + 1) * N_FD_CATS].sum(axis=1)
        y_c_ict = y_c * ict_product_mask
        Y_ict_by_region[:, c] = y_c_ict
        ict_fd_totals[c] = y_c_ict.sum()

    # Remove regions with zero ICT final demand
    active = ict_fd_totals > 0
    n_active = active.sum()
    print(f"  Active consumption regions: {n_active}/{N_REGIONS}")

    if n_active > 0:
        Y_active = Y_ict_by_region[:, active]
        t0 = time.time()
        Z_p_cons = neumann_solve(B_dense, D_sparse, Y_active)
        print(f"  Phase 2 solve: {time.time() - t0:.0f}s")

        Z_i_cons = D_sparse @ Z_p_cons  # industry outputs

        # Material footprint by extraction origin (aggregated over sectors)
        # For each consumption region and extraction region, sum material
        # extraction across sectors
        results = []
        active_indices = np.where(active)[0]
        for col_k, c in enumerate(active_indices):
            z_i_c = Z_i_cons[:, col_k]
            for r_origin in range(N_REGIONS):
                start = r_origin * N_SECTORS
                end = start + N_SECTORS
                z_i_region = z_i_c[start:end]
                if z_i_region.sum() == 0:
                    continue
                s_region = s_intensity[:, start:end]
                mf_region = (s_region * z_i_region[np.newaxis, :]).sum(axis=1)
                total_mf = mf_region.sum()
                if total_mf > 0:
                    results.append({
                        "consumption_region_idx": c,
                        "extraction_region_idx": r_origin,
                        "ict_final_demand": ict_fd_totals[c],
                        **{f"mat_{m}": mf_region[m] for m in range(n_materials)},
                    })

            if (col_k + 1) % 20 == 0:
                print(f"    Processed {col_k + 1}/{n_active} consumption regions")
    else:
        results = []

    print(f"  Generated {len(results)} origin-destination-material entries")

    return M_ict, ict_keys, results, g, q


# ---------------------------------------------------------------------------
# Build tidy output DataFrames
# ---------------------------------------------------------------------------
def build_multiplier_df(M_ict, ict_keys, region_order, mat_sats, year):
    """Build a tidy DataFrame of ICT material multipliers."""
    records = []
    mat_list = mat_sats.sort_values("lfd_nr")

    for k, (r_idx, s1) in enumerate(ict_keys):
        for m in range(len(mat_list)):
            val = M_ict[m, k]
            if val > 0:
                row = mat_list.iloc[m]
                records.append({
                    "year": year,
                    "region_idx": r_idx,
                    "region": region_order[r_idx],
                    "ict_sector": s1,
                    "ict_sector_name": ICT_SECTORS[s1],
                    "material_id": int(row["lfd_nr"]),
                    "material_name": row["sat_indicator"],
                    "mfa_category": row["mfa_category"],
                    "mfa_subcategory": row["mfa_subcategory"],
                    "multiplier": val,
                })

    return pd.DataFrame(records)


def build_footprint_df(results, region_order, mat_sats, year):
    """Build a tidy DataFrame of consumption-based ICT footprints."""
    n_materials = len(mat_sats)
    mat_list = mat_sats.sort_values("lfd_nr")

    records = []
    for entry in results:
        c_idx = entry["consumption_region_idx"]
        r_idx = entry["extraction_region_idx"]
        for m in range(n_materials):
            val = entry[f"mat_{m}"]
            if val > 0:
                row = mat_list.iloc[m]
                records.append({
                    "year": year,
                    "consumption_region": region_order[c_idx],
                    "extraction_region": region_order[r_idx],
                    "material_id": int(row["lfd_nr"]),
                    "material_name": row["sat_indicator"],
                    "mfa_category": row["mfa_category"],
                    "mfa_subcategory": row["mfa_subcategory"],
                    "footprint_tonnes": val,
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def process_year(year, ind_full, prod_full, full_to_ind, full_to_prod,
                 region_order, mat_sats):
    """Full Leontief computation pipeline for a single year."""
    print(f"\n{'=' * 70}")
    print(f"  PROCESSING YEAR {year}")
    print(f"{'=' * 70}")

    zip_path = PART1_DIR / f"GLORIA_MRIOs_60_{year}.zip"
    if not zip_path.exists():
        print(f"  WARNING: ZIP not found for {year}, skipping")
        return None, None

    t_year_start = time.time()

    # 1. Read SUT blocks
    T_ip, T_pi = read_sut_blocks(zip_path, full_to_ind, full_to_prod)

    # 2. Read Y-matrix
    Y_prod = read_y_matrix(zip_path, prod_full)

    # 3. Read TQ satellite data
    mat_sat_ids = mat_sats["lfd_nr"].values
    S_mat = read_tq_materials(year, mat_sat_ids, ind_full)

    # 4. Compute Leontief and footprints
    M_ict, ict_keys, results, g, q = compute_schur_and_solve(
        T_ip, T_pi, Y_prod, S_mat, region_order, mat_sats
    )

    # 5. Build DataFrames
    mult_df = build_multiplier_df(M_ict, ict_keys, region_order, mat_sats, year)
    foot_df = build_footprint_df(results, region_order, mat_sats, year)

    elapsed = time.time() - t_year_start
    print(f"\n  Year {year} complete: {elapsed / 60:.1f} min")
    print(f"  Multipliers: {len(mult_df):,} rows | Footprints: {len(foot_df):,} rows")

    return mult_df, foot_df


def main():
    """Run the full pipeline across benchmark years."""
    print("=" * 70)
    print("  ICT MATERIAL FOOTPRINT — LEONTIEF INVERSE COMPUTATION")
    print("  GLORIA MRIO, 164 regions × 120 sectors × 367 materials")
    print("=" * 70)

    # Build index mappings
    print("\n[Setup] Building index mappings...")
    ind_full, prod_full, full_to_ind, full_to_prod = build_index_maps()

    # Load metadata
    print("[Setup] Loading metadata...")
    regions, sectors, mat_sats, region_order = load_metadata()
    print(f"  Regions: {len(region_order)} | Sectors: {len(sectors)} | "
          f"Material satellites: {len(mat_sats)}")

    # Process years
    years_to_process = [y for y in BENCHMARK_YEARS]
    print(f"\n[Pipeline] Processing {len(years_to_process)} benchmark years: "
          f"{years_to_process}")

    all_mult = []
    all_foot = []

    for year in years_to_process:
        mult_df, foot_df = process_year(
            year, ind_full, prod_full, full_to_ind, full_to_prod,
            region_order, mat_sats,
        )
        if mult_df is not None:
            all_mult.append(mult_df)
            all_foot.append(foot_df)

            # Save intermediate results
            mult_df.to_parquet(OUTPUT_DIR / f"ict_multipliers_{year}.parquet",
                               index=False)
            foot_df.to_parquet(OUTPUT_DIR / f"ict_footprint_{year}.parquet",
                               index=False)
            print(f"  Saved: ict_multipliers_{year}.parquet, ict_footprint_{year}.parquet")

    # Combine all years
    if all_mult:
        combined_mult = pd.concat(all_mult, ignore_index=True)
        combined_foot = pd.concat(all_foot, ignore_index=True)
        combined_mult.to_parquet(OUTPUT_DIR / "ict_multipliers_all.parquet",
                                 index=False)
        combined_foot.to_parquet(OUTPUT_DIR / "ict_footprint_all.parquet",
                                 index=False)
        print(f"\n  Combined multipliers: {len(combined_mult):,} rows")
        print(f"  Combined footprints: {len(combined_foot):,} rows")

    # Save material classification and region order to shared data directory
    shared_dir = _REPO / "data"
    mat_sats.to_parquet(shared_dir / "material_classification.parquet", index=False)

    pd.DataFrame({
        "region_idx": range(len(region_order)),
        "region_acronym": region_order,
    }).to_parquet(shared_dir / "region_order.parquet", index=False)

    print("\n" + "=" * 70)
    print("  PIPELINE COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    # Allow running a single year from command line
    if len(sys.argv) > 1:
        BENCHMARK_YEARS = [int(y) for y in sys.argv[1:]]
    main()
