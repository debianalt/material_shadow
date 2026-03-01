# The Material Shadow of the Digital Economy

Tracing the supply-chain material footprint of ICT across 164 economies, 1990–2029.

**Author:** Raimundo Elías Gómez
**Affiliations:** CONICET / National University of Misiones (Argentina); Institute of Sociology, University of Porto (Portugal)
**Contact:** rgomez@letras.up.pt
**ORCID:** 0000-0002-4468-9618

## Overview

Five ICT sectors — computers and electronics, electrical equipment, publishing, telecommunications, and information services — extract precisely zero tonnes of raw material, yet their supply chains mobilise over 4 gigatonnes of upstream material extraction globally. This repository implements a consumption-based material footprint analysis using the Leontief inverse applied to the GLORIA multi-regional input–output database (164 economies, 120 sectors, 367 material indicators, 8 benchmark years from 1990 to 2029).

## Repository Structure

```
material_shadow/
├── data/
│   ├── baseline/                        # Footprint and multiplier parquets (5-sector ICT)
│   ├── sensitivity_ex91/                # Sensitivity analysis excluding sector 91
│   ├── material_classification.parquet  # 367 material indicators with MFA classification
│   ├── region_order.parquet             # 164 GLORIA region codes
│   ├── ne_110m_admin_0_countries.zip    # Natural Earth shapefile for choropleth maps
│   └── wdi_panel.parquet               # World Development Indicators panel
├── figures/                             # All figures (PNG, 300 dpi)
├── scripts/
│   ├── 01_compute_ict_footprint.py      # Leontief inverse via Schur complement + Neumann series
│   ├── 01b_sensitivity_ex91.py          # Sensitivity: excluding sector 91 (Electrical equipment)
│   ├── 02_analysis_figures.py           # Generate all figures (6 main + 4 supplementary)
│   └── 03_sensitivity_comparison.py     # Baseline vs ex91 comparison metrics
├── requirements.txt
├── LICENSE
└── README.md
```

## Data Sources

| Dataset | Source | Period | Description |
|---------|--------|--------|-------------|
| Material extraction | GLORIA MRIO (Loop060) satellite accounts | 1990–2029 | Supply-chain material footprint by country, sector, and material |
| Shapefile | Natural Earth | — | 1:110m country boundaries |
| WDI | World Bank | 1990–2022 | GDP, population, urbanisation |

The GLORIA MRIO database is available from the Industrial Ecology Virtual Laboratory (https://ielab.info; Lenzen et al., 2017; 2022). The full database (~60 GB) is required only for scripts 01/01b; pre-computed outputs for scripts 02–03 are included in `data/`.

## Methodology

### 1. Leontief Inverse Computation (01_compute_ict_footprint.py)

Reads Supply-Use Tables from GLORIA ZIP archives and computes the product-by-product Leontief inverse via the Schur complement. The full SUT system has block structure:

```
| I    -D |   where D = market share matrix (very sparse, ~19K nnz)
| -B    I |         B = input coefficient matrix (~92% dense, 3.1 GB)
```

The Schur complement S = I − B×D is solved via Neumann series iteration (convergence ~10⁻⁴ in 30–130 iterations), avoiding direct factorisation of the 19,680 × 19,680 system. Material multipliers and consumption-based footprints are computed for all 164 regions across 8 benchmark years.

### 2. Sensitivity Analysis (01b_sensitivity_ex91.py)

Identical pipeline excluding sector 91 (Electrical equipment, ISIC C27), the broadest of the five ICT sectors. Tests whether geographic asymmetry, copper dominance, and temporal trajectory are robust to sector definition.

### 3. Figures (02_analysis_figures.py)

Generates 10 publication-quality figures:

| Figure | Content |
|--------|---------|
| Fig 1 | Global ICT material shadow by MFA category, 1990–2029 (stacked area) |
| Fig 2 | Metal ore subcategories, 2029 (horizontal bar) |
| Fig 3 | Geographic distribution: butterfly chart — consumption vs extraction |
| Fig 4 | Net importers vs exporters, 2029 (diverging bar) |
| Fig 5 | Bilateral flows: chord diagram — top supply-chain corridors |
| Fig 6 | US ICT material shadow by extraction origin (bar) |
| Fig S1 | MFA category shares over time (100% stacked bar) |
| Fig S2 | China vs USA temporal trajectory |
| Fig S3 | Copper extraction by country of origin (stacked area) |
| Fig S4 | Choropleth: ICT material shadow net balance (world map) |

### 4. Sensitivity Comparison (03_sensitivity_comparison.py)

Compares baseline vs ex91 results: global totals, MFA shares, consumer/extractor rankings, net balance signs, and copper share of metal ores.

## Quick Start

### From pre-computed data

```bash
pip install -r requirements.txt
cd scripts
python 02_analysis_figures.py
python 03_sensitivity_comparison.py
```

Figures are saved to `figures/` in PNG and PDF formats.

### From GLORIA source data

```bash
export GLORIA_DIR=/path/to/gloria-mrio
cd scripts
python 01_compute_ict_footprint.py
python 01b_sensitivity_ex91.py
```

Each year takes approximately 9 minutes; the full pipeline (8 years) runs in ~65 minutes on a machine with 16 GB RAM.

## Key Results

- **ICT material shadow:** 4.02 Gt (2029), 3.5% of global extraction (~115 Gt), grown 3.6-fold since 1990
- **Material composition:** non-metallic minerals 44.0%, metal ores 27.8% (copper 40.3% of metals), fossil fuels 21.4%, biomass 6.8%
- **Top consumers:** China (1.17 Gt), India (0.46 Gt), United States (0.45 Gt)
- **Top extractors:** China (1.38 Gt), India (0.40 Gt), Australia (0.18 Gt)
- **Net importers:** USA (+268 Mt), Japan (+172 Mt), Germany (+81 Mt)
- **Net exporters:** China (−218 Mt), Australia (−136 Mt), Chile (−129 Mt)
- **Sensitivity:** excluding sector 91 reduces the shadow by 30–42% but preserves all qualitative patterns

## References

Lenzen, M., Geschke, A., Abd Rahman, M. D., et al. (2017). The Global MRIO Lab. *Economic Systems Research*, 29(2), 158–186. https://doi.org/10.1080/09535314.2017.1301887

Lenzen, M., Geschke, A., West, J., et al. (2022). Implementing the material footprint to measure progress towards Sustainable Development Goals 8 and 12. *Nature Sustainability*, 5(2), 157–166. https://doi.org/10.1038/s41893-021-00811-6

## Licence

Code: MIT. Data: subject to GLORIA MRIO terms of use.

## Citation

```
Gómez, R. E. (2026). The Material Shadow of the Digital Economy: Tracing
the Supply-Chain Footprint of ICT across 164 Economies, 1990–2029.
```
