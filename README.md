# The Material Shadow of the Digital Economy

Replication materials for:

> Gómez, R. E. (2026). The Material Shadow of the Digital Economy: Tracing the Supply-Chain Footprint of ICT across 164 Economies, 1990–2029. *Technological Forecasting and Social Change*.

This study traces the upstream material extraction induced by five ICT sectors using the Leontief inverse applied to the GLORIA multi-regional input–output database (164 economies, 120 sectors, 367 material indicators, 8 benchmark years).

## Repository structure

```
subirGitHubTFSC/
├── data/
│   ├── baseline/              # Footprint and multiplier parquets (5-sector ICT)
│   ├── sensitivity_ex91/      # Sensitivity analysis excluding sector 91
│   ├── material_classification.parquet
│   ├── region_order.parquet
│   ├── ne_110m_admin_0_countries.zip   # Natural Earth shapefile (110m)
│   └── wdi_panel.parquet
├── figures/                   # Generated figures (PNG, 300 dpi)
├── scripts/
│   ├── 01_compute_ict_footprint.py     # Leontief inverse via Schur complement
│   ├── 01b_sensitivity_ex91.py         # Sensitivity: excluding sector 91
│   ├── 02_analysis_figures.py          # Generate all figures (6 main + 4 supplementary)
│   └── 03_sensitivity_comparison.py    # Baseline vs ex91 comparison metrics
└── README.md
```

## Requirements

Python 3.10+ with the following packages:

```
numpy
pandas
scipy
matplotlib
seaborn
geopandas
mpl-chord-diagram
pyarrow
```

Install with:

```bash
pip install numpy pandas scipy matplotlib seaborn geopandas mpl-chord-diagram pyarrow
```

## Replication

### Generating figures (from pre-computed data)

The `data/` directory contains all pre-computed footprint and multiplier parquets. To regenerate the figures:

```bash
cd scripts
python 02_analysis_figures.py
python 03_sensitivity_comparison.py
```

Figures are saved to `figures/` in PNG and PDF formats.

### Recomputing from GLORIA source data

Scripts `01_compute_ict_footprint.py` and `01b_sensitivity_ex91.py` recompute the Leontief inverse from the raw GLORIA MRIO database. These require:

1. The GLORIA MRIO Part I database (~60 GB), available from [ielab.info](https://ielab.info) (Lenzen et al., 2017, 2022).
2. Set the `GLORIA_DIR` environment variable to the root of your local GLORIA database, or edit the default path in the script header.

```bash
export GLORIA_DIR=/path/to/gloria-mrio
cd scripts
python 01_compute_ict_footprint.py
python 01b_sensitivity_ex91.py
```

Each year takes approximately 9 minutes; the full pipeline (8 years) runs in ~65 minutes on a machine with 16 GB RAM.

## Citation

If you use these materials, please cite the article (reference to be updated upon publication).

## Licence

MIT
