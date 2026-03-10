# zs2fast

A fast Python extension for converting Zwick ZS2 test machine files to Parquet format, written in Rust/PyO3.

## Features

- Fast gzip decompression and binary parsing of .zs2 files
- Extracts channel time-series data with automatic unit resolution
- Extracts evaluated parameters and test results with units
- Converts to Parquet format using Apache Arrow
- Built with PyO3 for seamless Python integration
- Supports Python 3.9–3.12 on Linux, macOS, and Windows

## Installation

### From PyPI

```bash
pip install zs2fast
```

### From Source

Requires Rust and Python 3.9+.

```bash
# Install maturin
pip install maturin

# Clone and build
git clone https://github.com/weltidom/zs2fast.git
cd zs2fast
maturin develop --release
```

## Usage

```python
import zs2fast
import polars as pl

# Convert a .zs2 file to raw long-format Parquet
# Useful for exploring binary structure and raw values
zs2fast.zs2_to_parquet("input.zs2", "output.parquet", include_u32=False)
# columns: series, subtype, index, value

# Export channel time-series with semantic names and units
zs2fast.zs2_channels_to_parquet("input.zs2", "channels.parquet")
# columns: sample_idx, channel_idx, channel_name, unit, timepoint, value, data_type
channels = pl.read_parquet("channels.parquet")
print(channels.filter(pl.col('unit').is_not_null()).head())

# Export calculated test results (Bruchbild, elongation, max force, etc.)
# Units are resolved from EinheitName → QS_ValSetting blob → UnitTables
zs2fast.zs2_evaluated_params_to_parquet("input.zs2", "evaluated_params.parquet")
# columns: sample_idx, param_id, short_name, param_name, unit, value, value_text
params = pl.read_parquet("evaluated_params.parquet")
print(params.filter(pl.col('value').is_not_null()).head())

# Extract per-sample test settings and parameters (not calculated results)
zs2fast.zs2_parameterliste_results_to_parquet("input.zs2", "sample_params.parquet")
# columns: sample_id, result_id, result_name, unit, value_text, value
sample_params = pl.read_parquet("sample_params.parquet")
print(sample_params.head())
```

## Unit Resolution

The parser automatically resolves measurement units using a 3-tier strategy:

1. **Direct unit name** (`EinheitName` field) — Direct unit symbol (mm, N, MPa, etc.)
2. **Unit table key** (from `QS_ValSetting` binary blob) — References `/UnitTables` section (UT_Length → mm, UT_Force → N, etc.)
3. **Fallback inference** — Attempts to infer units from table key name

Channels typically use tier 1–2, while evaluated parameters primarily use tiers 2–3. This ensures maximum unit coverage across different .zs2 file structures.

## Development

### Build

```bash
maturin build --release
```

Note: for this PyO3 extension module on macOS, prefer `maturin build`/`maturin develop`.
Running plain `cargo build --release` can fail with unresolved Python symbols
(`_Py*`, `__Py_*`) during linking.

### Lint

```bash
cargo fmt
cargo clippy
```

### Test

```bash
cargo test
```

## License

See LICENSE file for details.
