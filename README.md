# zs2fast

A fast Python extension for converting ZS2 files to Parquet format, written in Rust.

## Features

- Fast decompression and parsing of .zs2 files
- Converts to Parquet format using Apache Arrow
- Built with PyO3 for seamless Python integration

## Installation

### From Source

Requires Rust and Python 3.9+.

```bash
# Install maturin
pip install maturin

# Build and install
maturin develop --release
```

### From PyPI (coming soon)

```bash
pip install zs2fast
```

## Usage

```python
import zs2fast

# Convert a .zs2 file to Parquet
zs2fast.zs2_to_parquet("input.zs2", "output.parquet", include_u32=False)

# Inspect available channels and decoded UTF-16 metadata
catalog = zs2fast.zs2_channel_catalog("input.zs2")
# rows: (series, subtype, count, metadata)
print(catalog[:5])

# Extract evaluated per-sample parameters with semantic names
params = zs2fast.zs2_evaluated_params("input.zs2")
# rows: (sample_idx, param_id, short_name, param_name, value)
print(params[:10])

# Extract evaluated parameters including optional text/enum values
params_rich = zs2fast.zs2_evaluated_params_with_text("input.zs2")
# rows: (sample_idx, param_id, short_name, param_name, value, value_text)
print(params_rich[:10])

# Export evaluated parameters directly to Parquet
zs2fast.zs2_evaluated_params_to_parquet("input.zs2", "evaluated_params.parquet")
# parquet columns: sample_idx, param_id, short_name, param_name, value, value_text

# Extract per-sample test results from:
# .../Series/SeriesElements/Elem{s}/.../EvalContext/ParamContext/ParameterListe/Elem{p}
# useful for values like Zugscherfestigkeit / Bruchbild
zs2fast.zs2_parameterliste_results_to_parquet("input.zs2", "sample_results.parquet")
# parquet columns: sample_id, result_id, result_name, unit, value_text, value

# Backward-compatible alias (same output)
zs2fast.zs2_shear_test_results_to_parquet("input.zs2", "sample_results.parquet")
```

## Development

### Build

```bash
maturin build --release
```

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
