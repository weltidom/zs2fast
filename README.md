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
