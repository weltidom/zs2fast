use flate2::read::GzDecoder;
use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str;
use thiserror::Error;

use arrow::array::{ArrayRef, Float64Builder, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[derive(Error, Debug)]
enum Zs2Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Bad marker: missing 0xDEADBEAF")]
    BadMarker,
    #[error("Parse error at offset {offset}: {msg}")]
    Parse { offset: usize, msg: String },
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

type Res<T> = Result<T, Zs2Error>;

impl From<Zs2Error> for PyErr {
    fn from(err: Zs2Error) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
    }
}

#[pyfunction]
fn zs2_to_parquet(
    input_zs2: &str,
    output_parquet: &str,
    include_u32: Option<bool>,
) -> PyResult<()> {
    let include_u32 = include_u32.unwrap_or(false);

    // 1) Decompress gz (.zs2 is gzipped)
    let f = File::open(input_zs2)?;
    let mut gz = GzDecoder::new(BufReader::new(f));
    let mut data = Vec::<u8>::new();
    gz.read_to_end(&mut data)?;

    // 2) Quick sanity: 0xDE AD BE AF (little-endian u32 -> bytes: AF BE AD DE)
    if data.len() < 4 || data[0..4] != [0xAF, 0xBE, 0xAD, 0xDE] {
        return Err(Zs2Error::BadMarker.into());
    }

    // 3) Scan stream
    let mut i = 4usize;
    let n = data.len();

    // Name stack reflects nesting caused by 0xDD and ended by 0xFF
    let mut name_stack: Vec<String> = Vec::with_capacity(8);

    // Output builders (long format)
    let mut b_series = StringBuilder::new();
    let mut b_subtyp = StringBuilder::new();
    let mut b_index = UInt32Builder::new();
    let mut b_value = Float64Builder::new();

    while i < n {
        // 0xFF means "end of nesting" with no name
        if data[i] == 0xFF {
            name_stack.pop();
            i += 1;
            continue;
        }

        // Read name (length-prefixed ASCII)
        ensure_len(i + 1, n, i)?;
        let name_len = data[i] as usize;
        i += 1;
        ensure_len(i + name_len, n, i)?;
        let name = str::from_utf8(&data[i..i + name_len])?.to_string();
        i += name_len;

        if i >= n {
            break;
        }
        let dtype = data[i];

        match dtype {
            0xEE => {
                // EE block: [EE][u16 subtype][u32 count][payload...]
                ensure_len(i + 1 + 2 + 4, n, i)?;
                i += 1;
                let sub = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;
                let cnt =
                    u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
                i += 4;

                // Compose series name from current path + leaf name
                let series = if name_stack.is_empty() {
                    name.clone()
                } else {
                    let mut s = name_stack.join("/");
                    if !name.is_empty() {
                        s.push('/');
                        s.push_str(&name);
                    }
                    s
                };

                match sub {
                    0x0004 => {
                        // float32 list
                        let need = cnt * 4;
                        ensure_len(i + need, n, i)?;
                        // read f32 -> f64
                        for idx in 0..cnt {
                            let off = i + idx * 4;
                            let v = f32::from_le_bytes([
                                data[off],
                                data[off + 1],
                                data[off + 2],
                                data[off + 3],
                            ]) as f64;
                            b_series.append_value(&series);
                            b_subtyp.append_value("EE04");
                            b_index.append_value(idx as u32);
                            b_value.append_value(v);
                        }
                        i += need;
                    }
                    0x0005 => {
                        // float64 list
                        let need = cnt * 8;
                        ensure_len(i + need, n, i)?;
                        for idx in 0..cnt {
                            let off = i + idx * 8;
                            let v = f64::from_le_bytes([
                                data[off],
                                data[off + 1],
                                data[off + 2],
                                data[off + 3],
                                data[off + 4],
                                data[off + 5],
                                data[off + 6],
                                data[off + 7],
                            ]);
                            b_series.append_value(&series);
                            b_subtyp.append_value("EE05");
                            b_index.append_value(idx as u32);
                            b_value.append_value(v);
                        }
                        i += need;
                    }
                    0x0016 => {
                        // u32 list (index/time/etc). Optional to include or skip.
                        let need = cnt * 4;
                        ensure_len(i + need, n, i)?;
                        if include_u32 {
                            for idx in 0..cnt {
                                let off = i + idx * 4;
                                let v = u32::from_le_bytes([
                                    data[off],
                                    data[off + 1],
                                    data[off + 2],
                                    data[off + 3],
                                ]) as f64;
                                b_series.append_value(&series);
                                b_subtyp.append_value("EE16");
                                b_index.append_value(idx as u32);
                                b_value.append_value(v);
                            }
                        }
                        i += need;
                    }
                    0x0011 => {
                        // u8 list — we skip in v1 to keep the Parquet tidy; will add toggle later
                        // Need to read length (cnt) bytes and skip them.
                        let need = cnt;
                        ensure_len(i + need, n, i)?;
                        i += need;
                    }
                    0x0000 => {
                        // empty list
                    }
                    _ => {
                        // Unknown EE subtype — attempt to skip (best-effort)
                        // We don't know the unit size; bail gracefully by not advancing beyond bounds.
                        return Err(Zs2Error::Parse {
                            offset: i,
                            msg: format!("Unknown EE subtype 0x{sub:04X}"),
                        }
                        .into());
                    }
                }
            }

            0xAA | 0x00 => {
                // UTF-16LE string with bit31 length marker
                // [0xAA|0x00][u32 len_with_marker][len*2 bytes]
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;
                let char_count = (raw & 0x7FFF_FFFF) as usize;
                let need = char_count * 2;
                ensure_len(i + need, n, i)?;
                // We don't use the value here; just skip.
                i += need;
            }

            0xDD => {
                // Start of nested section. Skip the small dd-payload and push name on the stack.
                // Layout: [0xDD][u8 len][len bytes ASCII]
                ensure_len(i + 2, n, i)?;
                let len = data[i + 1] as usize;
                ensure_len(i + 2 + len, n, i)?;
                i += 2 + len;
                name_stack.push(name);
            }

            0xFF => {
                // Handled at top; unreachable here (we tested it already).
                i += 1;
            }

            // Basic numeric scalars we don't need: just skip their fixed width
            0x11 | 0x22 | 0x33 | 0x44 => {
                i += 1 + 4;
            }
            0x55 | 0x66 => {
                i += 1 + 2;
            }
            0x88 | 0x99 => {
                i += 1 + 1;
            }
            0xBB => {
                i += 1 + 4;
            }
            0xCC => {
                i += 1 + 8;
            }

            _ => {
                return Err(Zs2Error::Parse {
                    offset: i,
                    msg: format!("Unknown data type tag 0x{dtype:02X} for name {name}"),
                }
                .into());
            }
        }
    }

    // 4) Build Arrow table
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("series", DataType::Utf8, false),
        Field::new("subtype", DataType::Utf8, false),
        Field::new("index", DataType::UInt32, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let a_series: ArrayRef = std::sync::Arc::new(b_series.finish());
    let a_subtyp: ArrayRef = std::sync::Arc::new(b_subtyp.finish());
    let a_index: ArrayRef = std::sync::Arc::new(b_index.finish());
    let a_value: ArrayRef = std::sync::Arc::new(b_value.finish());

    let batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![a_series, a_subtyp, a_index, a_value],
    )
    .map_err(|e| Zs2Error::from(e))?;

    // 5) Write Parquet
    let file = File::create(output_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, std::sync::Arc::clone(&schema), Some(props))
            .map_err(|e| Zs2Error::from(e))?;
    writer.write(&batch).map_err(|e| Zs2Error::from(e))?;
    writer.close().map_err(|e| Zs2Error::from(e))?;

    Ok(())
}

#[inline]
fn ensure_len(want: usize, n: usize, at: usize) -> Res<()> {
    if want > n {
        return Err(Zs2Error::Parse {
            offset: at,
            msg: format!("unexpected EOF (need {want}, have {n})"),
        });
    }
    Ok(())
}

#[pymodule]
fn zs2fast(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zs2_to_parquet, m)?)?;
    Ok(())
}
