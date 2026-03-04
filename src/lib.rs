use flate2::read::GzDecoder;
use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str;
use std::collections::HashMap;
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
#[pyo3(signature = (input_zs2, output_parquet, include_u32=None))]
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
    .map_err(Zs2Error::from)?;

    // 5) Write Parquet
    let file = File::create(output_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::clone(&schema), Some(props))
        .map_err(Zs2Error::from)?;
    writer.write(&batch).map_err(Zs2Error::from)?;
    writer.close().map_err(Zs2Error::from)?;

    Ok(())
}

#[pyfunction]
fn zs2_channels_to_parquet(input_zs2: &str, output_parquet: &str) -> PyResult<()> {
    // 1) Decompress
    let f = File::open(input_zs2)?;
    let mut gz = GzDecoder::new(BufReader::new(f));
    let mut data = Vec::<u8>::new();
    gz.read_to_end(&mut data)?;

    if data.len() < 4 || data[0..4] != [0xAF, 0xBE, 0xAD, 0xDE] {
        return Err(Zs2Error::BadMarker.into());
    }

    let n = data.len();

    // ===== PASS 1: Extract parameter dictionary and channel mappings =====
    #[derive(Default)]
    struct ParamDictEntry {
        param_id: Option<u32>,
        short_name: String,
        param_name: String,
        unit_name: String,
    }
    
    let mut eig_dict_by_elem: HashMap<u32, ParamDictEntry> = HashMap::new();
    let mut cm_dict_by_elem: HashMap<u32, ParamDictEntry> = HashMap::new();
    let mut channel_trs_ids: HashMap<String, u32> = HashMap::new(); // "sample_{s}/ch_{idx}" -> TrsChannelId
    let mut samples_seen: Vec<u32> = Vec::new(); // list of sample indices found

    let mut i = 4usize;
    let mut name_stack: Vec<String> = Vec::with_capacity(8);

    while i < n {
        if data[i] == 0xFF {
            if !name_stack.is_empty() {
                name_stack.pop();
            }
            i += 1;
            continue;
        }

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
        let path = name_stack.join("/") + "/" + &name;
        let is_eigenschaftenliste = path.contains("/EigenschaftenListe/");
        let is_channel_manager = path.contains("/SeriesDef/TestTaskDefs/")
            && path.contains("/ChannelManager/ChannelManager/Elem");
        let elem_idx_opt = if is_eigenschaftenliste || is_channel_manager {
            extract_elem_index(&path)
        } else {
            None
        };

        match dtype {
            0xEE => {
                ensure_len(i + 1 + 2 + 4, n, i)?;
                i += 1;
                let sub = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;
                let cnt = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;

                // Extract parameter IDs from EigenschaftenListe
                if is_eigenschaftenliste && path.ends_with("/ID") && sub == 0x0016 && cnt >= 1 {
                    let need = cnt as usize * 4;
                    ensure_len(i + need, n, i)?;
                    let param_id = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                    if let Some(elem_idx) = elem_idx_opt {
                        eig_dict_by_elem.entry(elem_idx).or_default().param_id = Some(param_id);
                    }
                    i += need;
                    continue;
                }

                if is_channel_manager && path.ends_with("/ID") && sub == 0x0016 && cnt >= 1 {
                    let need = cnt as usize * 4;
                    ensure_len(i + need, n, i)?;
                    let param_id = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                    if let Some(elem_idx) = elem_idx_opt {
                        cm_dict_by_elem.entry(elem_idx).or_default().param_id = Some(param_id);
                    }
                    i += need;
                    continue;
                }

                // Skip array data
                let bytes_per_item = match sub {
                    0x0004 | 0x0016 | 0x0005 => if sub == 0x0005 { 8 } else { 4 },
                    0x0011 => 1usize,
                    _ => 0usize,
                };
                let need = (cnt as usize) * bytes_per_item;
                ensure_len(i + need, n, i)?;
                i += need;
            }

            0xAA | 0x00 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;
                let char_count = (raw & 0x7FFF_FFFF) as usize;
                let need = char_count * 2;
                ensure_len(i + need, n, i)?;

                // Extract parameter names from Name/Text in EigenschaftenListe
                if is_eigenschaftenliste && path.ends_with("/Name/Text") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            eig_dict_by_elem.entry(elem_idx).or_default().param_name = text;
                        }
                    }
                }

                if is_eigenschaftenliste && path.ends_with("/Kurzzeichen/Text") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            eig_dict_by_elem.entry(elem_idx).or_default().short_name = text;
                        }
                    }
                }

                // Extract unit names from EinheitName in EigenschaftenListe
                if is_eigenschaftenliste && path.ends_with("/EinheitName") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            eig_dict_by_elem.entry(elem_idx).or_default().unit_name = text;
                        }
                    }
                }

                if is_channel_manager && path.ends_with("/Name/Text") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            cm_dict_by_elem.entry(elem_idx).or_default().param_name = text;
                        }
                    }
                }

                if is_channel_manager && path.ends_with("/Kurzzeichen/Text") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            cm_dict_by_elem.entry(elem_idx).or_default().short_name = text;
                        }
                    }
                }

                if is_channel_manager && path.ends_with("/UnitTableName") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            cm_dict_by_elem.entry(elem_idx).or_default().unit_name = text;
                        }
                    }
                }

                // Extract actual unit symbols from Einheit/Kurzzeichen in ChannelManager
                if is_channel_manager && path.ends_with("/Einheit/Kurzzeichen") {
                    if let Ok(text) = decode_utf16le(&data[i..i + need]) {
                        if let Some(elem_idx) = elem_idx_opt {
                            // Override UnitTableName with actual unit symbol
                            cm_dict_by_elem.entry(elem_idx).or_default().unit_name = text;
                        }
                    }
                }

                i += need;
            }

            0xDD => {
                ensure_len(i + 2, n, i)?;
                let len = data[i + 1] as usize;
                ensure_len(i + 2 + len, n, i)?;
                i += 2 + len;
                name_stack.push(name);
            }

            0xFF => i += 1,
            0x11 => {
                // Extract TrsChannelId mappings from DataChannels (0x11 is a scalar byte)
                if path.contains("/DataChannels/") && path.contains("/TrsChannelId") {
                    ensure_len(i + 1 + 4, n, i)?;
                    i += 1;
                    let trs_id = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                    i += 4;

                    if let Some(sample_idx) = extract_sample_index(&path) {
                        if let Some(ch_idx) = extract_data_channel_index(&path) {
                            if !samples_seen.contains(&sample_idx) {
                                samples_seen.push(sample_idx);
                            }
                            let key = format!("sample_{}/ch_{}", sample_idx, ch_idx);
                            channel_trs_ids.insert(key, trs_id);
                        }
                    }
                } else {
                    i += 1 + 4;
                }
            }
            0x22 | 0x33 | 0x44 => {
                // Extract ID from scalar u32/i32/f32 in EigenschaftenListe
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw_u32 = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;
                
                if is_eigenschaftenliste && path.ends_with("/ID") {
                    if let Some(elem_idx) = elem_idx_opt {
                        eig_dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u32);
                    }
                }

                if is_channel_manager && path.ends_with("/ID") {
                    if let Some(elem_idx) = elem_idx_opt {
                        cm_dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u32);
                    }
                }
            }
            0x55 | 0x66 => {
                // Extract ID from scalar u16 in EigenschaftenListe
                ensure_len(i + 1 + 2, n, i)?;
                i += 1;
                let raw_u16 = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;
                
                if is_eigenschaftenliste && path.ends_with("/ID") {
                    if let Some(elem_idx) = elem_idx_opt {
                        eig_dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u16 as u32);
                    }
                }

                if is_channel_manager && path.ends_with("/ID") {
                    if let Some(elem_idx) = elem_idx_opt {
                        cm_dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u16 as u32);
                    }
                }
            }
            0x88 | 0x99 => i += 1 + 1,
            0xBB => i += 1 + 4,
            0xCC => i += 1 + 8,
            _ => {
                return Err(Zs2Error::Parse {
                    offset: i,
                    msg: format!("Unknown data type tag 0x{dtype:02X}"),
                }
                .into())
            }
        }
    }

    // ===== Build inverse mapping: param_id -> (name, unit) =====
    let mut trs_to_name: HashMap<u32, String> = HashMap::new();
    let mut trs_to_unit: HashMap<u32, String> = HashMap::new();

    // Build UnitTableName -> unit symbol mapping (e.g. UT_Displacement -> mm)
    // from EigenschaftenListe text fields.
    let mut unit_table_to_symbol: HashMap<String, String> = HashMap::new();
    for entry in eig_dict_by_elem.values() {
        let unit = entry.unit_name.trim();
        if unit.is_empty() {
            continue;
        }

        let short_key = entry.short_name.trim();
        if !short_key.is_empty() {
            unit_table_to_symbol
                .entry(short_key.to_string())
                .or_insert_with(|| unit.to_string());
        }

        let name_key = entry.param_name.trim();
        if !name_key.is_empty() {
            unit_table_to_symbol
                .entry(name_key.to_string())
                .or_insert_with(|| unit.to_string());
        }
    }

    // Process ChannelManager first for names (using actual channel names from ChannelManager)
    for (_, entry) in cm_dict_by_elem {
        if let Some(param_id) = entry.param_id {
            let name = if !entry.param_name.trim().is_empty() {
                entry.param_name
            } else {
                entry.short_name
            };
            trs_to_name.insert(param_id, name);
            // Use resolved UnitTableName when available (e.g. UT_Force -> N).
            let mut resolved_unit = unit_table_to_symbol
                .get(entry.unit_name.trim())
                .cloned()
                .unwrap_or(entry.unit_name);
            if resolved_unit.starts_with("UT_") {
                if let Some(symbol) = infer_unit_from_unit_table_name(&resolved_unit) {
                    resolved_unit = symbol.to_string();
                }
            }
            trs_to_unit.entry(param_id).or_insert(resolved_unit);
        }
    }

    // Process EigenschaftenListe last for units - these have actual unit symbols (N, mm, s, etc.)
    // and should override the UnitTableName values from ChannelManager
    for (_, entry) in eig_dict_by_elem {
        if let Some(param_id) = entry.param_id {
            let name = if !entry.param_name.trim().is_empty() {
                entry.param_name
            } else {
                entry.short_name
            };
            // Keep the channel name from ChannelManager if it exists (it's more descriptive)
            trs_to_name.entry(param_id).or_insert(name);
            // Override with EigenschaftenListe unit if available (actual unit symbols)
            if !entry.unit_name.trim().is_empty() {
                let mut resolved_unit = unit_table_to_symbol
                    .get(entry.unit_name.trim())
                    .cloned()
                    .unwrap_or(entry.unit_name);
                if resolved_unit.starts_with("UT_") {
                    if let Some(symbol) = infer_unit_from_unit_table_name(&resolved_unit) {
                        resolved_unit = symbol.to_string();
                    }
                }
                trs_to_unit.insert(param_id, resolved_unit);
            }
        }
    }

    // ===== PASS 2: Extract channel data with semantic names =====
    i = 4;
    name_stack.clear();

    let mut sample_idx_builder = UInt32Builder::new();
    let mut channel_idx_builder = UInt32Builder::new();
    let mut channel_name_builder = StringBuilder::new();
    let mut unit_name_builder = StringBuilder::new();
    let mut timepoint_builder = UInt32Builder::new();
    let mut value_builder = Float64Builder::new();
    let mut data_type_builder = StringBuilder::new();

    while i < n {
        if data[i] == 0xFF {
            if !name_stack.is_empty() {
                name_stack.pop();
            }
            i += 1;
            continue;
        }

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
        let path = name_stack.join("/") + "/" + &name;

        if dtype == 0xEE {
            ensure_len(i + 1 + 2 + 4, n, i)?;
            i += 1;
            let sub = u16::from_le_bytes([data[i], data[i + 1]]);
            i += 2;
            let cnt = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
            i += 4;

            // Check if this is a real-time capture channel
            if path.contains("/DataChannels/") && path.contains("RealTimeCapture/Trs/SingleGroupDataBlock") {
                if let (Some(sample_idx), Some(ch_idx)) = (
                    extract_sample_index(&path),
                    extract_data_channel_index(&path),
                ) {
                    let (data_type_name, bytes_per_item) = match sub {
                        0x0004 => ("f32", 4usize),
                        0x0005 => ("f64", 8usize),
                        0x0016 => ("u32", 4usize),
                        0x0011 => ("u8", 1usize),
                        _ => ("unknown", 0usize),
                    };

                    let need = (cnt as usize) * bytes_per_item;
                    ensure_len(i + need, n, i)?;
                    let blob = &data[i..i + need];

                    // Look up channel name from TrsChannelId
                    let key = format!("sample_{}/ch_{}", sample_idx, ch_idx);
                    let trs_id = channel_trs_ids.get(&key).copied();
                    let ch_name = if let Some(tid) = trs_id {
                        trs_to_name
                            .get(&tid)
                            .cloned()
                            .unwrap_or_else(|| format!("Ch{}", ch_idx))
                    } else {
                        format!("Ch{}", ch_idx)
                    };

                    // Look up unit name from TrsChannelId -> unit_names
                    let unit = if let Some(tid) = trs_id {
                        let explicit = trs_to_unit
                            .get(&tid)
                            .cloned()
                            .unwrap_or_default();
                        if explicit.trim().is_empty() {
                            infer_unit_from_channel_name(&ch_name).to_string()
                        } else {
                            explicit
                        }
                    } else {
                        infer_unit_from_channel_name(&ch_name).to_string()
                    };

                    // Extract values
                    for (tp, chunk) in blob.chunks(bytes_per_item).enumerate() {
                        let value = match sub {
                            0x0004 => {
                                if chunk.len() >= 4 {
                                    f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as f64
                                } else {
                                    f64::NAN
                                }
                            }
                            0x0005 => {
                                if chunk.len() >= 8 {
                                    f64::from_le_bytes([
                                        chunk[0], chunk[1], chunk[2], chunk[3],
                                        chunk[4], chunk[5], chunk[6], chunk[7],
                                    ])
                                } else {
                                    f64::NAN
                                }
                            }
                            0x0016 => {
                                if chunk.len() >= 4 {
                                    u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as f64
                                } else {
                                    f64::NAN
                                }
                            }
                            0x0011 => {
                                if !chunk.is_empty() {
                                    chunk[0] as f64
                                } else {
                                    f64::NAN
                                }
                            }
                            _ => f64::NAN,
                        };

                        if value.is_finite() {
                            sample_idx_builder.append_value(sample_idx);
                            channel_idx_builder.append_value(ch_idx);
                            channel_name_builder.append_value(&ch_name);
                            unit_name_builder.append_value(&unit);
                            timepoint_builder.append_value(tp as u32);
                            value_builder.append_value(value);
                            data_type_builder.append_value(data_type_name);
                        }
                    }

                    i += need;
                    continue;
                }
            }

            // Skip non-channel data
            let bytes_per_item = match sub {
                0x0004 | 0x0016 | 0x0005 => if sub == 0x0005 { 8 } else { 4 },
                0x0011 => 1,
                _ => 0,
            };
            let need = (cnt as usize) * bytes_per_item;
            ensure_len(i + need, n, i)?;
            i += need;
        } else if dtype == 0xAA || dtype == 0x00 {
            ensure_len(i + 1 + 4, n, i)?;
            i += 1;
            let raw = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
            i += 4;
            let char_count = (raw & 0x7FFF_FFFF) as usize;
            let need = char_count * 2;
            ensure_len(i + need, n, i)?;
            i += need;
        } else if dtype == 0xDD {
            ensure_len(i + 2, n, i)?;
            let len = data[i + 1] as usize;
            ensure_len(i + 2 + len, n, i)?;
            i += 2 + len;
            name_stack.push(name);
        } else {
            match dtype {
                0xFF => i += 1,
                0x11 | 0x22 | 0x33 | 0x44 => i += 1 + 4,
                0x55 | 0x66 => i += 1 + 2,
                0x88 | 0x99 => i += 1 + 1,
                0xBB => i += 1 + 4,
                0xCC => i += 1 + 8,
                _ => {
                    return Err(Zs2Error::Parse {
                        offset: i,
                        msg: format!("Unknown data type tag 0x{dtype:02X}"),
                    }
                    .into())
                }
            }
        }
    }

    // Build and write Parquet
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("sample_idx", DataType::UInt32, false),
        Field::new("channel_idx", DataType::UInt32, false),
        Field::new("channel_name", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, true),
        Field::new("timepoint", DataType::UInt32, false),
        Field::new("value", DataType::Float64, true),
        Field::new("data_type", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(sample_idx_builder.finish()),
            std::sync::Arc::new(channel_idx_builder.finish()),
            std::sync::Arc::new(channel_name_builder.finish()),
            std::sync::Arc::new(unit_name_builder.finish()),
            std::sync::Arc::new(timepoint_builder.finish()),
            std::sync::Arc::new(value_builder.finish()),
            std::sync::Arc::new(data_type_builder.finish()),
        ],
    )
    .map_err(Zs2Error::from)?;

    let file = File::create(output_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::clone(&schema), Some(props))
        .map_err(Zs2Error::from)?;
    writer.write(&batch).map_err(Zs2Error::from)?;
    writer.close().map_err(Zs2Error::from)?;

    Ok(())
}

/// Extract evaluated parameters from EigenschaftenListe to Parquet
#[pyfunction]
fn zs2_evaluated_params_to_parquet(input_zs2: &str, output_parquet: &str) -> PyResult<()> {
    let f = File::open(input_zs2)?;
    let mut gz = GzDecoder::new(BufReader::new(f));
    let mut data = Vec::<u8>::new();
    gz.read_to_end(&mut data)?;

    if data.len() < 4 || data[0..4] != [0xAF, 0xBE, 0xAD, 0xDE] {
        return Err(Zs2Error::BadMarker.into());
    }

    #[derive(Default)]
    struct DictEntry {
        param_id: Option<u32>,
        short_name: String,
        param_name: String,
    }

    #[derive(Default)]
    struct SampleParam {
        param_id: Option<u32>,
        value_numeric: Option<f64>,
        value_text: Option<String>,
    }

    let mut dict_by_elem: HashMap<u32, DictEntry> = HashMap::new();
    let mut sample_params: HashMap<(u32, u32), SampleParam> = HashMap::new();

    let mut i = 4usize;
    let n = data.len();
    let mut name_stack: Vec<String> = Vec::with_capacity(8);

    while i < n {
        if data[i] == 0xFF {
            if !name_stack.is_empty() {
                name_stack.pop();
            }
            i += 1;
            continue;
        }

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
        let path = name_stack.join("/") + "/" + &name;
        let leaf = path.rsplit('/').next().unwrap_or("");
        let sample_key = extract_direct_sample_parameter_key(&path);
        let in_global_dict = path.contains("/Series/EvalContext/ParamContext/EigenschaftenListe/");
        let dict_elem_idx = if in_global_dict { extract_elem_index(&path) } else { None };

        match dtype {
            0xAA | 0x00 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;
                let char_count = (raw & 0x7FFF_FFFF) as usize;
                let need = char_count * 2;
                ensure_len(i + need, n, i)?;
                let text = decode_utf16le(&data[i..i + need])?;
                i += need;

                if let Some(elem_idx) = dict_elem_idx {
                    let entry = dict_by_elem.entry(elem_idx).or_default();
                    if path.ends_with("/Name/Text") {
                        entry.param_name = text.trim().to_string();
                    } else if path.ends_with("/Kurzzeichen/Text") {
                        entry.short_name = text.trim().to_string();
                    }
                }
            }

            0x22 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw_u32 = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                let val = i32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as f64;
                i += 4;

                if in_global_dict && path.ends_with("/ID") {
                    if let Some(elem_idx) = dict_elem_idx {
                        dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u32);
                    }
                }

                if let Some((s_idx, p_idx)) = sample_key {
                    let entry = sample_params.entry((s_idx, p_idx)).or_default();
                    if leaf == "ID" {
                        entry.param_id = Some(raw_u32);
                    } else if is_likely_value_leaf(leaf) {
                        entry.value_numeric = Some(val);
                    }
                }
            }

            0x44 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let val = f32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as f64;
                i += 4;

                if let Some((s_idx, p_idx)) = sample_key {
                    let entry = sample_params.entry((s_idx, p_idx)).or_default();
                    if leaf == "ID" {
                        entry.param_id = Some(val as u32);
                    } else if is_likely_value_leaf(leaf) {
                        entry.value_numeric = Some(val);
                    }
                }
            }

            0xCC => {
                ensure_len(i + 1 + 8, n, i)?;
                i += 1;
                let val = f64::from_le_bytes([
                    data[i], data[i + 1], data[i + 2], data[i + 3],
                    data[i + 4], data[i + 5], data[i + 6], data[i + 7],
                ]);
                i += 8;

                if let Some((s_idx, p_idx)) = sample_key {
                    let entry = sample_params.entry((s_idx, p_idx)).or_default();
                    if leaf == "ID" {
                        entry.param_id = Some(val as u32);
                    } else if is_likely_value_leaf(leaf) {
                        entry.value_numeric = Some(val);
                    }
                }
            }

            0xDD => {
                ensure_len(i + 2, n, i)?;
                let len = data[i + 1] as usize;
                ensure_len(i + 2 + len, n, i)?;
                i += 2 + len;
                name_stack.push(name);
            }

            0xEE => {
                ensure_len(i + 1 + 2 + 4, n, i)?;
                i += 1;
                let sub = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;
                let cnt = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;

                let bytes_per_item = match sub {
                    0x0004 | 0x0016 => 4,
                    0x0005 => 8,
                    0x0011 => 1,
                    _ => 0,
                };
                let need = (cnt as usize) * bytes_per_item;
                ensure_len(i + need, n, i)?;

                if sub == 0x0016 && cnt >= 1 && need >= 4 && leaf == "ID" {
                    let raw_u32 = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);

                    if in_global_dict {
                        if let Some(elem_idx) = dict_elem_idx {
                            dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u32);
                        }
                    }

                    if let Some((s_idx, p_idx)) = sample_key {
                        sample_params.entry((s_idx, p_idx)).or_default().param_id = Some(raw_u32);
                    }
                }

                if let Some((s_idx, p_idx)) = sample_key {
                    if sub == 0x0011 {
                        let blob = &data[i..i + need];
                        let entry = sample_params.entry((s_idx, p_idx)).or_default();
                        if leaf == "QS_ValPar" {
                            if entry.value_numeric.is_none() {
                                entry.value_numeric = decode_qs_valpar_f64(blob);
                            }
                        } else if leaf == "QS_TextPar" {
                            if entry.value_text.is_none() {
                                entry.value_text = decode_qs_textpar(blob);
                            }
                        }
                    }
                }

                i += need;
            }

            0x11 | 0x33 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw_u32 = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                let val = raw_u32 as f64;
                i += 4;

                if in_global_dict && path.ends_with("/ID") {
                    if let Some(elem_idx) = dict_elem_idx {
                        dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u32);
                    }
                }

                if let Some((s_idx, p_idx)) = sample_key {
                    let entry = sample_params.entry((s_idx, p_idx)).or_default();
                    if leaf == "ID" {
                        entry.param_id = Some(raw_u32);
                    } else if is_likely_value_leaf(leaf) {
                        entry.value_numeric = Some(val);
                    }
                }
            }
            0x55 | 0x66 => {
                ensure_len(i + 1 + 2, n, i)?;
                i += 1;
                let raw_u16 = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;

                if in_global_dict && path.ends_with("/ID") {
                    if let Some(elem_idx) = dict_elem_idx {
                        dict_by_elem.entry(elem_idx).or_default().param_id = Some(raw_u16 as u32);
                    }
                }

                if let Some((s_idx, p_idx)) = sample_key {
                    let entry = sample_params.entry((s_idx, p_idx)).or_default();
                    if leaf == "ID" {
                        entry.param_id = Some(raw_u16 as u32);
                    } else if is_likely_value_leaf(leaf) {
                        entry.value_numeric = Some(raw_u16 as f64);
                    }
                }
            }
            0x88 | 0x99 => i += 1 + 1,
            0xBB => i += 1 + 4,

            _ => {
                return Err(Zs2Error::Parse {
                    offset: i,
                    msg: format!("Unknown data type tag 0x{dtype:02X}"),
                }
                .into())
            }
        }
    }

    let mut sample_idx_builder = UInt32Builder::new();
    let mut param_id_builder = UInt32Builder::new();
    let mut short_name_builder = StringBuilder::new();
    let mut param_name_builder = StringBuilder::new();
    let mut value_builder = Float64Builder::new();
    let mut value_text_builder = StringBuilder::new();

    let mut dict_by_param_id: HashMap<u32, (String, String)> = HashMap::new();
    for (_, d) in dict_by_elem {
        if let Some(pid) = d.param_id {
            dict_by_param_id.insert(pid, (d.short_name, d.param_name));
        }
    }

    let mut sorted_rows: Vec<_> = sample_params.into_iter().collect();
    sorted_rows.sort_by_key(|((sample_idx, plist_idx), _)| (*sample_idx, *plist_idx));

    for ((sample_idx, _plist_idx), entry) in sorted_rows {
        if let Some(pid) = entry.param_id {
            let (short_name, param_name) = dict_by_param_id
                .get(&pid)
                .cloned()
                .unwrap_or((String::new(), String::new()));

            sample_idx_builder.append_value(sample_idx);
            param_id_builder.append_value(pid);
            short_name_builder.append_value(short_name);
            param_name_builder.append_value(param_name);

            if let Some(val) = entry.value_numeric {
                value_builder.append_value(val);
            } else {
                value_builder.append_null();
            }

            if let Some(text) = &entry.value_text {
                value_text_builder.append_value(text);
            } else {
                value_text_builder.append_null();
            }
        }
    }

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("sample_idx", DataType::UInt32, false),
        Field::new("param_id", DataType::UInt32, false),
        Field::new("short_name", DataType::Utf8, false),
        Field::new("param_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("value_text", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(sample_idx_builder.finish()),
            std::sync::Arc::new(param_id_builder.finish()),
            std::sync::Arc::new(short_name_builder.finish()),
            std::sync::Arc::new(param_name_builder.finish()),
            std::sync::Arc::new(value_builder.finish()),
            std::sync::Arc::new(value_text_builder.finish()),
        ],
    )
    .map_err(Zs2Error::from)?;

    let file = File::create(output_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::clone(&schema), Some(props))
        .map_err(Zs2Error::from)?;
    writer.write(&batch).map_err(Zs2Error::from)?;
    writer.close().map_err(Zs2Error::from)?;

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

fn extract_sample_index(path: &str) -> Option<u32> {
    if let Some(start) = path.find("/SeriesElements/Elem") {
        let rest = &path[start + 20..];
        if let Some(end) = rest.find('/') {
            if let Ok(idx) = rest[..end].parse::<u32>() {
                return Some(idx);
            }
        }
    }
    None
}

fn extract_data_channel_index(path: &str) -> Option<u32> {
    if let Some(start) = path.find("/DataChannels/Elem") {
        let rest = &path[start + 18..];
        if let Some(end) = rest.find('/') {
            if let Ok(idx) = rest[..end].parse::<u32>() {
                return Some(idx);
            }
        }
    }
    None
}

fn extract_elem_index(path: &str) -> Option<u32> {
    if let Some((_, rest)) = path.rsplit_once("/EigenschaftenListe/Elem") {
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            if let Ok(idx) = digits.parse::<u32>() {
                return Some(idx);
            }
        }
    }

    if let Some((_, rest)) = path.rsplit_once("/Elem") {
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            if let Ok(idx) = digits.parse::<u32>() {
                return Some(idx);
            }
        }
    }
    None
}

fn extract_direct_sample_parameter_key(path: &str) -> Option<(u32, u32)> {
    let marker = "/SeriesElements/Elem";
    let start = path.find(marker)?;
    let rest = &path[start + marker.len()..];

    let sample_digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if sample_digits.is_empty() {
        return None;
    }
    let sample_idx = sample_digits.parse::<u32>().ok()?;

    let after_sample = &rest[sample_digits.len()..];
    let direct_tail = "/EvalContext/ParamContext/ParameterListe/Elem";
    if !after_sample.starts_with(direct_tail) {
        return None;
    }

    let param_rest = &after_sample[direct_tail.len()..];
    let plist_digits: String = param_rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if plist_digits.is_empty() {
        return None;
    }
    let plist_idx = plist_digits.parse::<u32>().ok()?;

    Some((sample_idx, plist_idx))
}

fn decode_qs_valpar_f64(blob: &[u8]) -> Option<f64> {
    if blob.len() >= 9 {
        let v = f64::from_le_bytes([
            blob[1], blob[2], blob[3], blob[4], blob[5], blob[6], blob[7], blob[8],
        ]);
        if v.is_finite() && v.abs() < 1.0e12 {
            return Some(v);
        }
    }
    None
}

fn is_plausible_text_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || " äöüÄÖÜß_./:-+()[]{}%#".contains(c)
}

fn decode_qs_textpar(blob: &[u8]) -> Option<String> {
    if blob.len() < 4 {
        return None;
    }

    let mut best: Option<(i32, String)> = None;

    for start in 0..(blob.len() - 1) {
        let mut chars: Vec<char> = Vec::new();

        for j in ((start)..(blob.len() - 1)).step_by(2) {
            let u = u16::from_le_bytes([blob[j], blob[j + 1]]);
            if u == 0 {
                break;
            }
            if let Some(c) = char::from_u32(u as u32) {
                if !is_plausible_text_char(c) {
                    break;
                }
                chars.push(c);
                if chars.len() >= 64 {
                    break;
                }
            } else {
                break;
            }
        }

        if chars.is_empty() {
            continue;
        }

        let candidate: String = chars.iter().collect::<String>().trim().to_string();
        if candidate.len() < 2 {
            continue;
        }

        let has_alpha = candidate.chars().any(|c| c.is_alphabetic() || "äöüÄÖÜß".contains(c));
        if !has_alpha {
            continue;
        }

        let mut score: i32 = 0;
        if candidate.len() <= 10 {
            score += 8;
        }
        if candidate.chars().all(|c| c.is_ascii_alphabetic()) {
            score += 15;
        }
        if candidate.contains(':') {
            score -= 8;
        }
        if candidate.to_lowercase().contains("1252:") {
            score -= 20;
        }
        score -= candidate.len() as i32 / 2;

        match &best {
            None => best = Some((score, candidate)),
            Some((best_score, _)) if score > *best_score => best = Some((score, candidate)),
            _ => {}
        }
    }

    best.map(|(_, s)| s)
}

fn decode_utf16le(bytes: &[u8]) -> Res<String> {
    if bytes.len() % 2 != 0 {
        return Ok(String::new());
    }
    let mut result = String::new();
    for chunk in bytes.chunks_exact(2) {
        let code_point = u16::from_le_bytes([chunk[0], chunk[1]]);
        if let Some(c) = char::from_u32(code_point as u32) {
            result.push(c);
        }
    }
    Ok(result.trim_end_matches('\0').to_string())
}

fn extract_sample_and_parameter_index(path: &str) -> Option<(u32, u32)> {
    let sample_idx = if let Some((_, rest)) = path.split_once("/SeriesElements/Elem") {
        let sample_digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if sample_digits.is_empty() {
            return None;
        }
        sample_digits.parse::<u32>().ok()?
    } else {
        return None;
    };

    let param_idx = if let Some((_, rest)) = path.split_once("/EvalContext/ParamContext/ParameterListe/Elem") {
        let param_digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if param_digits.is_empty() {
            return None;
        }
        param_digits.parse::<u32>().ok()?
    } else {
        return None;
    };

    Some((sample_idx, param_idx))
}

fn decode_first_utf16_segment_from_blob(blob: &[u8]) -> Option<String> {
    if blob.len() < 4 {
        return None;
    }

    for start in (0..blob.len() - 1).step_by(2) {
        let mut units: Vec<u16> = Vec::new();
        for j in (start..blob.len() - 1).step_by(2) {
            let u = u16::from_le_bytes([blob[j], blob[j + 1]]);
            if u == 0 {
                break;
            }
            units.push(u);
            if units.len() >= 128 {
                break;
            }
        }

        if units.len() < 2 {
            continue;
        }

        if let Ok(decoded) = String::from_utf16(&units) {
            let trimmed = decoded.trim();
            if trimmed.len() >= 2
                && trimmed
                    .chars()
                    .any(|c| c.is_alphabetic() || "äöüÄÖÜß".contains(c))
            {
                return Some(trimmed.to_string());
            }
        }
    }

    None
}

fn decode_numeric_from_qs_valpar(blob: &[u8]) -> Option<f64> {
    if blob.len() >= 8 {
        let v = f64::from_le_bytes([
            blob[0], blob[1], blob[2], blob[3], blob[4], blob[5], blob[6], blob[7],
        ]);
        if v.is_finite() && v.abs() < 1.0e15 && (v == 0.0 || v.abs() > 1.0e-100) {
            return Some(v);
        }
    }

    None
}

fn is_likely_value_leaf(leaf: &str) -> bool {
    let l = leaf.to_lowercase();
    if ["id", "idx", "index", "count", "anzahl", "typ", "type", "nr", "nummer"]
        .iter()
        .any(|bad| l == *bad)
    {
        return false;
    }
    l.contains("wert") || l.contains("val") || l.contains("value") || l.contains("result")
}

/// Extract per-sample evaluated test results from:
/// Document/Body/batch/Series/SeriesElements/Elem{sample}/.../EvalContext/ParamContext/ParameterListe/Elem{param}
#[pyfunction]
fn zs2_parameterliste_results_to_parquet(input_zs2: &str, output_parquet: &str) -> PyResult<()> {
    let f = File::open(input_zs2)?;
    let mut gz = GzDecoder::new(BufReader::new(f));
    let mut data = Vec::<u8>::new();
    gz.read_to_end(&mut data)?;

    if data.len() < 4 || data[0..4] != [0xAF, 0xBE, 0xAD, 0xDE] {
        return Err(Zs2Error::BadMarker.into());
    }

    #[derive(Default, Clone)]
    struct ResultData {
        name: String,
        unit: String,
        value_text: Option<String>,
        value_numeric: Option<f64>,
    }

    let mut results_by_key: HashMap<(u32, u32), ResultData> = HashMap::new();
    let mut global_param_defs: HashMap<u32, (String, String)> = HashMap::new();

    let mut i = 4usize;
    let n = data.len();
    let mut name_stack: Vec<String> = Vec::with_capacity(8);

    while i < n {
        if data[i] == 0xFF {
            if !name_stack.is_empty() {
                name_stack.pop();
            }
            i += 1;
            continue;
        }

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
        let path = name_stack.join("/") + "/" + &name;
        let key = extract_sample_and_parameter_index(&path);
        let leaf = path.rsplit('/').next().unwrap_or("");

        match dtype {
            0xAA | 0x00 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let raw = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;
                let char_count = (raw & 0x7FFF_FFFF) as usize;
                let need = char_count * 2;
                ensure_len(i + need, n, i)?;
                let text = decode_utf16le(&data[i..i + need])?;
                i += need;

                if let Some(k) = key {
                    let entry = results_by_key.entry(k).or_default();
                    let trimmed = text.trim();
                    if path.ends_with("/Name/Text") {
                        if entry.name.is_empty() {
                            entry.name = trimmed.to_string();
                        }
                    } else if path.ends_with("/EinheitName") {
                        if entry.unit.is_empty() {
                            entry.unit = trimmed.to_string();
                        }
                    } else if is_likely_value_leaf(leaf) && !trimmed.is_empty() {
                        if entry.value_text.is_none() {
                            entry.value_text = Some(trimmed.to_string());
                        }
                        let normalized = trimmed.replace(',', ".");
                        if entry.value_numeric.is_none() {
                            if let Ok(v) = normalized.parse::<f64>() {
                                entry.value_numeric = Some(v);
                            }
                        }
                    }
                } else if path.contains("/Series/EvalContext/ParamContext/EigenschaftenListe/") {
                    if let Some(param_idx) = extract_elem_index(&path) {
                        let def = global_param_defs.entry(param_idx).or_insert((String::new(), String::new()));
                        let trimmed = text.trim();
                        if path.ends_with("/Name/Text") && !trimmed.is_empty() {
                            def.0 = trimmed.to_string();
                        } else if path.ends_with("/EinheitName") && !trimmed.is_empty() {
                            def.1 = trimmed.to_string();
                        }
                    }
                }
            }

            0x22 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let val = i32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as f64;
                i += 4;

                if let Some(k) = key {
                    if is_likely_value_leaf(leaf) {
                        results_by_key.entry(k).or_default().value_numeric = Some(val);
                    }
                }
            }

            0x44 => {
                ensure_len(i + 1 + 4, n, i)?;
                i += 1;
                let val = f32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as f64;
                i += 4;

                if let Some(k) = key {
                    if is_likely_value_leaf(leaf) {
                        results_by_key.entry(k).or_default().value_numeric = Some(val);
                    }
                }
            }

            0xCC => {
                ensure_len(i + 1 + 8, n, i)?;
                i += 1;
                let val = f64::from_le_bytes([
                    data[i], data[i + 1], data[i + 2], data[i + 3],
                    data[i + 4], data[i + 5], data[i + 6], data[i + 7],
                ]);
                i += 8;

                if let Some(k) = key {
                    if is_likely_value_leaf(leaf) {
                        results_by_key.entry(k).or_default().value_numeric = Some(val);
                    }
                }
            }

            0xDD => {
                ensure_len(i + 2, n, i)?;
                let len = data[i + 1] as usize;
                ensure_len(i + 2 + len, n, i)?;
                i += 2 + len;
                name_stack.push(name);
            }

            0xEE => {
                ensure_len(i + 1 + 2 + 4, n, i)?;
                i += 1;
                let sub = u16::from_le_bytes([data[i], data[i + 1]]);
                i += 2;
                let cnt = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
                i += 4;

                let bytes_per_item: usize = match sub {
                    0x0004 | 0x0016 => 4,
                    0x0005 => 8,
                    0x0011 => 1,
                    _ => 0,
                };
                let need = (cnt as usize) * bytes_per_item;
                ensure_len(i + need, n, i)?;

                if let Some(k) = key {
                    if sub == 0x0011 {
                        let blob = &data[i..i + need];
                        let entry = results_by_key.entry(k).or_default();

                        if leaf == "QS_TextPar" || leaf.contains("TextPar") {
                            if let Some(decoded) = decode_first_utf16_segment_from_blob(blob) {
                                if entry.value_text.is_none() {
                                    entry.value_text = Some(decoded);
                                }
                            }
                        } else if leaf == "QS_ValPar" || leaf.contains("ValPar") {
                            if entry.value_numeric.is_none() {
                                entry.value_numeric = decode_numeric_from_qs_valpar(blob);
                            }
                            if entry.value_text.is_none() {
                                if let Some(decoded) = decode_first_utf16_segment_from_blob(blob) {
                                    entry.value_text = Some(decoded);
                                }
                            }
                        }
                    }
                }

                i += need;
            }

            0x11 | 0x33 => i += 1 + 4,
            0x55 | 0x66 => i += 1 + 2,
            0x88 | 0x99 => i += 1 + 1,
            0xBB => i += 1 + 4,

            _ => {
                return Err(Zs2Error::Parse {
                    offset: i,
                    msg: format!("Unknown data type tag 0x{dtype:02X}"),
                }
                .into())
            }
        }
    }

    let mut sample_id_builder = UInt32Builder::new();
    let mut result_id_builder = UInt32Builder::new();
    let mut result_name_builder = StringBuilder::new();
    let mut unit_builder = StringBuilder::new();
    let mut value_text_builder = StringBuilder::new();
    let mut value_builder = Float64Builder::new();

    let mut sorted_results: Vec<_> = results_by_key.into_iter().collect();
    sorted_results.sort_by_key(|((sample_id, result_id), _)| (*sample_id, *result_id));

    for ((sample_id, result_id), result_data) in sorted_results {
        let (def_name, def_unit) = global_param_defs
            .get(&result_id)
            .cloned()
            .unwrap_or((String::new(), String::new()));

        let final_name = if result_data.name.is_empty() {
            def_name
        } else {
            result_data.name.clone()
        };
        let final_unit = if result_data.unit.is_empty() {
            def_unit
        } else {
            result_data.unit.clone()
        };

        if !final_name.is_empty() &&
           (result_data.value_numeric.is_some() || result_data.value_text.is_some()) {
            sample_id_builder.append_value(sample_id);
            result_id_builder.append_value(result_id);
            result_name_builder.append_value(&final_name);
            unit_builder.append_value(&final_unit);

            if let Some(text) = &result_data.value_text {
                value_text_builder.append_value(text);
            } else {
                value_text_builder.append_null();
            }

            if let Some(val) = result_data.value_numeric {
                value_builder.append_value(val);
            } else {
                value_builder.append_null();
            }
        }
    }

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("sample_id", DataType::UInt32, false),
        Field::new("result_id", DataType::UInt32, false),
        Field::new("result_name", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, true),
        Field::new("value_text", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(sample_id_builder.finish()),
            std::sync::Arc::new(result_id_builder.finish()),
            std::sync::Arc::new(result_name_builder.finish()),
            std::sync::Arc::new(unit_builder.finish()),
            std::sync::Arc::new(value_text_builder.finish()),
            std::sync::Arc::new(value_builder.finish()),
        ],
    )
    .map_err(Zs2Error::from)?;

    let file = File::create(output_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::clone(&schema), Some(props))
        .map_err(Zs2Error::from)?;
    writer.write(&batch).map_err(Zs2Error::from)?;
    writer.close().map_err(Zs2Error::from)?;

    Ok(())
}

fn infer_unit_from_channel_name(channel_name: &str) -> &'static str {
    let lower = channel_name.to_lowercase();

    if lower.contains("kraft") {
        "N"
    } else if lower.contains("weg") || lower.contains("dehnung") {
        "mm"
    } else if lower.contains("zeit") || lower.contains("datum") {
        "s"
    } else if lower.contains("belastungspunkt") {
        "index"
    } else {
        ""
    }
}

fn infer_unit_from_unit_table_name(unit_table_name: &str) -> Option<&'static str> {
    match unit_table_name {
        "UT_Displacement" | "UT_Length" => Some("mm"),
        "UT_Force" => Some("N"),
        "UT_Force/Area" | "UT_Stress" => Some("MPa"),
        "UT_Time" => Some("s"),
        "UT_Temperature" => Some("°C"),
        "UT_Velocity" => Some("mm/s"),
        "UT_Force/Time" => Some("N/s"),
        "UT_Strain/Time" => Some("1/s"),
        "UT_NoUnit" => Some(""),
        _ => None,
    }
}

#[pymodule]
fn zs2fast(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zs2_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(zs2_channels_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(zs2_evaluated_params_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(zs2_parameterliste_results_to_parquet, m)?)?;
    Ok(())
}
