use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, Cursor, Read};
use std::sync::Arc;

use arrow::array::{Array, BinaryBuilder, MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use crate::ts_id_gen::{SeededHasher, TsIdGenerator};

pub mod data_reader;
pub mod encoding;
pub mod generated;
pub mod ts_id_gen;

// Re-export encoding types for convenience
pub use encoding::{
    FlatBufferEncoder, LengthPrefixedEncoder, MemcomparableEncoder, RowEncoder, VarintEncoder,
};

pub struct Labels {
    pub label_names: Vec<String>,
    pub label_name_hash: u64,
    pub label_values: Vec<Vec<String>>,
}

/// Create a reader from a file path, automatically handling gzip compression.
///
/// If the path ends with `.gz`, the file is decompressed using gzip.
pub fn open_csv_reader(path: &str) -> Box<dyn Read> {
    let file = File::open(path).expect("Failed to open file");
    if path.ends_with(".gz") {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    }
}

/// Read labels from a CSV reader and compute the label name hash.
pub fn read_labels_and_hash<H>(reader: Box<dyn Read>) -> Labels
where
    H: Default + Hasher + SeededHasher,
{
    let mut csv_reader = csv::ReaderBuilder::new().from_reader(reader);

    let label_names: Vec<String> = csv_reader
        .headers()
        .expect("Failed to read headers")
        .iter()
        .map(|s| s.to_owned())
        .collect();

    let mut generator = TsIdGenerator::<H>::default();
    generator.write_label_names(label_names.iter().map(|s| s.as_bytes()));
    let label_name_hash = generator.build_ts_id();

    let label_values: Vec<Vec<String>> = csv_reader
        .records()
        .map(|record| {
            record
                .expect("Failed to read record")
                .iter()
                .map(|s| s.to_owned())
                .collect()
        })
        .collect();

    Labels {
        label_names,
        label_name_hash,
        label_values,
    }
}

// ============================================================================
// Parquet encoding functions
// ============================================================================

/// Encode rows to parquet using any RowEncoder implementation.
pub fn encode_to_parquet<E: RowEncoder + ?Sized>(
    encoder: &E,
    rows: &[Vec<(u32, String)>],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![Field::new("primary_key", DataType::Binary, false)]);
    let schema = Arc::new(schema);

    let mut builder = BinaryBuilder::new();
    let mut encoded_row = Vec::new();
    for row in rows {
        encoder.encode(&mut encoded_row, row);
        builder.append_value(&encoded_row);
        encoded_row.clear();
    }

    let array = Arc::new(builder.finish());
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![array])?;

    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}

/// Encode using MapArray in Arrow (special case - uses label names as keys).
pub fn encode_to_parquet_maparray(
    label_names: &[String],
    label_values: &[Vec<String>],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let key_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut map_builder = MapBuilder::new(None, key_builder, value_builder);

    for row in label_values {
        map_builder.append(true)?;
        for (label_name, value) in label_names.iter().zip(row.iter()) {
            map_builder.keys().append_value(label_name);
            map_builder.values().append_value(value);
        }
    }

    let map_array = map_builder.finish();
    let map_field = Field::new("labels", map_array.data_type().clone(), false);
    let schema = Schema::new(vec![map_field]);
    let schema = Arc::new(schema);

    let map_array = Arc::new(map_array);
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![map_array])?;

    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_column_dictionary_enabled(
            ColumnPath::new(vec![
                "labels".to_owned(),
                "entries".to_owned(),
                "keys".to_owned(),
            ]),
            true,
        )
        .set_column_dictionary_enabled(
            ColumnPath::new(vec![
                "labels".to_owned(),
                "entries".to_owned(),
                "values".to_owned(),
            ]),
            true,
        )
        .build();
    let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hash::DefaultHasher;

    use fxhash::FxHasher64;
    use xxhash_rust::xxh3::Xxh3;
    use xxhash_rust::xxh64::Xxh64;

    use super::*;

    fn test_hasher<H: Hasher + Default + SeededHasher>(amp: usize) {
        let labels = read_labels_and_hash::<H>(open_csv_reader("./assets/labels.csv.gz"));
        let mut all_hash_codes = HashSet::with_capacity(labels.label_values.len());

        for label in labels.label_values.iter() {
            for idx in 0..amp {
                let mut ts_id_gen = TsIdGenerator::<H>::from_seed(labels.label_name_hash);
                let label_values: Vec<_> = label
                    .iter()
                    .map(|s| format!("{}-{}", s, idx).into_bytes())
                    .collect();
                ts_id_gen.write_label_values(label_values.iter().map(|a| a.as_slice()));
                let i = ts_id_gen.build_ts_id();
                assert!(!all_hash_codes.contains(&i));
                all_hash_codes.insert(i);
            }
        }
    }

    #[ignore]
    #[test]
    fn check_collisions() {
        let amp = 100_000_000usize / 660;
        test_hasher::<Xxh3>(amp);
        test_hasher::<Xxh64>(amp);
        test_hasher::<FxHasher64>(amp);
        test_hasher::<DefaultHasher>(amp);
    }

    fn to_pairs(label_values: &[Vec<String>]) -> Vec<Vec<(u32, String)>> {
        label_values
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(idx, val)| (idx as u32, val.clone()))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_encode_maparray() {
        let labels = read_labels_and_hash::<DefaultHasher>(open_csv_reader("./assets/labels.csv.gz"));
        let encoded =
            encode_to_parquet_maparray(&labels.label_names, &labels.label_values).unwrap();
        println!("maparray size: {:.2}k", encoded.len() as f64 / 1024.0);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_with_trait() {
        let labels = read_labels_and_hash::<DefaultHasher>(open_csv_reader("./assets/labels.csv.gz"));
        let rows = to_pairs(&labels.label_values);

        // Test all encoders using the trait
        let encoders: Vec<Box<dyn RowEncoder>> = vec![
            Box::new(LengthPrefixedEncoder),
            Box::new(VarintEncoder),
            Box::new(MemcomparableEncoder),
            Box::new(FlatBufferEncoder),
        ];

        for encoder in &encoders {
            let encoded = encode_to_parquet(encoder.as_ref(), &rows).unwrap();
            println!(
                "{} size: {:.2}k",
                encoder.name(),
                encoded.len() as f64 / 1024.0
            );
            assert!(!encoded.is_empty());
        }
    }
}
