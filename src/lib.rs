use std::fs::File;
use std::hash::Hasher;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, BinaryBuilder, MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use csv::{StringRecord, StringRecordsIntoIter};
use flatbuffers::FlatBufferBuilder;
use memcomparable::Serializer;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde::Serialize;

use crate::generated::{
    LabelAndColumnId, LabelAndColumnIdArgs, PrimaryKeys, PrimaryKeysArgs,
    finish_primary_keys_buffer, root_as_primary_keys,
};
use crate::ts_id_gen::{SeededHasher, TsIdGenerator};

pub mod data_reader;
pub mod generated;
pub mod ts_id_gen;

pub struct LabelValuesIterator {
    label_names: Vec<String>,
    label_name_hash: u64,
    iterator: StringRecordsIntoIter<File>,
}

impl LabelValuesIterator {
    pub(crate) fn next(&mut self) -> Option<(&[String], Vec<String>, u64)> {
        let record = self.iterator.next()?;
        let record1: StringRecord = record.unwrap();
        let row = record1.iter().map(|s| s.to_owned()).collect::<Vec<_>>();
        Some((self.label_names.as_slice(), row, self.label_name_hash))
    }
}

pub struct Labels {
    pub label_names: Vec<String>,
    pub label_name_hash: u64,
    pub label_values: Vec<Vec<String>>,
}

pub fn read_labels_and_hash<H>(path: &str) -> Labels
where
    H: Default + Hasher + SeededHasher,
{
    let mut iterator = read_labels::<H>(path);
    let mut values = vec![];
    while let Some((_, labels, _)) = iterator.next() {
        values.push(labels);
    }
    Labels {
        label_names: iterator.label_names,
        label_name_hash: iterator.label_name_hash,
        label_values: values,
    }
}

fn read_labels<H>(path: &str) -> LabelValuesIterator
where
    H: Default + Hasher + SeededHasher,
{
    let mut result = csv::ReaderBuilder::new().from_path(path).unwrap();
    let label_names = result.headers().unwrap();
    let label_names = label_names
        .iter()
        .map(|s| s.to_owned())
        .collect::<Vec<String>>();

    let mut generator = TsIdGenerator::<H>::default();
    generator.write_label_names(label_names.iter().map(|s| s.as_bytes()));
    let label_name_hash = generator.build_ts_id();
    let iter = result.into_records();
    LabelValuesIterator {
        label_names,
        label_name_hash,
        iterator: iter,
    }
}

// Method 3: Use MapArray in Arrow
pub fn encode_to_parquet_maparray(
    label_names: &[String],
    label_values: &[Vec<String>],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Build MapArray first to get the correct schema
    let key_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut map_builder = MapBuilder::new(None, key_builder, value_builder);

    for row in label_values {
        map_builder.append(true)?;
        // Use label names (field names) as map keys, paired with their corresponding values
        for (label_name, value) in label_names.iter().zip(row.iter()) {
            map_builder.keys().append_value(label_name);
            map_builder.values().append_value(value);
        }
    }

    let map_array = map_builder.finish();

    // Get the schema from the MapArray
    let map_field = Field::new("labels", map_array.data_type().clone(), false);
    let schema = Schema::new(vec![map_field]);
    let schema = Arc::new(schema);

    let map_array = Arc::new(map_array);
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![map_array])?;

    // Write to memory parquet
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

fn encode_row_flatbuffer<'a>(
    fb_builder: &mut FlatBufferBuilder,
    row: impl Iterator<Item = (u32, &'a String)>,
) {
    let label_entries: Vec<_> = row
        .map(|(col_idx, value)| {
            let label_value = fb_builder.create_string(value);
            LabelAndColumnId::create(
                fb_builder,
                &LabelAndColumnIdArgs {
                    column_id: col_idx,
                    label_value: Some(label_value),
                },
            )
        })
        .collect();

    let label_values_vec = fb_builder.create_vector(&label_entries);
    let primary_keys = PrimaryKeys::create(
        fb_builder,
        &PrimaryKeysArgs {
            label_values: Some(label_values_vec),
        },
    );
    finish_primary_keys_buffer(fb_builder, primary_keys);
}

// Method 2: Encode using flatbuffer and write to parquet as binary array
pub fn encode_to_parquet_flatbuffer(
    _label_names: &[String],
    label_values: &[Vec<String>],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Create schema with a single binary column for encoded primary keys
    let schema = Schema::new(vec![Field::new("primary_key", DataType::Binary, false)]);
    let schema = Arc::new(schema);

    let mut builder = BinaryBuilder::new();
    for row in label_values {
        let mut fb_builder = flatbuffers::FlatBufferBuilder::new();
        encode_row_flatbuffer(
            &mut fb_builder,
            row.iter().enumerate().map(|(idx, val)| (idx as u32, val)),
        );
        builder.append_value(fb_builder.finished_data());
    }

    let array = Arc::new(builder.finish());
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![array])?;

    // Write to memory parquet
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}

// Method 1: Encode using memcomparable and write to parquet as binary array
pub fn encode_to_parquet_memcomparable(
    _label_names: &[String],
    label_values: &[Vec<String>],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Create schema with a single binary column for encoded primary keys
    let schema = Schema::new(vec![Field::new("primary_key", DataType::Binary, false)]);
    let schema = Arc::new(schema);

    // Encode all rows
    let mut builder = BinaryBuilder::new();
    let mut encoded_row = Vec::new();
    for row in label_values {
        // Reuse serializer for all pairs in this row
        let mut serializer = Serializer::new(&mut encoded_row);
        for (col_idx, value) in row.iter().enumerate() {
            let column_id = col_idx as u32;
            // Serialize column_id first
            column_id.serialize(&mut serializer).unwrap();
            // Then serialize value
            value.serialize(&mut serializer).unwrap();
        }
        // Get the encoded buffer from serializer
        let _ = serializer.into_inner();
        builder.append_value(&encoded_row);
        encoded_row.clear();
    }

    let array = Arc::new(builder.finish());
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![array])?;

    // Write to memory parquet
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let props = WriterProperties::builder().build();
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
        let labels = read_labels_and_hash::<H>("./labels.csv");
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

    // Check hash collision in 100 million label combinations.
    // This is important because we use tsid to distinguish
    // between different time series.
    #[test]
    fn check_collisions() {
        let amp = 100_000_000usize / 660;
        test_hasher::<Xxh3>(amp);
        test_hasher::<Xxh64>(amp);
        test_hasher::<FxHasher64>(amp);
        test_hasher::<DefaultHasher>(amp);
    }

    #[test]
    fn test_encode_maparray() {
        let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels-2-truncated.csv");
        let label_names = labels.label_names;
        let label_values = labels.label_values;

        let size = encode_to_parquet_maparray(&label_names, &label_values).unwrap();

        println!("size: {}k", size.len() as f64 / 1024.0);
        std::fs::write("./maparray.parqyet", size).unwrap();
    }

    #[test]
    fn test_encode_memcomparable() {
        let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels-2-truncated.csv");
        let label_names = labels.label_names;
        let label_values = labels.label_values;

        let size = encode_to_parquet_memcomparable(&label_names, &label_values).unwrap();

        println!("size: {}k", size.len() as f64 / 1024.0);
        std::fs::write("./memcomparable.parqyet", size).unwrap();
    }

    #[test]
    fn test_encode_flatbuffer() {
        let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels-2-truncated.csv");
        let label_names = labels.label_names;
        let label_values = labels.label_values;

        let size = encode_to_parquet_flatbuffer(&label_names, &label_values).unwrap();

        println!("size: {}k", size.len() as f64 / 1024.0);
        std::fs::write("./flatbuffer.parquet", size).unwrap();
    }

    #[test]
    fn test_encode_row_flatbuffer_roundtrip() {
        // Test data: simulate label values with their column indices
        let row = vec![
            "value_0".to_string(),
            "value_1".to_string(),
            "value_2".to_string(),
        ];

        // Encode
        let mut fb_builder = flatbuffers::FlatBufferBuilder::new();
        encode_row_flatbuffer(
            &mut fb_builder,
            row.iter().enumerate().map(|(idx, val)| (idx as u32, val)),
        );
        let encoded_data = fb_builder.finished_data();

        // Decode and verify
        let primary_keys = root_as_primary_keys(encoded_data).expect("Failed to decode FlatBuffer");
        let label_values = primary_keys
            .label_values()
            .expect("label_values should be present");

        assert_eq!(label_values.len(), row.len());

        for (i, label_entry) in label_values.iter().enumerate() {
            assert_eq!(label_entry.column_id(), i as u32);
            assert_eq!(
                label_entry.label_value(),
                Some(row[i].as_str()),
                "Mismatch at index {}",
                i
            );
        }
    }

    #[test]
    fn test_encode_row_flatbuffer_roundtrip_empty() {
        // Test with empty row
        let row: Vec<String> = vec![];

        let mut fb_builder = flatbuffers::FlatBufferBuilder::new();
        encode_row_flatbuffer(
            &mut fb_builder,
            row.iter().enumerate().map(|(idx, val)| (idx as u32, val)),
        );
        let encoded_data = fb_builder.finished_data();

        let primary_keys = root_as_primary_keys(encoded_data).expect("Failed to decode FlatBuffer");
        let label_values = primary_keys
            .label_values()
            .expect("label_values should be present");

        assert_eq!(label_values.len(), 0);
    }

    #[test]
    fn test_encode_row_flatbuffer_roundtrip_with_special_chars() {
        // Test with special characters and unicode
        let row = vec![
            "hello world".to_string(),
            "with\ttab".to_string(),
            "with\nnewline".to_string(),
            "unicode: 你好🌍".to_string(),
            "".to_string(), // empty string
        ];

        let mut fb_builder = flatbuffers::FlatBufferBuilder::new();
        encode_row_flatbuffer(
            &mut fb_builder,
            row.iter().enumerate().map(|(idx, val)| (idx as u32, val)),
        );
        let encoded_data = fb_builder.finished_data();

        let primary_keys = root_as_primary_keys(encoded_data).expect("Failed to decode FlatBuffer");
        let label_values = primary_keys
            .label_values()
            .expect("label_values should be present");

        assert_eq!(label_values.len(), row.len());

        for (i, label_entry) in label_values.iter().enumerate() {
            assert_eq!(label_entry.column_id(), i as u32);
            assert_eq!(
                label_entry.label_value(),
                Some(row[i].as_str()),
                "Mismatch at index {}",
                i
            );
        }
    }
}
