//! FlatBuffer encoding for zero-copy deserialization.
//!
//! Uses the FlatBuffers schema defined in `schemas/label_row.fbs`.
//! Provides efficient zero-copy access during reads.

use flatbuffers::FlatBufferBuilder;

use crate::generated::{
    finish_primary_keys_buffer, root_as_primary_keys, LabelAndColumnId, LabelAndColumnIdArgs,
    PrimaryKeys, PrimaryKeysArgs,
};

use super::RowEncoder;

/// FlatBuffer encoder for zero-copy deserialization.
#[derive(Debug, Clone, Copy, Default)]
pub struct FlatBufferEncoder;

impl RowEncoder for FlatBufferEncoder {
    fn name(&self) -> &'static str {
        "flatbuffer"
    }

    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, &str)]) {
        let mut fb_builder = FlatBufferBuilder::new();
        let label_entries: Vec<_> = row
            .iter()
            .map(|(col_idx, value)| {
                let label_value = fb_builder.create_string(value);
                LabelAndColumnId::create(
                    &mut fb_builder,
                    &LabelAndColumnIdArgs {
                        column_id: *col_idx,
                        label_value: Some(label_value),
                    },
                )
            })
            .collect();

        let label_values_vec = fb_builder.create_vector(&label_entries);
        let primary_keys = PrimaryKeys::create(
            &mut fb_builder,
            &PrimaryKeysArgs {
                label_values: Some(label_values_vec),
            },
        );
        finish_primary_keys_buffer(&mut fb_builder, primary_keys);
        buffer.extend_from_slice(fb_builder.finished_data());
    }

    fn decode(&self, data: &[u8]) -> Vec<(u32, String)> {
        let primary_keys = root_as_primary_keys(data).expect("Failed to decode FlatBuffer");
        let label_values = primary_keys
            .label_values()
            .expect("label_values should be present");

        label_values
            .iter()
            .map(|entry| {
                (
                    entry.column_id(),
                    entry.label_value().unwrap_or("").to_string(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::tests as test_helpers;

    #[test]
    fn roundtrip() {
        test_helpers::test_roundtrip(&FlatBufferEncoder);
    }

    #[test]
    fn roundtrip_empty() {
        test_helpers::test_roundtrip_empty(&FlatBufferEncoder);
    }

    #[test]
    fn roundtrip_special_chars() {
        test_helpers::test_roundtrip_special_chars(&FlatBufferEncoder);
    }

    #[test]
    fn roundtrip_large_col_ids() {
        test_helpers::test_roundtrip_large_col_ids(&FlatBufferEncoder);
    }
}
