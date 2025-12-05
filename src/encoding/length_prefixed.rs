//! Length-prefixed binary encoding.
//!
//! Format: `[num_entries: u32][column_id: u32][len: u32][bytes]...`
//!
//! Simple and fast encoding using fixed-size 4-byte headers for all integers.

use super::RowEncoder;

/// Length-prefixed encoder using fixed 4-byte integers.
#[derive(Debug, Clone, Copy, Default)]
pub struct LengthPrefixedEncoder;

impl RowEncoder for LengthPrefixedEncoder {
    fn name(&self) -> &'static str {
        "length_prefixed"
    }

    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, String)]) {
        buffer.extend_from_slice(&(row.len() as u32).to_le_bytes());
        for (col_id, value) in row {
            buffer.extend_from_slice(&col_id.to_le_bytes());
            buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value.as_bytes());
        }
    }

    fn decode(&self, data: &[u8]) -> Vec<(u32, String)> {
        let mut result = Vec::new();
        let mut offset = 0;

        let num_entries = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..num_entries {
            let col_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let value = String::from_utf8(data[offset..offset + len].to_vec()).unwrap();
            offset += len;
            result.push((col_id, value));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::tests as test_helpers;

    #[test]
    fn roundtrip() {
        test_helpers::test_roundtrip(&LengthPrefixedEncoder);
    }

    #[test]
    fn roundtrip_empty() {
        test_helpers::test_roundtrip_empty(&LengthPrefixedEncoder);
    }

    #[test]
    fn roundtrip_special_chars() {
        test_helpers::test_roundtrip_special_chars(&LengthPrefixedEncoder);
    }

    #[test]
    fn roundtrip_large_col_ids() {
        test_helpers::test_roundtrip_large_col_ids(&LengthPrefixedEncoder);
    }
}
