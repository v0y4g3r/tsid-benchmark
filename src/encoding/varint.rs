//! Varint (LEB128) encoding for space-efficient integer storage.
//!
//! Format: `[num_entries: varint][column_id: varint][len: varint][bytes]...`
//!
//! Uses variable-length encoding for integers, saving space when values are small.

use super::RowEncoder;

/// Varint encoder using LEB128 variable-length integers.
#[derive(Debug, Clone, Copy, Default)]
pub struct VarintEncoder;

impl RowEncoder for VarintEncoder {
    fn name(&self) -> &'static str {
        "varint"
    }

    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, String)]) {
        encode_varint(buffer, row.len() as u32);
        for (col_id, value) in row {
            encode_varint(buffer, *col_id);
            encode_varint(buffer, value.len() as u32);
            buffer.extend_from_slice(value.as_bytes());
        }
    }

    fn decode(&self, data: &[u8]) -> Vec<(u32, String)> {
        let mut result = Vec::new();
        let mut offset = 0;

        let (num_entries, bytes) = decode_varint(&data[offset..]);
        offset += bytes;

        for _ in 0..num_entries {
            let (col_id, bytes) = decode_varint(&data[offset..]);
            offset += bytes;
            let (len, bytes) = decode_varint(&data[offset..]);
            offset += bytes;
            let len = len as usize;
            let value = String::from_utf8(data[offset..offset + len].to_vec()).unwrap();
            offset += len;
            result.push((col_id, value));
        }
        result
    }
}

/// Encode a u32 as varint (LEB128).
pub fn encode_varint(buffer: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint (LEB128) from a slice, returning (value, bytes_read).
pub fn decode_varint(data: &[u8]) -> (u32, usize) {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (result, bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::tests as test_helpers;

    #[test]
    fn roundtrip() {
        test_helpers::test_roundtrip(&VarintEncoder);
    }

    #[test]
    fn roundtrip_empty() {
        test_helpers::test_roundtrip_empty(&VarintEncoder);
    }

    #[test]
    fn roundtrip_special_chars() {
        test_helpers::test_roundtrip_special_chars(&VarintEncoder);
    }

    #[test]
    fn roundtrip_large_col_ids() {
        test_helpers::test_roundtrip_large_col_ids(&VarintEncoder);
    }

    #[test]
    fn varint_encoding() {
        let test_values = [0u32, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX];

        for &val in &test_values {
            let mut buffer = Vec::new();
            encode_varint(&mut buffer, val);
            let (decoded, _) = decode_varint(&buffer);
            assert_eq!(decoded, val, "Failed for value {}", val);
        }
    }
}
