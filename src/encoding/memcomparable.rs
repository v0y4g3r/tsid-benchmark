//! Memcomparable encoding for sortable binary keys.
//!
//! Uses the memcomparable serialization format which produces byte sequences
//! that can be compared lexicographically to determine ordering.

use memcomparable::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::RowEncoder;

/// Memcomparable encoder for sortable binary encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemcomparableEncoder;

impl RowEncoder for MemcomparableEncoder {
    fn name(&self) -> &'static str {
        "memcomparable"
    }

    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, String)]) {
        let mut serializer = Serializer::new(buffer);
        for (col_id, value) in row {
            col_id.serialize(&mut serializer).unwrap();
            value.serialize(&mut serializer).unwrap();
        }
    }

    fn decode(&self, data: &[u8]) -> Vec<(u32, String)> {
        let mut res = vec![];
        let mut des = Deserializer::new(data);
        while des.has_remaining() {
            let column_id = u32::deserialize(&mut des).unwrap();
            let value: String = String::deserialize(&mut des).unwrap();
            res.push((column_id, value));
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode() {
        let encoder = MemcomparableEncoder;
        let pairs: Vec<(u32, String)> = vec![(0, "value_0".to_owned()), (1, "value_1".to_owned())];

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);

        // Just verify encoding produces output
        assert!(!buffer.is_empty());
    }

    #[test]
    fn roundtrip() {
        crate::encoding::tests::test_roundtrip(&MemcomparableEncoder);
    }

    #[test]
    fn roundtrip_empty() {
        crate::encoding::tests::test_roundtrip_empty(&MemcomparableEncoder);
    }

    #[test]
    fn roundtrip_special_chars() {
        crate::encoding::tests::test_roundtrip_special_chars(&MemcomparableEncoder);
    }

    #[test]
    fn roundtrip_large_col_ids() {
        crate::encoding::tests::test_roundtrip_large_col_ids(&MemcomparableEncoder);
    }
}
