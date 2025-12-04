//! Memcomparable encoding for sortable binary keys.
//!
//! Uses the memcomparable serialization format which produces byte sequences
//! that can be compared lexicographically to determine ordering.

use memcomparable::Serializer;
use serde::Serialize;

use super::RowEncoder;

/// Memcomparable encoder for sortable binary encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemcomparableEncoder;

impl RowEncoder for MemcomparableEncoder {
    fn name(&self) -> &'static str {
        "memcomparable"
    }

    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, &str)]) {
        let mut serializer = Serializer::new(buffer);
        for (col_id, value) in row {
            col_id.serialize(&mut serializer).unwrap();
            value.serialize(&mut serializer).unwrap();
        }
    }

    fn decode(&self, _data: &[u8]) -> Vec<(u32, String)> {
        // Note: memcomparable deserialization is not implemented as it requires
        // a deserializer which is more complex. For benchmarking purposes,
        // we focus on the encoding path.
        unimplemented!("memcomparable decoding is not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode() {
        let encoder = MemcomparableEncoder;
        let pairs: Vec<(u32, &str)> = vec![(0, "value_0"), (1, "value_1")];

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);

        // Just verify encoding produces output
        assert!(!buffer.is_empty());
    }
}
