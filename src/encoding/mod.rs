//! Row encoding implementations for different serialization formats.
//!
//! Each encoder implements the `RowEncoder` trait which provides a unified interface
//! for encoding and decoding `(column_id, value)` pairs.

mod flatbuffer;
mod length_prefixed;
mod memcomparable;
mod varint;

pub use flatbuffer::FlatBufferEncoder;
pub use length_prefixed::LengthPrefixedEncoder;
pub use memcomparable::MemcomparableEncoder;
pub use varint::VarintEncoder;

/// A trait for encoding and decoding rows of `(column_id, value)` pairs.
///
/// Implementations should be stateless and provide efficient serialization
/// for storing label key-value pairs in binary format.
pub trait RowEncoder {
    /// Returns the name of the encoding scheme.
    fn name(&self) -> &'static str;

    /// Encodes a row of `(column_id, value)` pairs into the buffer.
    ///
    /// The buffer is not cleared before encoding, allowing for reuse.
    /// Callers should clear the buffer if needed.
    fn encode(&self, buffer: &mut Vec<u8>, row: &[(u32, String)]);

    /// Decodes a row from the given data.
    ///
    /// Returns a vector of `(column_id, value)` pairs.
    fn decode(&self, data: &[u8]) -> Vec<(u32, String)>;
}

/// Helper to encode a row and return as a new Vec.
pub fn encode_to_vec<E: RowEncoder>(encoder: &E, row: &[(u32, String)]) -> Vec<u8> {
    let mut buffer = Vec::new();
    encoder.encode(&mut buffer, row);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper to run roundtrip tests for any encoder
    pub fn test_roundtrip<E: RowEncoder>(encoder: &E) {
        let pairs: Vec<(u32, String)> = vec![
            (0, "value_0".to_owned()),
            (5, "value_5".to_owned()),
            (10, "value_10".to_owned()),
        ];

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);
        let decoded = encoder.decode(&buffer);

        assert_eq!(decoded.len(), pairs.len());
        for (i, (col_id, value)) in decoded.iter().enumerate() {
            assert_eq!(*col_id, pairs[i].0);
            assert_eq!(value, &pairs[i].1);
        }
    }

    pub fn test_roundtrip_empty<E: RowEncoder>(encoder: &E) {
        let pairs: Vec<(u32, String)> = vec![];

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);
        let decoded = encoder.decode(&buffer);

        assert_eq!(decoded.len(), 0);
    }

    pub fn test_roundtrip_special_chars<E: RowEncoder>(encoder: &E) {
        let pairs: Vec<(u32, String)> = [
            (0, "hello world"),
            (1, "with\ttab"),
            (2, "with\nnewline"),
            (3, "unicode: 你好🌍"),
            (4, ""),
        ]
        .into_iter()
        .map(|(col_id, val)| (col_id, val.to_owned()))
        .collect();

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);
        let decoded = encoder.decode(&buffer);

        assert_eq!(decoded.len(), pairs.len());
        for (i, (col_id, value)) in decoded.iter().enumerate() {
            assert_eq!(*col_id, pairs[i].0);
            assert_eq!(value, &pairs[i].1);
        }
    }

    pub fn test_roundtrip_large_col_ids<E: RowEncoder>(encoder: &E) {
        let pairs: Vec<(u32, String)> = [
            (0, "small"),
            (127, "one_byte_max"),
            (128, "two_bytes_min"),
            (16383, "two_bytes_max"),
            (16384, "three_bytes_min"),
        ]
        .into_iter()
        .map(|(col_id, val)| (col_id, val.to_owned()))
        .collect();

        let mut buffer = Vec::new();
        encoder.encode(&mut buffer, &pairs);
        let decoded = encoder.decode(&buffer);

        assert_eq!(decoded.len(), pairs.len());
        for (i, (col_id, value)) in decoded.iter().enumerate() {
            assert_eq!(*col_id, pairs[i].0);
            assert_eq!(value, &pairs[i].1);
        }
    }
}
