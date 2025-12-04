use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tsid_bench::{
    encode_to_parquet, encode_to_parquet_maparray, read_labels_and_hash, FlatBufferEncoder,
    LengthPrefixedEncoder, MemcomparableEncoder, RowEncoder, VarintEncoder,
};

/// Prepare label data and convert to (column_id, value) pairs.
fn prepare_label_data() -> (Vec<String>, Vec<Vec<String>>, Vec<Vec<(u32, String)>>) {
    let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels.csv");
    let label_names = labels.label_names;
    let label_values = labels.label_values.clone();
    let rows: Vec<Vec<(u32, String)>> = labels
        .label_values
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(idx, val)| (idx as u32, val))
                .collect()
        })
        .collect();
    (label_names, label_values, rows)
}

/// Generic encoding benchmark for any RowEncoder implementation.
fn benchmark_encoder<E: RowEncoder>(c: &mut Criterion, encoder: E) {
    let (_, _, rows) = prepare_label_data();

    let data = encode_to_parquet(&encoder, &rows).unwrap();
    println!(
        "parquet_encoding_{} file size: {} bytes ({:.2} KB)",
        encoder.name(),
        data.len(),
        data.len() as f64 / 1024.0
    );

    let bench_name = format!("parquet_encoding_{}", encoder.name());
    c.bench_function(&bench_name, |b| {
        b.iter(|| {
            encode_to_parquet(&encoder, black_box(&rows)).unwrap();
        });
    });
}

/// Generic decoding benchmark for any RowEncoder implementation.
fn benchmark_decoder<E: RowEncoder>(c: &mut Criterion, encoder: E, encoded_rows: &[Vec<u8>]) {
    let bench_name = format!("decode_{}", encoder.name());
    c.bench_function(&bench_name, |b| {
        b.iter(|| {
            for row in encoded_rows {
                black_box(encoder.decode(black_box(row)));
            }
        });
    });
}

/// Prepare encoded rows for decoding benchmarks.
fn prepare_encoded_rows<E: RowEncoder>(encoder: &E, rows: &[Vec<(u32, String)>]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| {
            let pairs: Vec<_> = row.iter().map(|(id, s)| (*id, s.as_str())).collect();
            let mut buffer = Vec::new();
            encoder.encode(&mut buffer, &pairs);
            buffer
        })
        .collect()
}

// ============================================================================
// Encoding Benchmarks
// ============================================================================

fn benchmark_length_prefixed(c: &mut Criterion) {
    benchmark_encoder(c, LengthPrefixedEncoder);
}

fn benchmark_varint(c: &mut Criterion) {
    benchmark_encoder(c, VarintEncoder);
}

fn benchmark_memcomparable(c: &mut Criterion) {
    benchmark_encoder(c, MemcomparableEncoder);
}

fn benchmark_flatbuffer(c: &mut Criterion) {
    benchmark_encoder(c, FlatBufferEncoder);
}

fn benchmark_maparray(c: &mut Criterion) {
    let (label_names, label_values, _) = prepare_label_data();

    let data = encode_to_parquet_maparray(&label_names, &label_values).unwrap();
    println!(
        "parquet_encoding_maparray file size: {} bytes ({:.2} KB)",
        data.len(),
        data.len() as f64 / 1024.0
    );

    c.bench_function("parquet_encoding_maparray", |b| {
        b.iter(|| {
            encode_to_parquet_maparray(black_box(&label_names), black_box(&label_values)).unwrap();
        });
    });
}

// ============================================================================
// Decoding Benchmarks
// ============================================================================

fn benchmark_decode_length_prefixed(c: &mut Criterion) {
    let (_, _, rows) = prepare_label_data();
    let encoder = LengthPrefixedEncoder;
    let encoded_rows = prepare_encoded_rows(&encoder, &rows);
    benchmark_decoder(c, encoder, &encoded_rows);
}

fn benchmark_decode_varint(c: &mut Criterion) {
    let (_, _, rows) = prepare_label_data();
    let encoder = VarintEncoder;
    let encoded_rows = prepare_encoded_rows(&encoder, &rows);
    benchmark_decoder(c, encoder, &encoded_rows);
}

fn benchmark_decode_flatbuffer(c: &mut Criterion) {
    let (_, _, rows) = prepare_label_data();
    let encoder = FlatBufferEncoder;
    let encoded_rows = prepare_encoded_rows(&encoder, &rows);
    benchmark_decoder(c, encoder, &encoded_rows);
}

fn benchmark_decode_flatbuffer_zero_copy(c: &mut Criterion) {
    let (_, _, rows) = prepare_label_data();
    let encoder = FlatBufferEncoder;
    let encoded_rows = prepare_encoded_rows(&encoder, &rows);

    c.bench_function("decode_flatbuffer_zero_copy", |b| {
        b.iter(|| {
            for row in &encoded_rows {
                // Only parse the root, don't iterate through entries
                let primary_keys = tsid_bench::generated::root_as_primary_keys(black_box(row))
                    .expect("Failed to decode");
                black_box(primary_keys.label_values());
            }
        });
    });
}

criterion_group!(
    benches,
    // Encoding benchmarks
    benchmark_length_prefixed,
    benchmark_varint,
    benchmark_memcomparable,
    benchmark_flatbuffer,
    benchmark_maparray,
    // Decoding benchmarks
    benchmark_decode_length_prefixed,
    benchmark_decode_varint,
    benchmark_decode_flatbuffer,
    benchmark_decode_flatbuffer_zero_copy,
);
criterion_main!(benches);
