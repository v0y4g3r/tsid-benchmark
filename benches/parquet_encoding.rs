use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tsid_bench::{
    encode_to_parquet_flatbuffer, encode_to_parquet_maparray, encode_to_parquet_memcomparable,
    read_labels_and_hash,
};

// Prepare label data: read from CSV, scale by 10x, and shuffle
fn prepare_label_data() -> (Vec<String>, Vec<Vec<String>>) {
    let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels.csv");
    let label_names = labels.label_names;
    let label_values = labels.label_values;
    let scaled_label_values = label_values;
    (label_names, scaled_label_values)
}

fn benchmark_memcomparable(c: &mut Criterion) {
    let (label_names, scaled_label_values) = prepare_label_data();

    // Measure file size once
    let data = encode_to_parquet_memcomparable(&label_names, &scaled_label_values).unwrap();
    let file_size = data.len();
    println!(
        "parquet_encoding_memcomparable file size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    c.bench_function("parquet_encoding_memcomparable", |b| {
        b.iter(|| {
            encode_to_parquet_memcomparable(
                black_box(&label_names),
                black_box(&scaled_label_values),
            )
            .unwrap();
        });
    });
}

fn benchmark_flatbuffer(c: &mut Criterion) {
    let (label_names, scaled_label_values) = prepare_label_data();

    // Measure file size once
    let data = encode_to_parquet_flatbuffer(&label_names, &scaled_label_values).unwrap();
    let file_size = data.len();
    println!(
        "parquet_encoding_flatbuffer file size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    c.bench_function("parquet_encoding_flatbuffer", |b| {
        b.iter(|| {
            encode_to_parquet_flatbuffer(black_box(&label_names), black_box(&scaled_label_values))
                .unwrap();
        });
    });
}

fn benchmark_maparray(c: &mut Criterion) {
    let (label_names, scaled_label_values) = prepare_label_data();

    // Measure file size once
    let data = encode_to_parquet_maparray(&label_names, &scaled_label_values).unwrap();
    let file_size = data.len();
    println!(
        "parquet_encoding_maparray file size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    c.bench_function("parquet_encoding_maparray", |b| {
        b.iter(|| {
            encode_to_parquet_maparray(black_box(&label_names), black_box(&scaled_label_values))
                .unwrap();
        });
    });
}

criterion_group!(
    benches,
    benchmark_memcomparable,
    benchmark_flatbuffer,
    benchmark_maparray
);
criterion_main!(benches);
