use std::hash::Hasher;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tsid_bench::read_labels_and_hash;
use tsid_bench::ts_id_gen::{SeededHasher, TsIdGenerator};
use xxhash_rust::xxh64::Xxh64;

fn benchmark_hasher<H, F>(c: &mut Criterion, name: &str, create_hasher: F)
where
    H: Hasher + SeededHasher,
    F: Fn() -> H,
{
    // Read labels from CSV - use DefaultHasher for reading since we just need the data
    let labels = read_labels_and_hash::<std::hash::DefaultHasher>("./labels.csv");
    let label_names: &Vec<String> = &labels.label_names;
    let label_values: &Vec<Vec<String>> = &labels.label_values;

    // Benchmark complete tsid generation: write_label_names + write_label_values + build_ts_id
    c.bench_function(&format!("{}", name), |b| {
        b.iter(|| {
            for label_value_row in label_values.iter() {
                let mut generator = TsIdGenerator::new(create_hasher());
                generator.write_label_names(black_box(
                    label_names.iter().map(|s: &String| s.as_bytes()),
                ));
                generator.write_label_values(black_box(
                    label_value_row.iter().map(|s: &String| s.as_bytes()),
                ));
                let tsid = black_box(generator.build_ts_id());
                black_box(tsid);
            }
        });
    });
}

fn benchmark_default_hasher(c: &mut Criterion) {
    benchmark_hasher::<std::hash::DefaultHasher, _>(c, "default", || {
        std::hash::DefaultHasher::default()
    });
}

fn benchmark_fx_hasher(c: &mut Criterion) {
    benchmark_hasher::<fxhash::FxHasher64, _>(c, "fxhash", || fxhash::FxHasher64::default());
}

fn benchmark_mur3_hasher(c: &mut Criterion) {
    benchmark_hasher::<mur3::Hasher128, _>(c, "mur3", || mur3::Hasher128::with_seed(0));
}

fn benchmark_xxh3_hasher(c: &mut Criterion) {
    benchmark_hasher::<xxhash_rust::xxh3::Xxh3, _>(c, "xxh3", || {
        xxhash_rust::xxh3::Xxh3::default()
    });
}

fn benchmark_xxh64_hasher(c: &mut Criterion) {
    benchmark_hasher::<Xxh64, _>(c, "xxh64", || Xxh64::default());
}

criterion_group!(
    benches,
    benchmark_default_hasher,
    benchmark_fx_hasher,
    benchmark_mur3_hasher,
    benchmark_xxh3_hasher,
    benchmark_xxh64_hasher
);
criterion_main!(benches);
