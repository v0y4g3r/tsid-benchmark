use criterion::{Criterion, black_box, criterion_group, criterion_main};
use fxhash::FxHasher64;
use tsid_bench::{open_csv_reader, read_labels_and_hash};
use tsid_bench::ts_id_gen::TsIdGenerator;
use xxhash_rust::xxh3::Xxh3;

fn reuse_label_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("reuse");
    // Benchmark complete tsid generation: write_label_names + write_label_values + build_ts_id
    group.bench_function("xx3", |b| {
        
        let labels = read_labels_and_hash::<Xxh3>(open_csv_reader("./assets/"));
        let label_values: &Vec<Vec<String>> = &labels.label_values;
        b.iter(|| {
            for label_value_row in label_values.iter() {
                let mut generator = TsIdGenerator::<Xxh3>::from_seed(labels.label_name_hash);
                generator.write_label_values(black_box(
                    label_value_row.iter().map(|s: &String| s.as_bytes()),
                ));
                let tsid = black_box(generator.build_ts_id());
                black_box(tsid);
            }
        });
    });

    group.bench_function("fxhash", |b| {
        let labels = read_labels_and_hash::<FxHasher64>(open_csv_reader("./assets/"));
        let label_values: &Vec<Vec<String>> = &labels.label_values;

        b.iter(|| {
            for label_value_row in label_values.iter() {
                let mut generator = TsIdGenerator::<FxHasher64>::from_seed(labels.label_name_hash);
                generator.write_label_values(black_box(
                    label_value_row.iter().map(|s: &String| s.as_bytes()),
                ));
                let tsid = black_box(generator.build_ts_id());
                black_box(tsid);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, reuse_label_hash);
criterion_main!(benches);
