use std::fs::File;
use std::hash::Hasher;

use csv::{StringRecord, StringRecordsIntoIter};

use crate::ts_id_gen::{SeededHasher, TsIdGenerator};

pub mod data_reader;
pub mod ts_id_gen;

pub struct LabelValuesIterator {
    label_names: Vec<String>,
    label_name_hash: u64,
    iterator: StringRecordsIntoIter<File>,
}

impl LabelValuesIterator {
    pub(crate) fn next(&mut self) -> Option<(&[String], Vec<String>, u64)> {
        let record = self.iterator.next()?;
        let record1: StringRecord = record.unwrap();
        let row = record1.iter().map(|s| s.to_owned()).collect::<Vec<_>>();
        Some((self.label_names.as_slice(), row, self.label_name_hash))
    }
}

pub struct Labels {
    pub label_names: Vec<String>,
    pub label_name_hash: u64,
    pub label_values: Vec<Vec<String>>,
}

pub fn read_labels_and_hash<H>(path: &str) -> Labels
where
    H: Default + Hasher + SeededHasher,
{
    let mut iterator = read_labels::<H>(path);
    let mut values = vec![];
    while let Some((_, labels, _)) = iterator.next() {
        values.push(labels);
    }
    Labels {
        label_names: iterator.label_names,
        label_name_hash: iterator.label_name_hash,
        label_values: values,
    }
}

fn read_labels<H>(path: &str) -> LabelValuesIterator
where
    H: Default + Hasher + SeededHasher,
{
    let mut result = csv::ReaderBuilder::new().from_path(path).unwrap();
    let label_names = result.headers().unwrap();
    let label_names = label_names
        .iter()
        .map(|s| s.to_owned())
        .collect::<Vec<String>>();

    let mut generator = TsIdGenerator::<H>::default();
    generator.write_label_names(label_names.iter().map(|s| s.as_bytes()));
    let label_name_hash = generator.build_ts_id();
    let iter = result.into_records();
    LabelValuesIterator {
        label_names,
        label_name_hash,
        iterator: iter,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hash::DefaultHasher;

    use fxhash::FxHasher64;
    use xxhash_rust::xxh3::Xxh3;
    use xxhash_rust::xxh64::Xxh64;

    use super::*;

    fn test_hasher<H: Hasher + Default + SeededHasher>(amp: usize) {
        let labels = read_labels_and_hash::<H>("./labels.csv");
        let mut all_hash_codes = HashSet::with_capacity(labels.label_values.len());

        for label in labels.label_values.iter() {
            for idx in 0..amp {
                let mut ts_id_gen = TsIdGenerator::<H>::from_seed(labels.label_name_hash);
                let label_values: Vec<_> = label
                    .iter()
                    .map(|s| format!("{}-{}", s, idx).into_bytes())
                    .collect();
                ts_id_gen.write_label_values(label_values.iter().map(|a| a.as_slice()));
                let i = ts_id_gen.build_ts_id();
                assert!(!all_hash_codes.contains(&i));
                all_hash_codes.insert(i);
            }
        }
    }

    // Check hash collision in 100 million label combinations.
    // This is important because we use tsid to distinguish
    // between different time series.
    #[test]
    fn check_collisions() {
        let amp = 100_000_000usize / 660;
        test_hasher::<Xxh3>(amp);
        test_hasher::<Xxh64>(amp);
        test_hasher::<FxHasher64>(amp);
        test_hasher::<DefaultHasher>(amp);
    }
}
