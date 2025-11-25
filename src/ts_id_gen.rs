use std::hash::{DefaultHasher, Hasher};

use fxhash::FxHasher64;
use mur3::Hasher128;
use xxhash_rust::xxh3::{Xxh3, Xxh3Builder};
use xxhash_rust::xxh64::Xxh64;

pub struct TsIdGenerator<H> {
    hasher: H,
}

impl<H> Default for TsIdGenerator<H>
where
    H: Default + Hasher,
{
    fn default() -> Self {
        Self {
            hasher: Default::default(),
        }
    }
}

impl<H> TsIdGenerator<H>
where
    H: Hasher + SeededHasher,
{
    pub fn new(hasher: H) -> Self {
        Self { hasher }
    }

    pub fn from_seed(seed: u64) -> Self {
        let h = H::from_seed(seed);
        Self { hasher: h }
    }

    pub fn write_label_names<'a>(&mut self, label_names: impl Iterator<Item = &'a [u8]>) {
        for label in label_names {
            self.hasher.write(label);
            self.hasher.write_u8(0xff);
        }
    }

    pub fn write_label_values<'a>(&mut self, label_values: impl Iterator<Item = &'a [u8]>) {
        for value in label_values {
            self.hasher.write(value);
            self.hasher.write_u8(0xff);
        }
    }

    pub fn build_ts_id(self) -> u64 {
        self.hasher.finish()
    }
}

pub type DefaultTsIdGenerator = TsIdGenerator<DefaultHasher>;
pub type FxTsIdGenerator = TsIdGenerator<FxHasher64>;
pub type Mur3TsIdGenerator = TsIdGenerator<Hasher128>;
pub type Xx3TsIdGenerator = TsIdGenerator<Xxh3>;
pub type Xx64TsIdGenerator = TsIdGenerator<Xxh64>;

impl Xx3TsIdGenerator {
    pub fn write_label_names_and_finish<'a>(
        &mut self,
        label_names: impl Iterator<Item = &'a [u8]>,
    ) -> u64 {
        for label in label_names {
            self.hasher.write(label);
            self.hasher.write_u8(0xff);
        }
        self.hasher.finish()
    }
}

pub trait SeededHasher {
    fn from_seed(seed: u64) -> Self;
}

impl SeededHasher for Xxh3 {
    fn from_seed(seed: u64) -> Self {
        Xxh3Builder::new().with_seed(seed).build()
    }
}

impl SeededHasher for Xxh64 {
    fn from_seed(seed: u64) -> Self {
        Xxh64::new(seed)
    }
}

impl SeededHasher for FxHasher64 {
    fn from_seed(seed: u64) -> Self {
        let mut hasher = FxHasher64::default();
        hasher.write_u64(seed);
        hasher
    }
}

impl SeededHasher for DefaultHasher {
    fn from_seed(seed: u64) -> Self {
        let mut hasher = DefaultHasher::default();
        hasher.write_u64(seed);
        hasher
    }
}

impl SeededHasher for Hasher128 {
    fn from_seed(seed: u64) -> Self {
        Hasher128::with_seed(seed as u32)
    }
}
