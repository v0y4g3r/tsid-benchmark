use std::hash::{Hash, Hasher};

pub struct TsIdGenerator<H> {
    hasher: H,
}

impl<H> TsIdGenerator<H>
where
    H: Hasher,
{
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
