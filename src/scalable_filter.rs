use crate::filter::BloomFilter;

pub enum ScaleFactor {
    SmallScaleSize,
    LargeScaleSize,
}

pub struct ScalableBloomFilter {
    initial_capacity: usize,
    filters: Vec<BloomFilter>,
    scale_factor: ScaleFactor,
}

impl ScalableBloomFilter {
    pub fn new(initial_capacity: usize, fpp: f64, scale_factor: ScaleFactor) -> Self {
        Self {
            initial_capacity,
            filters: Vec::new(),
            scale_factor: scale_factor,
        }
    }

    pub fn capacity(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.capacity())
    }

    pub fn size(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.size())
    }

    pub fn check(&self, bytes: &[u8]) -> bool {
        for f in self.filters.iter().rev() {
            if f.check(bytes) {
                return true;
            }
        }
        return false;
    }
}
