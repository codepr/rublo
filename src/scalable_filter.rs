use crate::filter::BloomFilter;
use std::error::Error;
use std::result::Result;

const RATIO: f64 = 0.9;

#[derive(Copy, Clone)]
pub enum ScaleFactor {
    SmallScaleSize = 2,
    LargeScaleSize = 4,
}

pub struct ScalableBloomFilter {
    initial_capacity: usize,
    filters: Vec<BloomFilter>,
    fpp: f64,
    scale_factor: ScaleFactor,
}

impl ScalableBloomFilter {
    pub fn new(initial_capacity: usize, fpp: f64, scale_factor: ScaleFactor) -> Self {
        Self {
            initial_capacity,
            filters: Vec::new(),
            fpp: fpp,
            scale_factor: scale_factor,
        }
    }

    pub fn capacity(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.capacity())
    }

    pub fn size(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.size())
    }

    pub fn set(&mut self, bytes: &[u8]) -> Result<bool, Box<dyn Error>> {
        if self.check(bytes) {
            return Ok(true);
        }
        if let Some(f) = self.filters.last() {
            if f.size() == f.capacity() {
                self.add_filter(
                    self.initial_capacity * self.scale_factor as usize,
                    self.fpp * RATIO,
                );
            }
        } else {
            self.add_filter(
                self.initial_capacity * self.scale_factor as usize,
                self.fpp * RATIO,
            );
        }
        let filter = self.filters.last().unwrap();
        filter.set(bytes)
    }

    pub fn check(&self, bytes: &[u8]) -> bool {
        for f in self.filters.iter().rev() {
            if f.check(bytes) {
                return true;
            }
        }
        return false;
    }

    fn add_filter(&mut self, capcity: usize, fpp: f64) {
        self.filters.push(BloomFilter::new(capacity, fpp))
    }
}
