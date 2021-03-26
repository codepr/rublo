use bitvec::prelude::*;
use chrono::{DateTime, Utc};
use fasthash::{murmur3::Hash32, FastHash};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::f64;
use std::fmt;
use std::fs;
use std::result::Result;

#[derive(Serialize, Deserialize)]
struct BloomFilter {
    capacity: usize,
    size: usize,
    bitmap: BitVec,
    hash_count: u32,
    hits: u64,
    miss: u64,
}

#[derive(Debug)]
struct BloomFilterError(String);

impl fmt::Display for BloomFilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bloomfilter error: {}", self.0)
    }
}

impl Error for BloomFilterError {}

impl BloomFilter {
    /// Create a new BloomFilter, a probabilistic space-efficient data structure which is
    /// used to test if an element is a member of a set, trading precision for efficiency
    /// and performance, allowing a tiny false-positive probability against a 0 false negative
    /// probability. This means that when tested, an item might be in the set or it absolutely
    /// isn't.
    ///
    /// The capacity is the number of items expected to be stored in the filter, fpp represents
    /// the false positive probability.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero or fpp is zero.
    pub fn new(capacity: usize, fpp: f64) -> BloomFilter {
        assert!(capacity > 0 && fpp > 0.);
        let bitmap_size = Self::get_bitmap_size(capacity, fpp);
        let hash_count = Self::get_optimal_hash_count(bitmap_size, capacity);
        BloomFilter {
            capacity: bitmap_size,
            size: 0,
            bitmap: bitvec![0u8; bitmap_size],
            hash_count: hash_count,
            hits: 0,
            miss: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    pub fn byte_space(&self) -> usize {
        self.capacity() / 8
    }

    pub fn hits(&self) -> u64 {
        self.hits
    }

    pub fn miss(&self) -> u64 {
        self.miss
    }

    pub fn set(&mut self, bytes: &[u8]) -> Result<bool, Box<dyn Error>> {
        let mut allbits = true;
        if self.size() == self.capacity() {
            return Err(Box::new(BloomFilterError("Full capacity reached".into())));
        }
        for i in 0..self.hash_count {
            let hash = (Hash32::hash_with_seed(bytes, i) as usize) % self.capacity;
            if allbits && self.bitmap[hash] {
                allbits = false;
            }
            self.bitmap.set(hash, true);
        }
        if allbits {
            self.size += 1
        }
        Ok(!allbits)
    }

    pub fn check(&mut self, bytes: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let hash = (Hash32::hash_with_seed(bytes, i) as usize) % self.capacity;
            if self.bitmap[hash] == false {
                self.miss += 1;
                return false;
            }
        }
        self.hits += 1;
        return true;
    }

    pub fn clear(&mut self) {
        self.bitmap.clear();
        self.size = 0;
    }

    pub fn to_file(&self, filename: &str) -> Result<(), Box<dyn Error>> {
        let serialized = bincode::serialize(self)?;
        fs::write(filename, &serialized)?;
        Ok(())
    }

    fn get_bitmap_size(items_count: usize, fpp: f64) -> usize {
        let log2 = f64::consts::LN_2;
        let log2_2 = log2 * log2;
        let m = -((items_count as f64) * fpp.ln()) / log2_2;
        return m.ceil() as usize;
    }

    fn get_optimal_hash_count(bitmap_size: usize, items_count: usize) -> u32 {
        let k = (bitmap_size as f64 / items_count as f64) * f64::consts::LN_2;
        return k.ceil() as u32;
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;

    #[test]
    fn test_new() {
        let bf = BloomFilter::new(5, 0.01);
        assert_eq!(bf.capacity(), 48);
        assert_eq!(bf.hash_count(), 7);
        let bf = BloomFilter::new(1500, 0.001);
        assert_eq!(bf.capacity(), 21567);
        assert_eq!(bf.hash_count(), 10);
        let bf = BloomFilter::new(400, 0.05);
        assert_eq!(bf.capacity(), 2495);
        assert_eq!(bf.hash_count(), 5);
        let bf = BloomFilter::new(192, 0.05);
        assert_eq!(bf.byte_space(), 149)
    }

    #[test]
    fn test_check() {
        let mut bf = BloomFilter::new(5, 0.01);
        for word in ["Vega", "Pandora", "Magnetar", "Pulsar", "Nebula"].iter() {
            bf.set(word.as_bytes()).unwrap();
        }
        for want in [
            ("Pandora", true),
            ("Magnetar", true),
            ("Blazar", false),
            ("Vega", true),
            ("Dwarf", false),
            ("Trail", false),
        ]
        .iter()
        {
            assert_eq!(bf.check(want.0.as_bytes()), want.1);
        }
    }
}

const FALSE_POSITIVE_PROBABILITY_RATIO: f64 = 0.9;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScaleFactor {
    #[serde(rename(deserialize = "small"))]
    SmallScaleSize = 2,
    #[serde(rename(deserialize = "large"))]
    LargeScaleSize = 4,
}

impl ScaleFactor {
    pub fn small_scale_size() -> Self {
        ScaleFactor::SmallScaleSize
    }

    pub fn large_scale_size() -> Self {
        ScaleFactor::LargeScaleSize
    }
}

#[derive(Serialize, Deserialize)]
pub struct ScalableBloomFilter {
    name: String,
    initial_capacity: usize,
    filters: Vec<BloomFilter>,
    fpp: f64,
    scale_factor: ScaleFactor,
    creation_time: DateTime<Utc>,
}

impl ScalableBloomFilter {
    pub fn new(name: String, initial_capacity: usize, fpp: f64, scale_factor: ScaleFactor) -> Self {
        Self {
            name,
            initial_capacity,
            filters: Vec::new(),
            fpp: fpp,
            scale_factor: scale_factor,
            creation_time: Utc::now(),
        }
    }

    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub fn capacity(&self) -> usize {
        if self.filters.is_empty() {
            return self.initial_capacity;
        }
        self.filters.iter().fold(0, |acc, x| acc + x.capacity())
    }

    pub fn size(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.size())
    }

    pub fn byte_space(&self) -> usize {
        if self.filters.is_empty() {
            return self.initial_capacity / 8;
        }
        self.filters.iter().fold(0, |acc, x| acc + x.byte_space())
    }

    pub fn hits(&self) -> u64 {
        self.filters.iter().fold(0, |acc, x| acc + x.hits())
    }

    pub fn miss(&self) -> u64 {
        self.filters.iter().fold(0, |acc, x| acc + x.miss())
    }

    pub fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time
    }

    pub fn clear(&mut self) {
        for filter in self.filters.iter_mut() {
            filter.clear();
        }
    }

    pub fn set(&mut self, bytes: &[u8]) -> Result<bool, Box<dyn Error>> {
        if self.check(bytes) {
            return Ok(true);
        }
        if let Some(f) = self.filters.last() {
            if f.size() == f.capacity() {
                self.add_filter(
                    self.initial_capacity * self.scale_factor as usize,
                    self.fpp * FALSE_POSITIVE_PROBABILITY_RATIO,
                );
            }
        } else {
            self.add_filter(
                self.initial_capacity * self.scale_factor as usize,
                self.fpp * FALSE_POSITIVE_PROBABILITY_RATIO,
            );
        }
        let filter = self.filters.last_mut().unwrap();
        filter.set(bytes)
    }

    pub fn check(&mut self, bytes: &[u8]) -> bool {
        for f in self.filters.iter_mut().rev() {
            if f.check(bytes) {
                return true;
            }
        }
        return false;
    }

    pub fn to_file(&self) -> Result<(), Box<dyn Error>> {
        let serialized = bincode::serialize(self)?;
        fs::write(&self.name, &serialized)?;
        Ok(())
    }

    pub fn from_file(&self) -> Result<ScalableBloomFilter, Box<dyn Error>> {
        let data = fs::read(&self.name)?;
        let filter = bincode::deserialize(&data[..])?;
        Ok(filter)
    }

    fn add_filter(&mut self, capacity: usize, fpp: f64) {
        self.filters.push(BloomFilter::new(capacity, fpp))
    }
}

#[cfg(test)]
mod scalable_filter_tests {
    use super::*;

    #[test]
    fn test_set() {
        let mut sbf =
            ScalableBloomFilter::new("test-sbf".into(), 5, 0.01, ScaleFactor::SmallScaleSize);
        for word in ["Vega", "Pandora", "Magnetar", "Pulsar", "Nebula"].iter() {
            sbf.set(word.as_bytes()).unwrap();
        }
        for want in [
            ("Pandora", true),
            ("Magnetar", true),
            ("Blazar", false),
            ("Vega", true),
            ("Dwarf", false),
            ("Trail", false),
        ]
        .iter()
        {
            assert_eq!(sbf.check(want.0.as_bytes()), want.1);
        }
        assert_eq!(sbf.filter_count(), 1);
        for word in ["Collider", "Neutron", "Positron", "Hyperion", "Arcadia"].iter() {
            sbf.set(word.as_bytes()).unwrap();
        }
        assert_eq!(sbf.size(), 2);
    }
}
