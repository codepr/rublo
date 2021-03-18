use bitvec::prelude::*;
use fasthash::{murmur3::Hash32, FastHash};
use std::error::Error;
use std::f64;
use std::fmt;
use std::result::Result;

pub struct BloomFilter {
    capacity: usize,
    size: usize,
    bitmap: BitVec,
    hash_count: u32,
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
            bitmap: bitvec![0; bitmap_size],
            hash_count: hash_count,
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

    pub fn check(&self, bytes: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let hash = (Hash32::hash_with_seed(bytes, i) as usize) % self.capacity;
            if self.bitmap[hash] == false {
                return false;
            }
        }
        return true;
    }

    pub fn clear(&mut self) {
        self.bitmap.clear();
        self.size = 0;
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
mod tests {
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
