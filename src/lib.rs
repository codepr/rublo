use bitvec::prelude::*;
use fasthash::{murmur3::Hash32, FastHash};
use std::f64;

pub struct BloomFilter {
    capacity: usize,
    bitmap: BitVec,
    hash_count: u32,
}

impl BloomFilter {
    pub fn new(capacity: usize, fpp: f64) -> BloomFilter {
        assert!(capacity > 0 && fpp > 0.);
        let bitmap_size = Self::get_bitmap_size(capacity, fpp);
        let hash_count = Self::get_optimal_hash_count(bitmap_size, capacity);
        BloomFilter {
            capacity: bitmap_size,
            bitmap: bitvec![0; bitmap_size],
            hash_count: hash_count,
        }
    }

    pub fn size(&self) -> usize {
        self.capacity
    }

    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    pub fn set(&mut self, bytes: &[u8]) {
        for i in 0..self.hash_count {
            let hash = (Hash32::hash_with_seed(bytes, i) as usize) % self.capacity;
            self.bitmap.set(hash, true);
        }
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
        self.bitmap.clear()
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
        assert_eq!(bf.size(), 48);
        assert_eq!(bf.hash_count(), 7);
        let bf = BloomFilter::new(1500, 0.001);
        assert_eq!(bf.size(), 21567);
        assert_eq!(bf.hash_count(), 10);
        let bf = BloomFilter::new(400, 0.05);
        assert_eq!(bf.size(), 2495);
        assert_eq!(bf.hash_count(), 5);
    }

    #[test]
    fn test_check() {
        let mut bf = BloomFilter::new(5, 0.01);
        for word in ["Vega", "Pandora", "Magnetar", "Pulsar", "Nebula"].iter() {
            bf.set(word.as_bytes())
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
