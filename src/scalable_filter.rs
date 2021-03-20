use crate::filter::BloomFilter;
use chrono::{DateTime, Utc};
use std::error::Error;
use std::result::Result;

const FALSE_POSITIVE_PROBABILITY_RATIO: f64 = 0.9;

#[derive(Copy, Clone)]
pub enum ScaleFactor {
    SmallScaleSize = 2,
    LargeScaleSize = 4,
}

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
        self.filters.iter().fold(0, |acc, x| acc + x.capacity())
    }

    pub fn size(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.size())
    }

    pub fn byte_space(&self) -> usize {
        self.filters.iter().fold(0, |acc, x| acc + x.byte_space())
    }

    pub fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time
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

    pub fn check(&self, bytes: &[u8]) -> bool {
        for f in self.filters.iter().rev() {
            if f.check(bytes) {
                return true;
            }
        }
        return false;
    }

    fn add_filter(&mut self, capacity: usize, fpp: f64) {
        self.filters.push(BloomFilter::new(capacity, fpp))
    }
}

#[cfg(test)]
mod tests {
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
