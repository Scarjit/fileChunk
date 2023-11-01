use serde::{Deserialize, Serialize};

const PRIME: u64 = 1_099_511_627_791;  // A valid large prime
const WINDOW_SIZE: usize = 64;  // Arbitrary window size

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RabinFingerprint {
    value: u64,
    base: u64,  // This is used to efficiently remove the oldest byte from the fingerprint
}

impl RabinFingerprint {
    pub(crate) fn new() -> Self {
        RabinFingerprint {
            value: 0,
            base: crate::bigmath::mod_pow(256, WINDOW_SIZE as u64, PRIME),
        }
    }

    // Add a new byte to the fingerprint
    pub(crate) fn push_byte(&mut self, byte: u8) {
        self.value = (self.value * 256 + byte as u64) % PRIME;
    }

    // Remove the oldest byte from the fingerprint
    fn pop_byte(&mut self, byte: u8) {
        self.value = (self.value + PRIME - (self.base * byte as u64 % PRIME)) % PRIME;
    }

    // Update the fingerprint with a new byte, pushing out the oldest byte if necessary
    pub(crate) fn roll_byte(&mut self, old_byte: u8, new_byte: u8) {
        self.pop_byte(old_byte);
        self.push_byte(new_byte);
    }

    pub(crate) fn value(&self) -> u64 {
        self.value
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rabin_fingerprinting() {
        // load ./tests/data/B100MB.bin
        let data = std::fs::read("../tests/data/B100MB.bin").unwrap();

        let mut fingerprint = RabinFingerprint::new();

        for &byte in &data[0..WINDOW_SIZE.min(data.len())] {
            fingerprint.push_byte(byte);
        }
        assert_ne!(fingerprint.value(), 0);  // Ensure the fingerprint is computed

        let initial_fingerprint = fingerprint.value();

        for i in 0..(data.len() - WINDOW_SIZE) {
            fingerprint.roll_byte(data[i], data[i + WINDOW_SIZE]);
            assert_ne!(fingerprint.value(), initial_fingerprint);  // The rolled fingerprint should differ from the initial one
        }
    }
}
