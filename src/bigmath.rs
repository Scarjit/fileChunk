
pub(crate) fn multiply_mod(a: u64, b: u64, modulus: u64) -> u64 {
    (((a % modulus) as u128  * (b % modulus) as u128 ) % modulus as u128) as u64
}

pub(crate) fn mod_pow(mut base: u64, mut exp: u64, modulus: u64) -> u64 {
    if modulus == 1 {
        return 0;
    }
    let mut result = 1;
    base %= modulus;
    while exp > 0 {
        if exp % 2 == 1 {
            result = multiply_mod(result, base, modulus);
        }
        exp >>= 1;
        base = multiply_mod(base, base, modulus);
    }
    result
}
