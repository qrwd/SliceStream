/// Placeholder hashing interface.
pub trait Hasher {
    fn digest_hex(&self, input: &[u8]) -> String;
}
