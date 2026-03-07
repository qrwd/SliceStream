/// Placeholder signature provider interface.
pub trait Signer {
    fn sign(&self, payload: &[u8]) -> Vec<u8>;
}
