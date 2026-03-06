use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlg {
    Sha256V1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<CanonicalValue>),
    Object(BTreeMap<String, CanonicalValue>),
}

pub fn hash_bytes(alg: HashAlg, bytes: &[u8]) -> [u8; 32] {
    match alg {
        HashAlg::Sha256V1 => sha256(bytes),
    }
}

pub fn hash_hex(alg: HashAlg, bytes: &[u8]) -> String {
    let digest = hash_bytes(alg, bytes);
    to_hex(&digest)
}

/// Placeholder canonical JSON serializer with stable key ordering and no external deps.
pub fn canonical_json_bytes(value: &CanonicalValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical_json(value, &mut out);
    out
}

fn write_canonical_json(v: &CanonicalValue, out: &mut Vec<u8>) {
    match v {
        CanonicalValue::Null => out.extend_from_slice(b"null"),
        CanonicalValue::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        CanonicalValue::Number(n) => out.extend_from_slice(n.as_bytes()),
        CanonicalValue::String(s) => {
            out.push(b'"');
            for ch in s.chars() {
                match ch {
                    '"' => out.extend_from_slice(b"\\\""),
                    '\\' => out.extend_from_slice(b"\\\\"),
                    '\n' => out.extend_from_slice(b"\\n"),
                    '\r' => out.extend_from_slice(b"\\r"),
                    '\t' => out.extend_from_slice(b"\\t"),
                    c if c < ' ' => {
                        let escaped = format!("\\u{:04x}", c as u32);
                        out.extend_from_slice(escaped.as_bytes());
                    }
                    c => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
            out.push(b'"');
        }
        CanonicalValue::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical_json(item, out);
            }
            out.push(b']');
        }
        CanonicalValue::Object(map) => {
            out.push(b'{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical_json(&CanonicalValue::String(k.clone()), out);
                out.push(b':');
                write_canonical_json(v, out);
            }
            out.push(b'}');
        }
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(nibble_to_hex((b >> 4) & 0x0f));
        s.push(nibble_to_hex(b & 0x0f));
    }
    s
}

fn nibble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!(),
    }
}

fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];

    let mut msg = input.to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    let mut w = [0u32; 64];
    for chunk in msg.chunks_exact(64) {
        for (i, bytes) in chunk.chunks_exact(4).take(16).enumerate() {
            w[i] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_v1_matches_known_vector() {
        assert_eq!(
            hash_hex(HashAlg::Sha256V1, b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn canonical_json_is_stable_for_key_order() {
        let mut a = BTreeMap::new();
        a.insert("b".to_string(), CanonicalValue::Number("2".to_string()));
        a.insert("a".to_string(), CanonicalValue::Number("1".to_string()));

        let mut b = BTreeMap::new();
        b.insert("a".to_string(), CanonicalValue::Number("1".to_string()));
        b.insert("b".to_string(), CanonicalValue::Number("2".to_string()));

        assert_eq!(
            canonical_json_bytes(&CanonicalValue::Object(a)),
            canonical_json_bytes(&CanonicalValue::Object(b))
        );
    }
}
