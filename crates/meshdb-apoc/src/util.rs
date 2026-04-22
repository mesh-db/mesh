//! `apoc.util.*` — miscellaneous utilities.
//!
//! Currently the shipping surface is the cryptographic digest
//! family (`md5`, `sha1`, `sha256`, `sha384`, `sha512`). Each
//! function takes a single string argument and returns a
//! lower-case hex digest. `null` in → `null` out, matching the
//! null-propagation convention used across the rest of the APOC
//! surface.
//!
//! The RustCrypto digest crates (`md-5`, `sha1`, `sha2`) all share
//! the [`digest::Digest`] trait, so every hash function here
//! funnels through a single generic [`digest_hex`] helper and just
//! picks the concrete digest type.

use crate::ApocError;
use meshdb_core::Property;
// Re-exported by every RustCrypto hash crate that depends on
// `digest`; pulling it off `sha2` saves adding `digest` as a
// direct dependency.
use sha2::Digest;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "md5" => hash::<md5::Md5>(args, "md5"),
        "sha1" => hash::<sha1::Sha1>(args, "sha1"),
        "sha256" => hash::<sha2::Sha256>(args, "sha256"),
        "sha384" => hash::<sha2::Sha384>(args, "sha384"),
        "sha512" => hash::<sha2::Sha512>(args, "sha512"),
        _ => Err(ApocError::UnknownFunction(format!("apoc.util.{suffix}"))),
    }
}

fn hash<D: Digest>(args: &[Property], name: &str) -> Result<Property, ApocError> {
    if args.len() != 1 {
        return Err(ApocError::Arity {
            name: format!("apoc.util.{name}"),
            expected: 1,
            got: args.len(),
        });
    }
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => Ok(Property::String(digest_hex::<D>(s.as_bytes()))),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.util.{name}"),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn digest_hex<D: Digest>(bytes: &[u8]) -> String {
    let out = D::digest(bytes);
    let mut hex = String::with_capacity(out.len() * 2);
    for byte in out.iter() {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.to_string())
    }

    #[test]
    fn md5_matches_known_vector() {
        // RFC 1321 A.5: MD5("abc") = 900150983cd24fb0d6963f7d28e17f72
        let out = call("md5", &[s("abc")]).unwrap();
        assert_eq!(out, s("900150983cd24fb0d6963f7d28e17f72"));
    }

    #[test]
    fn md5_empty_string() {
        let out = call("md5", &[s("")]).unwrap();
        assert_eq!(out, s("d41d8cd98f00b204e9800998ecf8427e"));
    }

    #[test]
    fn sha1_matches_known_vector() {
        // FIPS 180-1 A.1: SHA1("abc") = a9993e36 4706816a ba3e2571 7850c26c 9cd0d89d
        let out = call("sha1", &[s("abc")]).unwrap();
        assert_eq!(out, s("a9993e364706816aba3e25717850c26c9cd0d89d"));
    }

    #[test]
    fn sha256_matches_known_vector() {
        // FIPS 180-2 B.1: SHA256("abc")
        let out = call("sha256", &[s("abc")]).unwrap();
        assert_eq!(
            out,
            s("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        );
    }

    #[test]
    fn sha384_matches_known_vector() {
        // FIPS 180-2 D.1: SHA384("abc")
        let out = call("sha384", &[s("abc")]).unwrap();
        assert_eq!(
            out,
            s("cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"),
        );
    }

    #[test]
    fn sha512_matches_known_vector() {
        // FIPS 180-2 C.1: SHA512("abc")
        let out = call("sha512", &[s("abc")]).unwrap();
        assert_eq!(
            out,
            s("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"),
        );
    }

    #[test]
    fn null_propagates() {
        for fname in ["md5", "sha1", "sha256", "sha384", "sha512"] {
            let out = call(fname, &[Property::Null]).unwrap();
            assert_eq!(out, Property::Null, "{fname} should null-propagate");
        }
    }

    #[test]
    fn non_string_errors() {
        let err = call("sha256", &[Property::Int64(42)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = call("md5", &[]).unwrap_err();
        match err {
            ApocError::Arity {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "apoc.util.md5");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected Arity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("blake3", &[s("x")]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.util.blake3"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
