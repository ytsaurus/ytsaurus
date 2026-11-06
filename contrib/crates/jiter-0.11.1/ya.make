# Native Rust build with dependencies shared through contrib/crates.

CRATE(jiter)

CRATE_VERSION(0.11.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/errors.rs
    src/jiter.rs
    src/number_decoder.rs
    src/parse.rs
    src/py_lossless_float.rs
    src/py_string_cache.rs
    src/python.rs
    src/simd_aarch64.rs
    src/string_decoder.rs
    src/value.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    num-bigint
    python
)

PEERDIR(
    contrib/crates/ahash-0.8.12
    contrib/crates/bitvec-1.1.1
    contrib/crates/lexical-parse-float-1.0.6
    contrib/crates/num-bigint-0.4.8
    contrib/crates/num-traits-0.2.19
    contrib/crates/pyo3-0.26.0
    contrib/crates/smallvec-1.16.4
)

CRATE_BUILD_SCRIPT(contrib/crates/jiter-0.11.1/build_script)

LICENSE("MIT")
VERSION(0.11.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/jiter/jiter-0.11.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/indoc-2.0.8
    contrib/crates/pyo3-macros-0.26.0
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()

RECURSE(build_script)
