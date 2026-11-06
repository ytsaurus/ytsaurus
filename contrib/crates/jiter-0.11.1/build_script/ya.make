# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.11.1)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/errors.rs
    ../src/jiter.rs
    ../src/lib.rs
    ../src/number_decoder.rs
    ../src/parse.rs
    ../src/py_lossless_float.rs
    ../src/py_string_cache.rs
    ../src/python.rs
    ../src/simd_aarch64.rs
    ../src/string_decoder.rs
    ../src/value.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    num-bigint
    python
)

PEERDIR(
    contrib/crates/pyo3-build-config-0.26.0
)

LICENSE("MIT")
VERSION(0.11.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/jiter/jiter-0.11.1.crate)

END()
