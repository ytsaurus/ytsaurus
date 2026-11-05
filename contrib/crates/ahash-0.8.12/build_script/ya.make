# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.8.12)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/aes_hash.rs
    ../src/convert.rs
    ../src/fallback_hash.rs
    ../src/hash_map.rs
    ../src/hash_quality_test.rs
    ../src/hash_set.rs
    ../src/lib.rs
    ../src/operations.rs
    ../src/random_state.rs
    ../src/specialize.rs
)

CRATE_INPUTS(
    ../FAQ.md
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    getrandom
    runtime-rng
    std
)

PEERDIR(
    contrib/crates/version_check-0.9.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.12)
ORIGINAL_SOURCE(https://static.crates.io/crates/ahash/ahash-0.8.12.crate)

END()
