# Native Rust build with dependencies shared through contrib/crates.

CRATE(ahash)

CRATE_VERSION(0.8.12)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/aes_hash.rs
    src/convert.rs
    src/fallback_hash.rs
    src/hash_map.rs
    src/hash_quality_test.rs
    src/hash_set.rs
    src/operations.rs
    src/random_state.rs
    src/specialize.rs
)

CRATE_INPUTS(
    FAQ.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    getrandom
    runtime-rng
    std
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/getrandom-0.3.4
    contrib/crates/once_cell-1.21.4
    contrib/crates/zerocopy-0.8.63
)

CRATE_BUILD_SCRIPT(contrib/crates/ahash-0.8.12/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.12)
ORIGINAL_SOURCE(https://static.crates.io/crates/ahash/ahash-0.8.12.crate)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()

RECURSE(build_script)
