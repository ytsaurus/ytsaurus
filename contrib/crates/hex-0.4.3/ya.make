# Native Rust build with dependencies shared through contrib/crates.

CRATE(hex)

CRATE_VERSION(0.4.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/serde.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/hex/hex-0.4.3.crate)

END()
