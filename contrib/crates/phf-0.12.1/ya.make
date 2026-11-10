# Native Rust build with dependencies shared through contrib/crates.

CRATE(phf)

CRATE_VERSION(0.12.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/map.rs
    src/ordered_map.rs
    src/ordered_set.rs
    src/set.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/phf_shared-0.12.1
)

LICENSE("MIT")
VERSION(0.12.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/phf/phf-0.12.1.crate)

END()
