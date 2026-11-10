# Native Rust build with dependencies shared through contrib/crates.

CRATE(phf_shared)

CRATE_VERSION(0.12.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/siphasher-1.0.4
)

LICENSE("MIT")
VERSION(0.12.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/phf_shared/phf_shared-0.12.1.crate)

END()
