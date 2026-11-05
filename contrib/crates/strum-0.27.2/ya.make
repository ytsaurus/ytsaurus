# Native Rust build with dependencies shared through contrib/crates.

CRATE(strum)

CRATE_VERSION(0.27.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/additional_attributes.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    derive
    std
    strum_macros
)

LICENSE("MIT")
VERSION(0.27.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/strum/strum-0.27.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/strum_macros-0.27.2
)

END()
