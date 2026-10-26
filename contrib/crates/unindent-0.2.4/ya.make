# Native Rust build with dependencies shared through contrib/crates.

CRATE(unindent)

CRATE_VERSION(0.2.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/unindent.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/unindent/unindent-0.2.4.crate)

END()
