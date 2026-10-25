# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(indoc)

CRATE_VERSION(2.0.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/expr.rs
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
VERSION(2.0.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/indoc/indoc-2.0.8.crate)

END()
