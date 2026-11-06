# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(pyo3_macros)

CRATE_VERSION(0.26.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/pyo3-macros-backend-0.26.0
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.26.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros/pyo3-macros-0.26.0.crate)

END()
