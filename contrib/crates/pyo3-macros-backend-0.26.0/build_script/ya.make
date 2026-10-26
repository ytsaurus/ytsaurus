# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.26.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/attributes.rs
    ../src/combine_errors.rs
    ../src/derive_attributes.rs
    ../src/frompyobject.rs
    ../src/intopyobject.rs
    ../src/introspection.rs
    ../src/konst.rs
    ../src/lib.rs
    ../src/method.rs
    ../src/module.rs
    ../src/params.rs
    ../src/pyclass.rs
    ../src/pyfunction.rs
    ../src/pyfunction/signature.rs
    ../src/pyimpl.rs
    ../src/pymethod.rs
    ../src/pyversions.rs
    ../src/quotes.rs
    ../src/utils.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/pyo3-build-config-0.26.0
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.26.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros-backend/pyo3-macros-backend-0.26.0.crate)

END()
