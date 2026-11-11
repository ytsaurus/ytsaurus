# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.25.1)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_ENV(PYO3_CONFIG_FILE=${ARCADIA_ROOT}/contrib/crates/pyo3-build-config-0.25.1/python-config.txt)

CRATE_SOURCES(
    ../src/errors.rs
    ../src/impl_.rs
    ../src/import_lib.rs
    ../src/lib.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../python-config.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    extension-module
    resolve-config
)

PEERDIR(
    contrib/crates/target-lexicon-0.13.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.25.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.25.1.crate)

END()
