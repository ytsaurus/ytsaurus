# Native Rust build with dependencies shared through contrib/crates.

CRATE(pyo3_build_config)

CRATE_VERSION(0.25.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_ENV(PYO3_CONFIG_FILE=${ARCADIA_ROOT}/contrib/crates/pyo3-build-config-0.25.1/python-config.txt)

CRATE_SOURCES(
    build.rs
    src/errors.rs
    src/impl_.rs
    src/import_lib.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    python-config.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    extension-module
    resolve-config
)

PEERDIR(
    contrib/crates/once_cell-1.21.4
    contrib/crates/target-lexicon-0.13.5
)

CRATE_BUILD_SCRIPT(contrib/crates/pyo3-build-config-0.25.1/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.25.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.25.1.crate)

END()

RECURSE(build_script)
