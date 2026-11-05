# Native Rust build with dependencies shared through contrib/crates.

CRATE(pyo3_build_config)

CRATE_VERSION(0.26.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_ENV(PYO3_CONFIG_FILE=${ARCADIA_ROOT}/contrib/crates/pyo3-build-config-0.26.0/python-config.txt)

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
    python3-dll-a
    resolve-config
)

PEERDIR(
    contrib/crates/python3-dll-a-0.2.15
    contrib/crates/target-lexicon-0.13.5
)

CRATE_BUILD_SCRIPT(contrib/crates/pyo3-build-config-0.26.0/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.26.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.26.0.crate)

END()

RECURSE(build_script)
