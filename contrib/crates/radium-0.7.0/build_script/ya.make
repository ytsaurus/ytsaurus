# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.7.0)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/macros.rs
    ../src/types.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE.txt
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.7.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/radium/radium-0.7.0.crate)

END()
