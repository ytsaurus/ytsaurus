# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.9.1)
CRATE_EDITION(2015)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/offset_of.rs
    ../src/raw_field.rs
    ../src/span_of.rs
)

CRATE_INPUTS(
    ../CHANGELOG.md
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

PEERDIR(
    contrib/crates/autocfg-1.5.1
)

LICENSE("MIT")
VERSION(0.9.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/memoffset/memoffset-0.9.1.crate)

END()
