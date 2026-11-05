# Native Rust build with dependencies shared through contrib/crates.

CRATE(memoffset)

CRATE_VERSION(0.9.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/offset_of.rs
    src/raw_field.rs
    src/span_of.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

CRATE_BUILD_SCRIPT(contrib/crates/memoffset-0.9.1/build_script)

LICENSE("MIT")
VERSION(0.9.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/memoffset/memoffset-0.9.1.crate)

END()

RECURSE(build_script)
