# Native Rust build with dependencies shared through contrib/crates.

CRATE(radium)

CRATE_VERSION(0.7.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/macros.rs
    src/types.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/radium-0.7.0/build_script)

LICENSE("MIT")
VERSION(0.7.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/radium/radium-0.7.0.crate)

END()

RECURSE(build_script)
