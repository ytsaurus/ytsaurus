# Native Rust build with dependencies shared through contrib/crates.

CRATE(tap)

CRATE_VERSION(1.0.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/conv.rs
    src/pipe.rs
    src/tap.rs
)

CRATE_INPUTS(
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(1.0.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/tap/tap-1.0.1.crate)

END()
