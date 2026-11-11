# Native Rust build with dependencies shared through contrib/crates.

CRATE(funty)

CRATE_VERSION(2.0.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    LICENSE.txt
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(2.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/funty/funty-2.0.0.crate)

END()
