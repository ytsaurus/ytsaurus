# Native Rust build with dependencies shared through contrib/crates.

CRATE(speedate)

CRATE_VERSION(0.17.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/config.rs
    src/date.rs
    src/datetime.rs
    src/duration.rs
    src/numbers.rs
    src/time.rs
    src/util.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    Makefile
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/lexical-parse-float-1.0.6
    contrib/crates/strum-0.27.2
)

LICENSE("MIT")
VERSION(0.17.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/speedate/speedate-0.17.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/strum_macros-0.27.2
)

END()
