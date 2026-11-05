# Native Rust build with dependencies shared through contrib/crates.

CRATE(lexical_parse_integer)

CRATE_VERSION(1.0.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/algorithm.rs
    src/api.rs
    src/options.rs
    src/parse.rs
)

CRATE_INPUTS(
    CODE_OF_CONDUCT.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE.md
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    format
    std
)

PEERDIR(
    contrib/crates/lexical-util-1.0.7
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/lexical-parse-integer/lexical-parse-integer-1.0.6.crate)

END()
