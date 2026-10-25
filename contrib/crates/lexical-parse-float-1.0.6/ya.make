# Native Rust build with dependencies shared through contrib/crates.

CRATE(lexical_parse_float)

CRATE_VERSION(1.0.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/api.rs
    src/bellerophon.rs
    src/bigint.rs
    src/binary.rs
    src/float.rs
    src/fpu.rs
    src/lemire.rs
    src/libm.rs
    src/limits.rs
    src/mask.rs
    src/number.rs
    src/options.rs
    src/parse.rs
    src/shared.rs
    src/slow.rs
    src/table.rs
    src/table_bellerophon_decimal.rs
    src/table_bellerophon_radix.rs
    src/table_binary.rs
    src/table_decimal.rs
    src/table_large.rs
    src/table_lemire.rs
    src/table_radix.rs
    src/table_small.rs
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
    default
    format
    std
)

PEERDIR(
    contrib/crates/lexical-parse-integer-1.0.6
    contrib/crates/lexical-util-1.0.7
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/lexical-parse-float/lexical-parse-float-1.0.6.crate)

END()
