# Native Rust build with dependencies shared through contrib/crates.

CRATE(lexical_util)

CRATE_VERSION(1.0.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/algorithm.rs
    src/api.rs
    src/ascii.rs
    src/assert.rs
    src/bf16.rs
    src/constants.rs
    src/digit.rs
    src/div128.rs
    src/error.rs
    src/extended_float.rs
    src/f16.rs
    src/feature_format.rs
    src/format.rs
    src/format_builder.rs
    src/format_flags.rs
    src/iterator.rs
    src/libm.rs
    src/mul.rs
    src/noskip.rs
    src/not_feature_format.rs
    src/num.rs
    src/options.rs
    src/prebuilt_formats.rs
    src/result.rs
    src/skip.rs
    src/step.rs
)

CRATE_INPUTS(
    CODE_OF_CONDUCT.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    format
    parse-floats
    parse-integers
    std
    write-floats
    write-integers
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/lexical-util/lexical-util-1.0.7.crate)

END()
