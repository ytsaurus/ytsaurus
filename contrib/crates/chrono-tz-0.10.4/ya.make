# Native Rust build with dependencies shared through contrib/crates.

CRATE(chrono_tz)

CRATE_VERSION(0.10.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/binary_search.rs
    src/prebuilt/directory.rs
    src/prebuilt/mod.rs
    src/prebuilt/timezones.rs
    src/serde.rs
    src/timezone_impl.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    tz/LICENSE
    tz/NEWS
    tz/africa
    tz/antarctica
    tz/asia
    tz/australasia
    tz/backward
    tz/etcetera
    tz/europe
    tz/northamerica
    tz/southamerica
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/chrono-0.4.45
    contrib/crates/phf-0.12.1
)

CRATE_BUILD_SCRIPT(contrib/crates/chrono-tz-0.10.4/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/chrono-tz/chrono-tz-0.10.4.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()

RECURSE(build_script)
