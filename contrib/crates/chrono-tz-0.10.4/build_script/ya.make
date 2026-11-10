# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.10.4)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/binary_search.rs
    ../src/lib.rs
    ../src/prebuilt/directory.rs
    ../src/prebuilt/mod.rs
    ../src/prebuilt/timezones.rs
    ../src/serde.rs
    ../src/timezone_impl.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE
    ../README.md
    ../tz/LICENSE
    ../tz/NEWS
    ../tz/africa
    ../tz/antarctica
    ../tz/asia
    ../tz/australasia
    ../tz/backward
    ../tz/etcetera
    ../tz/europe
    ../tz/northamerica
    ../tz/southamerica
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/chrono-tz/chrono-tz-0.10.4.crate)

END()
