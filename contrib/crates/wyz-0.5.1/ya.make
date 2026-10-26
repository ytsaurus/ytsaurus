# Native Rust build with dependencies shared through contrib/crates.

CRATE(wyz)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bidi.rs
    src/comu.rs
    src/exit.rs
    src/fmt.rs
    src/range.rs
    src/wm.rs
)

CRATE_INPUTS(
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/tap-1.0.1
)

LICENSE("MIT")
VERSION(0.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/wyz/wyz-0.5.1.crate)

END()
