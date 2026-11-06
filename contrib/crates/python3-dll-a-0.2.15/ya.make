# Native Rust build with dependencies shared through contrib/crates.

CRATE(python3_dll_a)

CRATE_VERSION(0.2.15)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    parse-stable-abi.py
    src/libpypy3-c.def
    src/libpypy3.10-c.def
    src/libpypy3.11-c.def
    src/libpypy3.9-c.def
    src/python3.def
    src/python310.def
    src/python311.def
    src/python312.def
    src/python313.def
    src/python313t.def
    src/python314.def
    src/python314t.def
    src/python37.def
    src/python38.def
    src/python39.def
)

CRATE_FLAGS(--cap-lints=allow)

IF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/cc-1.10.0
)

ENDIF()

LICENSE("MIT")
VERSION(0.2.15)
ORIGINAL_SOURCE(https://static.crates.io/crates/python3-dll-a/python3-dll-a-0.2.15.crate)

END()
