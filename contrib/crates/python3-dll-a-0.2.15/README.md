Standalone `python3(y)(t).dll` import library generator
=======================================================

[![Actions Status](https://github.com/PyO3/python3-dll-a/workflows/Test/badge.svg)](https://github.com/PyO3/python3-dll-a/actions)
[![Crate](https://img.shields.io/crates/v/python3-dll-a.svg)](https://crates.io/crates/python3-dll-a)
[![Documentation](https://docs.rs/python3-dll-a/badge.svg)](https://docs.rs/python3-dll-a)

Generates import libraries for the Python DLL
(either `python3.dll` or `python3y(t).dll`)
for MinGW-w64 and MSVC (cross-)compile targets.

This crate **does not require** Python 3 distribution files
to be present on the (cross-)compile host system.

This crate uses the binutils `dlltool` program to generate
the Python DLL import libraries for MinGW-w64 targets.
Setting `PYO3_MINGW_DLLTOOL` environment variable overrides
the default `dlltool` command name for the target.

**Note:** MSVC cross-compile targets require either LLVM binutils
or Zig to be available on the host system.
More specifically, `python3-dll-a` requires `llvm-dlltool` executable
to be present in `PATH` when targeting `*-pc-windows-msvc` from Linux.

Alternatively, `ZIG_COMMAND` environment variable may be set to e.g. `"zig"`
or `"python -m ziglang"`, then `zig dlltool` will be used in place
of `llvm-dlltool` (or MinGW binutils).

PyO3 integration
----------------

Since version **0.16.5**, the `pyo3` crate implements support
for both the Stable ABI and version-specific Python DLL import
library generation via its new `generate-import-lib` feature.

In this configuration, `python3-dll-a` becomes a `pyo3` crate dependency
and is automatically invoked by its build script in both native
and cross compilation scenarios.

### Example `Cargo.toml` usage for an `abi3` PyO3 extension module

```toml
[dependencies]
pyo3 = { version = "0.16.5", features = ["extension-module", "abi3-py37", "generate-import-lib"] }
```

### Example `Cargo.toml` usage for a standard PyO3 extension module

```toml
[dependencies]
pyo3 = { version = "0.16.5", features = ["extension-module", "generate-import-lib"] }
```

Standalone build script usage
-----------------------------

If an older `pyo3` crate version is used, or a different Python bindings
library is required, `python3-dll-a` can be used directly
from the crate build script.

The examples below assume using an older version of PyO3.

### Example `build.rs` script for an `abi3` PyO3 extension

The following cargo build script can be used to cross-compile Stable ABI
PyO3 extension modules for Windows (64/32-bit x86 or 64-bit ARM)
using either MinGW-w64 or MSVC target environment ABI:

```rust
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        let cross_lib_dir = std::env::var_os("PYO3_CROSS_LIB_DIR")
            .expect("PYO3_CROSS_LIB_DIR is not set when cross-compiling");
        let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
        let env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap();

        let libdir = std::path::Path::new(&cross_lib_dir);
        python3_dll_a::generate_implib_for_target(libdir, &arch, &env)
            .expect("python3.dll import library generator failed");
    }
}
```

A compatible `python3.dll` import library file named `python3.dll.a`
or `python3.lib` will be automatically created in the directory
pointed by the `PYO3_CROSS_LIB_DIR` environment variable.

### Example `cargo build` invocation

```sh
PYO3_CROSS_LIB_DIR=target/python3-dll cargo build --target x86_64-pc-windows-gnu
```

Generating version-specific `python3y.dll` import libraries
-----------------------------------------------------------

As an advanced feature, `python3-dll-a` can generate Python version
specific import libraries such as `python39.lib` or `python313t.lib`.

See the `ImportLibraryGenerator` builder API description for details.

Maintenance
-----------

This crate embeds Module-Definitions based on the `stable_abi.toml` file from CPython.

The upstream version of this file is located in the [CPython project][cpython]
repository under the path `Misc/stable_abi.toml`.

[cpython]: https://github.com/python/cpython/blob/main/Misc/stable_abi.toml
