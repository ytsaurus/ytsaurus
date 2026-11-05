//! Standalone `python3(y).dll` import library generator
//! ====================================================
//!
//! Generates import libraries for the Python DLL
//! (either `python3.dll` or `python3y.dll`)
//! for MinGW-w64 and MSVC (cross-)compile targets.
//!
//! This crate **does not require** Python 3 distribution files
//! to be present on the (cross-)compile host system.
//!
//! This crate uses the binutils `dlltool` program to generate
//! the Python DLL import libraries for MinGW-w64 targets.
//! Setting `PYO3_MINGW_DLLTOOL` environment variable overrides
//! the default `dlltool` command name for the target.
//!
//! **Note:** MSVC cross-compile targets require either LLVM binutils
//! or Zig to be available on the host system.
//! More specifically, `python3-dll-a` requires `llvm-dlltool` executable
//! to be present in `PATH` when targeting `*-pc-windows-msvc` from Linux.
//!
//! Alternatively, `ZIG_COMMAND` environment variable may be set to e.g. `"zig"`
//! or `"python -m ziglang"`, then `zig dlltool` will be used in place
//! of `llvm-dlltool` (or MinGW binutils).
//!
//! PyO3 integration
//! ----------------
//!
//! Since version **0.16.5**, the `pyo3` crate implements support
//! for both the Stable ABI and version-specific Python DLL import
//! library generation via its new `generate-import-lib` feature.
//!
//! In this configuration, `python3-dll-a` becomes a `pyo3` crate dependency
//! and is automatically invoked by its build script in both native
//! and cross compilation scenarios.
//!
//! ### Example `Cargo.toml` usage for an `abi3` PyO3 extension module
//!
//! ```toml
//! [dependencies]
//! pyo3 = { version = "0.16.5", features = ["extension-module", "abi3-py37", "generate-import-lib"] }
//! ```
//!
//! ### Example `Cargo.toml` usage for a standard PyO3 extension module
//!
//! ```toml
//! [dependencies]
//! pyo3 = { version = "0.16.5", features = ["extension-module", "generate-import-lib"] }
//! ```
//!
//! Standalone build script usage
//! -----------------------------
//!
//! If an older `pyo3` crate version is used, or a different Python bindings
//! library is required, `python3-dll-a` can be used directly
//! from the crate build script.
//!
//! The examples below assume using an older version of PyO3.
//!
//! ### Example `build.rs` script for an `abi3` PyO3 extension
//!
//! The following cargo build script can be used to cross-compile Stable ABI
//! PyO3 extension modules for Windows (64/32-bit x86 or 64-bit ARM)
//! using either MinGW-w64 or MSVC target environment ABI:
//!
//! ```no_run
//! fn main() {
//!     if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
//!         let cross_lib_dir = std::env::var_os("PYO3_CROSS_LIB_DIR")
//!             .expect("PYO3_CROSS_LIB_DIR is not set when cross-compiling");
//!         let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
//!         let env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap();
//!
//!         let libdir = std::path::Path::new(&cross_lib_dir);
//!         python3_dll_a::generate_implib_for_target(libdir, &arch, &env)
//!             .expect("python3.dll import library generator failed");
//!     }
//! }
//! ```
//!
//! A compatible `python3.dll` import library file named `python3.dll.a`
//! or `python3.lib` will be automatically created in the directory
//! pointed by the `PYO3_CROSS_LIB_DIR` environment variable.
//!
//! ### Example `cargo build` invocation
//!
//! ```sh
//! PYO3_CROSS_LIB_DIR=target/python3-dll cargo build --target x86_64-pc-windows-gnu
//! ```
//!
//! Generating version-specific `python3y.dll` import libraries
//! -----------------------------------------------------------
//!
//! As an advanced feature, `python3-dll-a` can generate Python version
//! specific import libraries such as `python39.lib` or `python313t.lib`.
//!
//! See the [`ImportLibraryGenerator`] builder API description for details.

#![deny(missing_docs)]
#![allow(clippy::needless_doctest_main)]
#![allow(clippy::uninlined_format_args)]

use std::env;
use std::fs::{create_dir_all, write};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Import library file extension for the GNU environment ABI (MinGW-w64)
const IMPLIB_EXT_GNU: &str = ".dll.a";

/// Import library file extension for the MSVC environment ABI
const IMPLIB_EXT_MSVC: &str = ".lib";

/// Canonical MinGW-w64 `dlltool` program name
const DLLTOOL_GNU: &str = "x86_64-w64-mingw32-dlltool";

/// Canonical MinGW-w64 `dlltool` program name (32-bit version)
const DLLTOOL_GNU_32: &str = "i686-w64-mingw32-dlltool";

/// Canonical `dlltool` program name for the MSVC environment ABI (LLVM dlltool)
const DLLTOOL_MSVC: &str = "llvm-dlltool";

/// Canonical `lib` program name for the MSVC environment ABI (MSVC lib.exe)
#[cfg(windows)]
const LIB_MSVC: &str = "lib.exe";

/// Python interpreter implementations
#[derive(Debug, Clone, Copy)]
pub enum PythonImplementation {
    /// CPython
    CPython,
    /// PyPy
    PyPy,
}

/// Windows import library generator for Python
///
/// Generates `python3.dll` or `pythonXY.dll` import library directly from the
/// embedded Python ABI definitions data for the specified compile target.
///
/// ABI-tagged versioned Python DLLs such as `python313t.dll` are also supported
/// via an optional ABI flags string parameter.
///
/// Example usage
/// -------------
///
/// ```no_run
/// # use std::path::Path;
/// # use python3_dll_a::ImportLibraryGenerator;
/// // Generate `python3.dll.a` in "target/python3-dll-a"
/// ImportLibraryGenerator::new("x86_64", "gnu")
///     .generate(Path::new("target/python3-dll-a"))
///     .unwrap();
///
/// // Generate `python3.lib` in "target/python3-lib"
/// ImportLibraryGenerator::new("x86_64", "msvc")
///     .generate(Path::new("target/python3-lib"))
///     .unwrap();
///
/// // Generate `python39.dll.a` in "target/python3-dll-a"
/// ImportLibraryGenerator::new("x86_64", "gnu")
///     .version(Some((3, 9)))
///     .generate(Path::new("target/python3-dll-a"))
///     .unwrap();
///
/// // Generate `python38.lib` in "target/python3-lib"
/// ImportLibraryGenerator::new("x86_64", "msvc")
///     .version(Some((3, 8)))
///     .generate(Path::new("target/python3-lib"))
///     .unwrap();
///
/// // Generate `python313t.lib` in "target/python3-lib"
/// ImportLibraryGenerator::new("x86_64", "msvc")
///     .version(Some((3, 13)))
///     .abiflags(Some("t"))
///     .generate(Path::new("target/python3-lib"))
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct ImportLibraryGenerator {
    /// The compile target architecture name (as in `CARGO_CFG_TARGET_ARCH`)
    arch: String,
    // The compile target environment ABI name (as in `CARGO_CFG_TARGET_ENV`)
    env: String,
    /// Major and minor Python version (for `pythonXY.dll` only)
    version: Option<(u8, u8)>,
    /// Python interpreter implementation
    implementation: PythonImplementation,
    /// Optional Python ABI flags
    ///
    /// For example, `"t"` stands for the free-threaded CPython v3.13 build
    /// aka CPython `3.13t`.
    abiflags: Option<String>,
}

impl ImportLibraryGenerator {
    /// Creates a new import library generator for the specified compile target.
    ///
    /// The compile target architecture name (as in `CARGO_CFG_TARGET_ARCH`)
    /// is passed in `arch`.
    ///
    /// The compile target environment ABI name (as in `CARGO_CFG_TARGET_ENV`)
    /// is passed in `env`.
    #[must_use]
    pub fn new(arch: &str, env: &str) -> Self {
        ImportLibraryGenerator {
            arch: arch.to_string(),
            env: env.to_string(),
            version: None,
            implementation: PythonImplementation::CPython,
            abiflags: None,
        }
    }

    /// Sets major and minor version for the `pythonXY.dll` import library.
    ///
    /// The version-agnostic `python3.dll` is generated by default.
    pub fn version(&mut self, version: Option<(u8, u8)>) -> &mut Self {
        self.version = version;
        self
    }

    /// Sets the ABI flags for the `pythonXY<abi>.dll` import library.
    ///
    /// For example, `"t"` stands for the free-threaded CPython v3.13 build
    /// aka CPython `3.13t`.
    /// In this case, `python313t.dll` import library will be generated.
    ///
    /// The untagged versioned `pythonXY.dll` import library
    /// is generated by default.
    pub fn abiflags(&mut self, flags: Option<&str>) -> &mut Self {
        self.abiflags = flags.map(ToOwned::to_owned);
        self
    }

    /// Sets Python interpreter implementation
    pub fn implementation(&mut self, implementation: PythonImplementation) -> &mut Self {
        self.implementation = implementation;
        self
    }

    /// Generates the Python DLL import library in `out_dir`.
    ///
    /// The version-agnostic `python3.dll` import library is generated
    /// by default unless the version-specific `pythonXY.dll` import
    /// was requested via `version()`.
    pub fn generate(&self, out_dir: &Path) -> Result<()> {
        create_dir_all(out_dir)?;

        let defpath = self.write_def_file(out_dir)?;

        // Try to guess the `dlltool` executable name from the target triple.
        let dlltool_command = DllToolCommand::find_for_target(&self.arch, &self.env)?;

        // Get the import library file extension from the used `dlltool` flavor.
        let implib_ext = dlltool_command.implib_file_ext();

        let implib_file = self.implib_file_path(out_dir, implib_ext);

        // Build the complete `dlltool` command with all required arguments.
        let mut command = dlltool_command.build(&defpath, &implib_file);

        // Run the selected `dlltool` executable to generate the import library.
        let status = command.status().map_err(|e| {
            let msg = format!("{:?} failed with {}", command, e);
            Error::new(e.kind(), msg)
        })?;

        if status.success() {
            Ok(())
        } else {
            let msg = format!("{:?} failed with {}", command, status);
            Err(Error::new(ErrorKind::Other, msg))
        }
    }

    /// Writes out the embedded Python library definitions file to `out_dir`.
    ///
    /// Returns the newly created `python3.def` or `pythonXY.def` file path.
    fn write_def_file(&self, out_dir: &Path) -> Result<PathBuf> {
        let (def_file, def_file_content) = match self.implementation {
            PythonImplementation::CPython => match self.version {
                None => ("python3.def", include_str!("python3.def")),
                Some((3, 7)) => ("python37.def", include_str!("python37.def")),
                Some((3, 8)) => ("python38.def", include_str!("python38.def")),
                Some((3, 9)) => ("python39.def", include_str!("python39.def")),
                Some((3, 10)) => ("python310.def", include_str!("python310.def")),
                Some((3, 11)) => ("python311.def", include_str!("python311.def")),
                Some((3, 12)) => ("python312.def", include_str!("python312.def")),
                Some((3, 13)) => match self.abiflags.as_deref() {
                    Some("t") => ("python313t.def", include_str!("python313t.def")),
                    None => ("python313.def", include_str!("python313.def")),
                    _ => return Err(Error::new(ErrorKind::Other, "Unsupported Python ABI flags")),
                },
                Some((3, 14)) => match self.abiflags.as_deref() {
                    Some("t") => ("python314t.def", include_str!("python314t.def")),
                    None => ("python314.def", include_str!("python314.def")),
                    _ => return Err(Error::new(ErrorKind::Other, "Unsupported Python ABI flags")),
                },
                _ => return Err(Error::new(ErrorKind::Other, "Unsupported Python version")),
            },
            PythonImplementation::PyPy => match self.version {
                Some((3, 7)) | Some((3, 8)) => ("libpypy3-c.def", include_str!("libpypy3-c.def")),
                Some((3, 9)) => ("libpypy3.9-c.def", include_str!("libpypy3.9-c.def")),
                Some((3, 10)) => ("libpypy3.10-c.def", include_str!("libpypy3.10-c.def")),
                Some((3, 11)) => ("libpypy3.11-c.def", include_str!("libpypy3.11-c.def")),
                _ => return Err(Error::new(ErrorKind::Other, "Unsupported PyPy version")),
            },
        };

        let mut defpath = out_dir.to_owned();
        defpath.push(def_file);

        write(&defpath, def_file_content)?;

        Ok(defpath)
    }

    /// Builds the generated import library file name.
    ///
    /// The output file extension is passed in `libext`.
    ///
    /// Returns the full import library file path under `out_dir`.
    fn implib_file_path(&self, out_dir: &Path, libext: &str) -> PathBuf {
        let abiflags = self.abiflags.as_deref().unwrap_or_default();
        let libname = match self.version {
            Some((major, minor)) => {
                format!("python{}{}{}{}", major, minor, abiflags, libext)
            }
            None => format!("python3{}", libext),
        };

        let mut libpath = out_dir.to_owned();
        libpath.push(libname);

        libpath
    }
}

/// Generates `python3.dll` import library directly from the embedded
/// Python Stable ABI definitions data for the specified compile target.
///
/// The import library file named `python3.dll.a` or `python3.lib` is created
/// in directory `out_dir`.
///
/// The compile target architecture name (as in `CARGO_CFG_TARGET_ARCH`)
/// is passed in `arch`.
///
/// The compile target environment ABI name (as in `CARGO_CFG_TARGET_ENV`)
/// is passed in `env`.
pub fn generate_implib_for_target(out_dir: &Path, arch: &str, env: &str) -> Result<()> {
    ImportLibraryGenerator::new(arch, env).generate(out_dir)
}

/// `dlltool` utility command builder
///
/// Supports Visual Studio `lib.exe`, MinGW, LLVM and Zig `dlltool` flavors.
#[derive(Debug)]
enum DllToolCommand {
    /// MinGW `dlltool` program (with prefix)
    Mingw { command: Command },
    /// LLVM `llvm-dlltool` program (no prefix)
    Llvm { command: Command, machine: String },
    /// MSVC `lib.exe` program (no prefix)
    LibExe { command: Command, machine: String },
    /// `zig dlltool` wrapper (no prefix)
    Zig { command: Command, machine: String },
}

impl DllToolCommand {
    /// Attempts to find the best matching `dlltool` flavor for the target.
    fn find_for_target(arch: &str, env: &str) -> Result<DllToolCommand> {
        // LLVM tools use their own target architecture names...
        let machine = match arch {
            "x86_64" => "i386:x86-64",
            "x86" => "i386",
            "aarch64" => "arm64",
            arch => arch,
        }
        .to_owned();

        // If `zig cc` is used as the linker, `zig dlltool` is the best choice.
        if let Some(command) = find_zig() {
            return Ok(DllToolCommand::Zig { command, machine });
        }

        match env {
            // 64-bit and 32-bit MinGW-w64 (aka `{x86_64,i686}-pc-windows-gnu`)
            "gnu" => Ok(DllToolCommand::Mingw {
                command: get_mingw_dlltool(arch)?,
            }),

            // MSVC ABI (multiarch)
            "msvc" => {
                if let Some(command) = find_lib_exe(arch) {
                    // MSVC tools use their own target architecture names...
                    let machine = match arch {
                        "x86_64" => "X64",
                        "x86" => "X86",
                        "aarch64" => "ARM64",
                        arch => arch,
                    }
                    .to_owned();

                    Ok(DllToolCommand::LibExe { command, machine })
                } else {
                    let command = Command::new(DLLTOOL_MSVC);

                    Ok(DllToolCommand::Llvm { command, machine })
                }
            }
            _ => {
                let msg = format!("Unsupported target env ABI '{}'", env);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }
    }

    /// Returns the import library file extension used by
    /// this `dlltool` flavor.
    fn implib_file_ext(&self) -> &'static str {
        if let DllToolCommand::Mingw { .. } = self {
            IMPLIB_EXT_GNU
        } else {
            IMPLIB_EXT_MSVC
        }
    }

    /// Generates the complete `dlltool` executable invocation command.
    fn build(self, defpath: &Path, libpath: &Path) -> Command {
        match self {
            Self::Mingw { mut command } => {
                command
                    .arg("--input-def")
                    .arg(defpath)
                    .arg("--output-lib")
                    .arg(libpath);

                command
            }
            Self::Llvm {
                mut command,
                machine,
            } => {
                command
                    .arg("-m")
                    .arg(machine)
                    .arg("-d")
                    .arg(defpath)
                    .arg("-l")
                    .arg(libpath);

                command
            }
            Self::LibExe {
                mut command,
                machine,
            } => {
                command
                    .arg(format!("/MACHINE:{}", machine))
                    .arg(format!("/DEF:{}", defpath.display()))
                    .arg(format!("/OUT:{}", libpath.display()));

                command
            }
            Self::Zig {
                mut command,
                machine,
            } => {
                // Same as `llvm-dlltool`, but invoked as `zig dlltool`.
                command
                    .arg("dlltool")
                    .arg("-m")
                    .arg(machine)
                    .arg("-d")
                    .arg(defpath)
                    .arg("-l")
                    .arg(libpath);

                command
            }
        }
    }
}

/// Chooses the appropriate MinGW-w64 `dlltool` executable
/// for the target architecture.
///
/// Examines the user-provided `PYO3_MINGW_DLLTOOL` environment variable first
/// and falls back to the default MinGW-w64 arch prefixes.
fn get_mingw_dlltool(arch: &str) -> Result<Command> {
    if let Ok(user_dlltool) = env::var("PYO3_MINGW_DLLTOOL") {
        Ok(Command::new(user_dlltool))
    } else {
        let prefix_dlltool = match arch {
            // 64-bit MinGW-w64 (aka `x86_64-pc-windows-gnu`)
            "x86_64" => Ok(DLLTOOL_GNU),
            // 32-bit MinGW-w64 (aka `i686-pc-windows-gnu`)
            "x86" => Ok(DLLTOOL_GNU_32),
            // AArch64?
            _ => {
                let msg = format!("Unsupported MinGW target arch '{}'", arch);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }?;

        Ok(Command::new(prefix_dlltool))
    }
}

/// Finds the `zig` executable (when built by `maturin --zig`).
///
/// Examines the `ZIG_COMMAND` environment variable
/// to find out if `zig cc` is being used as the linker.
fn find_zig() -> Option<Command> {
    // `ZIG_COMMAND` may contain simply `zig` or `/usr/bin/zig`,
    // or a more complex construct like `python3 -m ziglang`.
    let zig_command = env::var("ZIG_COMMAND").ok()?;

    // Try to emulate `sh -c ${ZIG_COMMAND}`.
    let mut zig_cmdlet = zig_command.split_ascii_whitespace();

    // Extract the main program component (e.g. `zig` or `python3`).
    let mut zig = Command::new(zig_cmdlet.next()?);

    // Append the rest of the commandlet.
    zig.args(zig_cmdlet);

    Some(zig)
}

/// Finds Visual Studio `lib.exe` when running on Windows.
#[cfg(windows)]
fn find_lib_exe(arch: &str) -> Option<Command> {
    let target = match arch {
        "x86_64" => "x86_64-pc-windows-msvc",
        "x86" => "i686-pc-windows-msvc",
        "aarch64" => "aarch64-pc-windows-msvc",
        _ => return None,
    };

    cc::windows_registry::find(target, LIB_MSVC)
}

#[cfg(not(windows))]
fn find_lib_exe(_arch: &str) -> Option<Command> {
    None
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[cfg(unix)]
    #[test]
    fn generate() {
        // FIXME: Use "target/<arch>" dirs for temporary files.
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("target");
        dir.push("x86_64-pc-windows-gnu");
        dir.push("python3-dll");

        ImportLibraryGenerator::new("x86_64", "gnu")
            .generate(&dir)
            .unwrap();

        for minor in 7..=14 {
            ImportLibraryGenerator::new("x86_64", "gnu")
                .version(Some((3, minor)))
                .generate(&dir)
                .unwrap();
        }

        // Free-threaded CPython v3.13+
        for minor in 13..=14 {
            ImportLibraryGenerator::new("x86_64", "gnu")
                .version(Some((3, minor)))
                .abiflags(Some("t"))
                .generate(&dir)
                .unwrap();
        }

        // PyPy
        for minor in 7..=11 {
            ImportLibraryGenerator::new("x86_64", "gnu")
                .version(Some((3, minor)))
                .implementation(PythonImplementation::PyPy)
                .generate(&dir)
                .unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn generate_gnu32() {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("target");
        dir.push("i686-pc-windows-gnu");
        dir.push("python3-dll");

        generate_implib_for_target(&dir, "x86", "gnu").unwrap();
    }

    #[test]
    fn generate_msvc() {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("target");
        dir.push("x86_64-pc-windows-msvc");
        dir.push("python3-dll");

        ImportLibraryGenerator::new("x86_64", "msvc")
            .generate(&dir)
            .unwrap();

        for minor in 7..=14 {
            ImportLibraryGenerator::new("x86_64", "msvc")
                .version(Some((3, minor)))
                .generate(&dir)
                .unwrap();
        }

        // Free-threaded CPython v3.13+
        for minor in 13..=14 {
            ImportLibraryGenerator::new("x86_64", "msvc")
                .version(Some((3, minor)))
                .abiflags(Some("t"))
                .generate(&dir)
                .unwrap();
        }

        // PyPy
        for minor in 7..=11 {
            ImportLibraryGenerator::new("x86_64", "msvc")
                .version(Some((3, minor)))
                .implementation(PythonImplementation::PyPy)
                .generate(&dir)
                .unwrap();
        }
    }

    #[test]
    fn generate_msvc32() {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("target");
        dir.push("i686-pc-windows-msvc");
        dir.push("python3-dll");

        generate_implib_for_target(&dir, "x86", "msvc").unwrap();
    }

    #[test]
    fn generate_msvc_arm64() {
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("target");
        dir.push("aarch64-pc-windows-msvc");
        dir.push("python3-dll");

        ImportLibraryGenerator::new("aarch64", "msvc")
            .generate(&dir)
            .unwrap();

        for minor in 7..=14 {
            ImportLibraryGenerator::new("aarch64", "msvc")
                .version(Some((3, minor)))
                .generate(&dir)
                .unwrap();
        }

        // Free-threaded CPython v3.13+
        for minor in 13..=14 {
            let mut generator = ImportLibraryGenerator::new("aarch64", "msvc");
            generator.version(Some((3, minor))).abiflags(Some("t"));
            let implib_file_path = generator.implib_file_path(&dir, IMPLIB_EXT_MSVC);
            let implib_file_stem = implib_file_path.file_stem().unwrap().to_str().unwrap();
            assert!(implib_file_stem.ends_with("t"));

            generator.generate(&dir).unwrap();
        }

        // PyPy
        for minor in 7..=11 {
            ImportLibraryGenerator::new("aarch64", "msvc")
                .version(Some((3, minor)))
                .implementation(PythonImplementation::PyPy)
                .generate(&dir)
                .unwrap();
        }
    }
}
