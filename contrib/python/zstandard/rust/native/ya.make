CRATE(backend_rust)

CRATE_VERSION(0.25.0)
CRATE_EDITION(2021)
CRATE_ROOT(rust-ext/src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    rust-ext/src/buffers.rs
    rust-ext/src/compression_chunker.rs
    rust-ext/src/compression_dict.rs
    rust-ext/src/compression_parameters.rs
    rust-ext/src/compression_reader.rs
    rust-ext/src/compression_writer.rs
    rust-ext/src/compressionobj.rs
    rust-ext/src/compressor.rs
    rust-ext/src/compressor_iterator.rs
    rust-ext/src/compressor_multi.rs
    rust-ext/src/constants.rs
    rust-ext/src/decompression_reader.rs
    rust-ext/src/decompression_writer.rs
    rust-ext/src/decompressionobj.rs
    rust-ext/src/decompressor.rs
    rust-ext/src/decompressor_iterator.rs
    rust-ext/src/decompressor_multi.rs
    rust-ext/src/exceptions.rs
    rust-ext/src/frame_parameters.rs
    rust-ext/src/stream.rs
    rust-ext/src/zstd_safe.rs
)

CRATE_INPUTS(Cargo.lock LICENSE)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/libc-0.2.191
    contrib/crates/num_cpus-1.17.0
    contrib/crates/pyo3-0.25.1
    contrib/crates/rayon-1.12.0
    contrib/crates/zstd-safe-7.3.0
    contrib/crates/zstd-sys-2.1.2+zstd.1.5.7
)

CRATE_PROC_MACROS(
    contrib/crates/indoc-2.0.8
    contrib/crates/pyo3-macros-0.25.1
    contrib/crates/serde_derive-1.0.229
)

LICENSE(BSD-3-Clause)
VERSION(0.25.0)

END()
