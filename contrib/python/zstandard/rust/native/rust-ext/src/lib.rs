// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use pyo3::{prelude::*, types::PySet};

mod buffers;
mod compression_chunker;
mod compression_dict;
mod compression_parameters;
mod compression_reader;
mod compression_writer;
mod compressionobj;
mod compressor;
mod compressor_iterator;
mod compressor_multi;
mod constants;
mod decompression_reader;
mod decompression_writer;
mod decompressionobj;
mod decompressor;
mod decompressor_iterator;
mod decompressor_multi;
mod exceptions;
mod frame_parameters;
mod stream;
mod zstd_safe;

use exceptions::ZstdError;

// Remember to change the string in c-ext/python-zstandard.h, zstandard/__init__.py,
// and debian/changelog as well.
const VERSION: &'static str = "0.25.0";

#[pymodule(gil_used = false)]
fn backend_rust(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let features = PySet::new(
        py,
        &[
            "buffer_types",
            "multi_compress_to_buffer",
            "multi_decompress_to_buffer",
        ],
    )?;
    module.add("backend_features", features)?;

    crate::buffers::init_module(module)?;
    crate::compression_dict::init_module(module)?;
    crate::compression_parameters::init_module(module)?;
    crate::compressor::init_module(module)?;
    crate::constants::init_module(py, module)?;
    crate::decompressor::init_module(module)?;
    crate::exceptions::init_module(py, module)?;
    crate::frame_parameters::init_module(module)?;

    Ok(())
}
