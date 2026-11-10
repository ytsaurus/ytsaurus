// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        exceptions::ZstdError,
        stream::{make_in_buffer_source, InBufferSource},
        zstd_safe::CCtx,
    },
    pyo3::{prelude::*, types::PyBytes, IntoPyObjectExt},
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdCompressorIterator {
    cctx: Arc<CCtx<'static>>,
    source: Box<dyn InBufferSource + Send>,
    write_size: usize,
    finished_output: bool,
}

unsafe impl Sync for ZstdCompressorIterator {}

#[pymethods]
impl ZstdCompressorIterator {
    // PyIterProtocol.

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<PyObject>> {
        if slf.finished_output {
            return Ok(None);
        }

        let py = unsafe { Python::assume_gil_acquired() };

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(slf.write_size);

        // Feed data into the compressor until there is output data.
        while let Some(mut in_buffer) = slf.source.input_buffer(py)? {
            let old_pos = in_buffer.pos;

            slf.cctx
                .compress_into_vec(
                    &mut dest_buffer,
                    &mut in_buffer,
                    zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
                )
                .map_err(|msg| ZstdError::new_err(format!("zstd compress error: {}", msg)))?;

            slf.source.record_bytes_read(in_buffer.pos - old_pos);

            // Emit compressed data, if available.
            if !dest_buffer.is_empty() {
                // TODO avoid buffer copy
                let chunk = PyBytes::new(py, &dest_buffer);

                return Ok(Some(chunk.into_py_any(py)?));
            }

            // Else read another chunk in hopes of producing output data.
            continue;
        }

        // Input data is exhausted. End the stream and emit what remains.

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null_mut(),
            size: 0,
            pos: 0,
        };

        let zresult = slf
            .cctx
            .compress_into_vec(
                &mut dest_buffer,
                &mut in_buffer,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|msg| {
                ZstdError::new_err(format!("error ending compression stream: {}", msg))
            })?;

        if zresult == 0 {
            slf.finished_output = true;
        }

        if !dest_buffer.is_empty() {
            // TODO avoid buffer copy.
            let chunk = PyBytes::new(py, &dest_buffer);

            return Ok(Some(chunk.into_py_any(py)?));
        }

        Ok(None)
    }
}

impl ZstdCompressorIterator {
    pub fn new(
        py: Python,
        cctx: Arc<CCtx<'static>>,
        reader: &Bound<'_, PyAny>,
        size: u64,
        read_size: usize,
        write_size: usize,
    ) -> PyResult<Self> {
        let source = make_in_buffer_source(py, reader, read_size)?;

        let size = match source.source_size() {
            Some(size) => size as _,
            None => size,
        };

        cctx.set_pledged_source_size(size)
            .map_err(|msg| ZstdError::new_err(format!("error setting source size: {}", msg)))?;

        Ok(Self {
            cctx,
            source,
            write_size,
            finished_output: false,
        })
    }
}
