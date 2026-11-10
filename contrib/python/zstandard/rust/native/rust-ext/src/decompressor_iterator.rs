// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        exceptions::ZstdError,
        stream::{make_in_buffer_source, InBufferSource},
        zstd_safe::DCtx,
    },
    pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes, IntoPyObjectExt},
    std::{cmp::min, sync::Arc},
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdDecompressorIterator {
    dctx: Arc<DCtx<'static>>,
    source: Box<dyn InBufferSource + Send>,
    write_size: usize,
    finished_output: bool,
}

unsafe impl Sync for ZstdDecompressorIterator {}

#[pymethods]
impl ZstdDecompressorIterator {
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

        // While input is available.
        while let Some(mut in_buffer) = slf.source.input_buffer(py)? {
            let old_pos = in_buffer.pos;

            let zresult = slf
                .dctx
                .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
                .map_err(|msg| ZstdError::new_err(format!("zstd decompress error: {}", msg)))?;

            slf.source.record_bytes_read(in_buffer.pos - old_pos);

            if zresult == 0 {
                slf.finished_output = true;
            }

            // Emit chunk if output buffer has data.
            if !dest_buffer.is_empty() {
                // TODO avoid buffer copy.
                let chunk = PyBytes::new(py, &dest_buffer);
                return Ok(Some(chunk.into_py_any(py)?));
            }

            // Repeat loop to collect more input data.
            continue;
        }

        // Input is exhausted. Emit what we have or finish.
        if !dest_buffer.is_empty() {
            // TODO avoid buffer copy.
            let chunk = PyBytes::new(py, &dest_buffer);
            Ok(Some(chunk.into_py_any(py)?))
        } else {
            Ok(None)
        }
    }
}

impl ZstdDecompressorIterator {
    pub fn new(
        py: Python,
        dctx: Arc<DCtx<'static>>,
        reader: &Bound<'_, PyAny>,
        read_size: usize,
        write_size: usize,
        skip_bytes: usize,
    ) -> PyResult<Self> {
        let mut source = make_in_buffer_source(py, reader, read_size)?;

        let mut skip_bytes = skip_bytes;
        while skip_bytes > 0 {
            let in_buffer = source
                .input_buffer(py)?
                .ok_or_else(|| PyValueError::new_err("skip_bytes larger than first input chunk"))?;

            let read = min(skip_bytes, in_buffer.size - in_buffer.pos);
            source.record_bytes_read(read);
            skip_bytes -= read;
        }

        Ok(Self {
            dctx,
            source,
            write_size,
            finished_output: false,
        })
    }
}
