// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        constants::{COMPRESSOBJ_FLUSH_BLOCK, COMPRESSOBJ_FLUSH_FINISH},
        zstd_safe::CCtx,
        ZstdError,
    },
    pyo3::{buffer::PyBuffer, exceptions::PyValueError, prelude::*, types::PyBytes},
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdCompressionObj {
    cctx: Arc<CCtx<'static>>,
    finished: bool,
}

unsafe impl Sync for ZstdCompressionObj {}

impl ZstdCompressionObj {
    pub fn new(cctx: Arc<CCtx<'static>>) -> PyResult<Self> {
        Ok(ZstdCompressionObj {
            cctx,
            finished: false,
        })
    }
}

#[pymethods]
impl ZstdCompressionObj {
    fn compress<'p>(&self, py: Python<'p>, buffer: PyBuffer<u8>) -> PyResult<Bound<'p, PyBytes>> {
        if self.finished {
            return Err(ZstdError::new_err(
                "cannot call compress() after compressor finished",
            ));
        }

        let mut source = unsafe {
            std::slice::from_raw_parts::<u8>(buffer.buf_ptr() as *const _, buffer.len_bytes())
        };

        // TODO consider collecting chunks and joining
        // TODO try to use zero copy into return value.
        let mut compressed = Vec::new();
        let write_size = zstd_safe::CCtx::out_size();

        let cctx = &self.cctx;
        while !source.is_empty() {
            let result = py
                .allow_threads(|| {
                    cctx.compress_chunk(
                        source,
                        zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
                        write_size,
                    )
                })
                .or_else(|msg| Err(ZstdError::new_err(format!("zstd compress error: {}", msg))))?;

            compressed.extend(result.0);
            source = result.1;
        }

        Ok(PyBytes::new(py, &compressed))
    }

    #[pyo3(signature = (flush_mode=None))]
    fn flush<'p>(
        &mut self,
        py: Python<'p>,
        flush_mode: Option<i32>,
    ) -> PyResult<Bound<'p, PyBytes>> {
        let flush_mode = if let Some(flush_mode) = flush_mode {
            match flush_mode {
                COMPRESSOBJ_FLUSH_FINISH => Ok(zstd_sys::ZSTD_EndDirective::ZSTD_e_end),
                COMPRESSOBJ_FLUSH_BLOCK => Ok(zstd_sys::ZSTD_EndDirective::ZSTD_e_flush),
                _ => Err(PyValueError::new_err("flush mode not recognized")),
            }
        } else {
            Ok(zstd_sys::ZSTD_EndDirective::ZSTD_e_end)
        }?;

        if self.finished {
            return Err(ZstdError::new_err("compressor object already finished"));
        }

        if flush_mode == zstd_sys::ZSTD_EndDirective::ZSTD_e_end {
            self.finished = true;
        }

        let write_size = zstd_safe::CCtx::out_size();
        let cctx = &self.cctx;

        // TODO avoid extra buffer copy.
        let mut result = Vec::new();

        loop {
            let (chunk, _, call_again) = py
                .allow_threads(|| cctx.compress_chunk(&[], flush_mode, write_size))
                .or_else(|msg| {
                    Err(ZstdError::new_err(format!(
                        "error ending compression stream: {}",
                        msg
                    )))
                })?;

            result.extend(&chunk);

            if !call_again {
                return Ok(PyBytes::new(py, &result));
            }
        }
    }
}
