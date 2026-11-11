// Copyright (c) 2020-present, Gregory Szorc
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
pub struct ZstdCompressionChunker {
    cctx: Arc<CCtx<'static>>,
    chunk_size: usize,
    finished: bool,
    iterator: Option<Py<ZstdCompressionChunkerIterator>>,
    partial_buffer: Option<Vec<u8>>,
}

unsafe impl Sync for ZstdCompressionChunker {}

impl ZstdCompressionChunker {
    pub fn new(cctx: Arc<CCtx<'static>>, chunk_size: usize) -> PyResult<Self> {
        Ok(Self {
            cctx,
            chunk_size,
            finished: false,
            iterator: None,
            partial_buffer: None,
        })
    }
}

impl ZstdCompressionChunker {
    fn ensure_state(&mut self, py: Python) {
        if let Some(it) = &self.iterator {
            if it.borrow(py).finished {
                if it.borrow(py).mode == IteratorMode::Finish {
                    self.finished = true;
                }

                if !it.borrow(py).dest_buffer.is_empty() {
                    // TODO can we avoid the memory copy?
                    // Vec.clone() won't preserve the capacity of the source.
                    // So we create a new Vec with desired capacity and copy to it.
                    // This is strictly better than a clone + resize.
                    let mut dest_buffer = Vec::with_capacity(self.chunk_size);
                    unsafe {
                        dest_buffer.set_len(it.borrow(py).dest_buffer.len());
                    }
                    dest_buffer.copy_from_slice(it.borrow(py).dest_buffer.as_slice());
                    self.partial_buffer = Some(dest_buffer);
                }

                self.iterator = None;
            }
        }
    }

    fn get_dest_buffer(&mut self) -> Vec<u8> {
        self.partial_buffer
            .take()
            .unwrap_or_else(|| Vec::with_capacity(self.chunk_size))
    }
}

#[pymethods]
impl ZstdCompressionChunker {
    fn compress(
        &mut self,
        py: Python,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<ZstdCompressionChunkerIterator>> {
        self.ensure_state(py);

        if self.finished {
            return Err(ZstdError::new_err(
                "cannot call compress() after compression finished",
            ));
        }

        let source = make_in_buffer_source(py, data, zstd_safe::CCtx::in_size())?;

        let it = Bound::new(
            py,
            ZstdCompressionChunkerIterator {
                cctx: self.cctx.clone(),
                source,
                mode: IteratorMode::Normal,
                dest_buffer: self.get_dest_buffer(),
                finished: false,
            },
        )?;

        self.iterator = Some(it.clone().unbind());

        Ok(it.unbind())
    }

    fn flush<'p>(&mut self, py: Python<'p>) -> PyResult<Py<ZstdCompressionChunkerIterator>> {
        self.ensure_state(py);

        if self.finished {
            return Err(ZstdError::new_err(
                "cannot call flush() after compression finished",
            ));
        }

        if self.iterator.is_some() {
            return Err(ZstdError::new_err(
                "cannot call flush() before consuming output from previous operation",
            ));
        }

        let source = make_in_buffer_source(py, &PyBytes::new(py, &[]), zstd_safe::CCtx::in_size())?;

        let it = Bound::new(
            py,
            ZstdCompressionChunkerIterator {
                cctx: self.cctx.clone(),
                source,
                mode: IteratorMode::Flush,
                dest_buffer: self.get_dest_buffer(),
                finished: false,
            },
        )?;

        self.iterator = Some(it.clone().unbind());

        Ok(it.unbind())
    }

    fn finish<'p>(&mut self, py: Python<'p>) -> PyResult<Py<ZstdCompressionChunkerIterator>> {
        self.ensure_state(py);

        if self.finished {
            return Err(ZstdError::new_err(
                "cannot call finish() after compression finished",
            ));
        }

        if self.iterator.is_some() {
            return Err(ZstdError::new_err(
                "cannot call finish() before consuming output from previous operation",
            ));
        }

        let source = make_in_buffer_source(py, &PyBytes::new(py, &[]), zstd_safe::CCtx::in_size())?;

        let it = Bound::new(
            py,
            ZstdCompressionChunkerIterator {
                cctx: self.cctx.clone(),
                source,
                mode: IteratorMode::Finish,
                dest_buffer: self.get_dest_buffer(),
                finished: false,
            },
        )?;

        self.iterator = Some(it.clone().unbind());

        Ok(it.unbind())
    }
}

#[derive(Debug, PartialEq)]
enum IteratorMode {
    Normal,
    Flush,
    Finish,
}

#[pyclass(module = "zstandard.backend_rust")]
struct ZstdCompressionChunkerIterator {
    cctx: Arc<CCtx<'static>>,
    source: Box<dyn InBufferSource + Send>,
    mode: IteratorMode,
    dest_buffer: Vec<u8>,
    finished: bool,
}

unsafe impl Sync for ZstdCompressionChunkerIterator {}

#[pymethods]
impl ZstdCompressionChunkerIterator {
    // PyIterProtocol.

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<PyObject>> {
        if slf.finished {
            return Ok(None);
        }

        let py = unsafe { Python::assume_gil_acquired() };

        // Consume any data left in the input.
        while let Some(mut in_buffer) = slf.source.input_buffer(py)? {
            let old_pos = in_buffer.pos;

            slf.cctx
                .clone()
                .compress_into_vec(
                    &mut slf.dest_buffer,
                    &mut in_buffer,
                    zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
                )
                .map_err(|msg| ZstdError::new_err(format!("zstd compress error: {}", msg)))?;

            slf.source.record_bytes_read(in_buffer.pos - old_pos);

            // If we produced a full output chunk, emit it.
            if slf.dest_buffer.len() == slf.dest_buffer.capacity() {
                let chunk = PyBytes::new(py, &slf.dest_buffer);
                slf.dest_buffer.clear();

                return Ok(Some(chunk.into_py_any(py)?));
            }

            // Else continue to compress available input data.
            continue;
        }

        // No more input data. A partial chunk may be in the destination
        // buffer. If we're in normal compression mode, we're done. Otherwise
        // if we're in flush or finish mode, we need to emit what data remains.

        let flush_mode = match slf.mode {
            IteratorMode::Normal => {
                slf.finished = true;
                return Ok(None);
            }
            IteratorMode::Flush => zstd_sys::ZSTD_EndDirective::ZSTD_e_flush,
            IteratorMode::Finish => zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
        };

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null(),
            size: 0,
            pos: 0,
        };

        let zresult = slf
            .cctx
            .clone()
            .compress_into_vec(&mut slf.dest_buffer, &mut in_buffer, flush_mode)
            .map_err(|msg| ZstdError::new_err(format!("zstd compress error: {}", msg)))?;

        // When flushing or finishing, we always emit data in the output
        // buffer. But the operation could fill the output buffer and not be
        // finished.

        // If we didn't emit anything to the output buffer, we must be finished.
        // Update state and stop iteration.
        if slf.dest_buffer.is_empty() {
            slf.finished = true;
            return Ok(None);
        }

        // If the flush or finish didn't fill the output buffer, we must
        // be done.
        // If compressor said operation is finished, we are also done.
        if zresult == 0 || slf.dest_buffer.len() < slf.dest_buffer.capacity() {
            slf.finished = true;
        }

        let chunk = PyBytes::new(py, &slf.dest_buffer);
        slf.dest_buffer.clear();

        Ok(Some(chunk.into_py_any(py)?))
    }
}
