// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    pyo3::{buffer::PyBuffer, exceptions::PyValueError, prelude::*, IntoPyObjectExt},
    zstd_sys::ZSTD_inBuffer,
};

/// Describes a type that can be resolved to a `zstd_sys::ZSTD_inBuffer`.
pub trait InBufferSource {
    /// Obtain the PyObject this instance is reading from.
    fn source_object(&self) -> &PyObject;

    /// The size of the input object, if available.
    fn source_size(&self) -> Option<usize>;

    /// Obtain a `zstd_sys::ZSTD_inBuffer` with input to feed to a compressor.
    fn input_buffer(&mut self, py: Python) -> PyResult<Option<ZSTD_inBuffer>>;

    /// Record that `count` bytes were read from the input buffer.
    fn record_bytes_read(&mut self, count: usize);

    /// Whether source data has been fully consumed.
    fn finished(&self) -> bool;
}

/// A data source where data is obtaine by calling `read()`.
struct ReadSource {
    source: PyObject,
    buffer: Option<PyBuffer<u8>>,
    read_size: usize,
    finished: bool,
    offset: usize,
}

impl InBufferSource for ReadSource {
    fn source_object(&self) -> &PyObject {
        &self.source
    }

    fn source_size(&self) -> Option<usize> {
        None
    }

    fn input_buffer(&mut self, py: Python) -> PyResult<Option<ZSTD_inBuffer>> {
        if self.finished() {
            Ok(None)
        // If we have a buffer, return remaining data in it.
        } else if let Some(buffer) = &self.buffer {
            Ok(Some(ZSTD_inBuffer {
                src: buffer.buf_ptr(),
                size: buffer.len_bytes(),
                pos: self.offset,
            }))
        // Attempt to read new data.
        } else {
            let data = self.source.call_method1(py, "read", (self.read_size,))?;
            let buffer = PyBuffer::get(data.bind(py))?;

            if buffer.len_bytes() == 0 {
                self.finished = true;
                Ok(None)
            } else {
                self.buffer = Some(buffer);
                self.offset = 0;

                Ok(Some(ZSTD_inBuffer {
                    src: self.buffer.as_ref().unwrap().buf_ptr(),
                    size: self.buffer.as_ref().unwrap().len_bytes(),
                    pos: self.offset,
                }))
            }
        }
    }

    fn record_bytes_read(&mut self, count: usize) {
        let buffer = self.buffer.as_ref().expect("buffer should be present");

        self.offset += count;

        // If we've exhausted the input buffer, drop it. On next call
        // to input_buffer() we'll try to read() more data and finish
        // the stream if nothing can be read.
        if self.offset >= buffer.len_bytes() {
            self.buffer = None;
        }
    }

    fn finished(&self) -> bool {
        self.finished
    }
}

/// A data source where data is obtained from a `PyObject`
/// conforming to the buffer protocol.
struct BufferSource {
    source: PyObject,
    buffer: PyBuffer<u8>,
    offset: usize,
}

impl InBufferSource for BufferSource {
    fn source_object(&self) -> &PyObject {
        &self.source
    }

    fn source_size(&self) -> Option<usize> {
        Some(self.buffer.len_bytes())
    }

    fn input_buffer(&mut self, _py: Python) -> PyResult<Option<ZSTD_inBuffer>> {
        if self.finished() {
            Ok(None)
        } else {
            Ok(Some(ZSTD_inBuffer {
                src: self.buffer.buf_ptr(),
                size: self.buffer.len_bytes(),
                pos: self.offset,
            }))
        }
    }

    fn record_bytes_read(&mut self, count: usize) {
        self.offset += count;
    }

    fn finished(&self) -> bool {
        self.offset >= self.buffer.len_bytes()
    }
}

pub(crate) fn make_in_buffer_source(
    py: Python,
    source: &Bound<'_, PyAny>,
    read_size: usize,
) -> PyResult<Box<dyn InBufferSource + Send>> {
    if source.hasattr("read")? {
        Ok(Box::new(ReadSource {
            source: source.into_py_any(py)?,
            buffer: None,
            read_size,
            finished: false,
            offset: 0,
        }))
    } else {
        let buffer = PyBuffer::get(&source.as_borrowed()).map_err(|_| {
            PyValueError::new_err(
                "must pass an object with a read() method or conforms to buffer protocol",
            )
        })?;

        Ok(Box::new(BufferSource {
            source: source.into_py_any(py)?,
            buffer,
            offset: 0,
        }))
    }
}
