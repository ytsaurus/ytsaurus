// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{exceptions::ZstdError, zstd_safe::DCtx},
    pyo3::{
        buffer::PyBuffer,
        exceptions::{PyOSError, PyValueError},
        prelude::*,
        types::PyBytes,
        IntoPyObjectExt,
    },
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdDecompressionWriter {
    dctx: Arc<DCtx<'static>>,
    writer: PyObject,
    write_size: usize,
    write_return_read: bool,
    closefd: bool,
    entered: bool,
    closing: bool,
    closed: bool,
}

unsafe impl Sync for ZstdDecompressionWriter {}

impl ZstdDecompressionWriter {
    pub fn new(
        py: Python,
        dctx: Arc<DCtx<'static>>,
        writer: &Bound<'_, PyAny>,
        write_size: usize,
        write_return_read: bool,
        closefd: bool,
    ) -> PyResult<Self> {
        Ok(Self {
            dctx,
            writer: writer.into_py_any(py)?,
            write_size,
            write_return_read,
            closefd,
            entered: false,
            closing: false,
            closed: false,
        })
    }
}

#[pymethods]
impl ZstdDecompressionWriter {
    // PyIterProtocol.

    fn __iter__(slf: PyRef<Self>) -> PyResult<()> {
        let py = slf.py();
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn __next__(slf: PyRef<Self>) -> PyResult<Option<()>> {
        let py = slf.py();
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn __enter__<'p>(mut slf: PyRefMut<'p, Self>, _py: Python<'p>) -> PyResult<PyRefMut<'p, Self>> {
        if slf.closed {
            Err(PyValueError::new_err("stream is closed"))
        } else if slf.entered {
            Err(ZstdError::new_err("cannot __enter__ multiple times"))
        } else {
            slf.entered = true;
            Ok(slf)
        }
    }

    #[allow(unused_variables)]
    fn __exit__<'p>(
        mut slf: PyRefMut<'p, Self>,
        py: Python<'p>,
        exc_type: PyObject,
        exc_value: PyObject,
        exc_tb: PyObject,
    ) -> PyResult<bool> {
        slf.entered = false;
        slf.close(py)?;

        // TODO release cctx and writer?

        Ok(false)
    }

    fn memory_size(&self) -> usize {
        self.dctx.memory_size()
    }

    fn close(&mut self, py: Python) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }

        self.closing = true;
        let res = self.flush(py);
        self.closing = false;
        self.closed = true;

        res?;

        if let Ok(close) = self.writer.getattr(py, "close") {
            if self.closefd {
                close.call0(py)?;
            }
        }

        Ok(())
    }

    #[getter]
    fn closed(&self) -> bool {
        self.closed
    }

    fn fileno(&self, py: Python) -> PyResult<PyObject> {
        if let Ok(fileno) = self.writer.getattr(py, "fileno") {
            fileno.call0(py)
        } else {
            Err(PyOSError::new_err(
                "filenot not available on underlying writer",
            ))
        }
    }

    fn flush(&self, py: Python) -> PyResult<PyObject> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        if let Ok(flush) = self.writer.getattr(py, "flush") {
            if !self.closing {
                return flush.call0(py);
            }
        }

        Ok(py.None())
    }

    fn isatty(&self) -> bool {
        false
    }

    fn readable(&self) -> bool {
        false
    }

    #[pyo3(signature = (size=None))]
    #[allow(unused_variables)]
    fn readline(&self, py: Python, size: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (hint=None))]
    #[allow(unused_variables)]
    fn readlines(&self, py: Python, hint: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (offset, whence=None))]
    #[allow(unused_variables)]
    fn seek(&self, py: Python, offset: isize, whence: Option<i32>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn seekable(&self) -> bool {
        false
    }

    fn tell(&self, py: Python) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (size=None))]
    #[allow(unused_variables)]
    fn truncate(&self, py: Python, size: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn writable(&self) -> bool {
        true
    }

    #[pyo3(signature = (lines))]
    #[allow(unused_variables)]
    fn writelines(&self, py: Python, lines: &Bound<'_, PyAny>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (size=None))]
    #[allow(unused_variables)]
    fn read(&self, py: Python, size: Option<usize>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn readall(&self, py: Python) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[allow(unused_variables)]
    fn readinto(&self, py: Python, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (size=None))]
    #[allow(unused_variables)]
    fn read1(&self, py: Python, size: Option<usize>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[allow(unused_variables)]
    fn readinto1(&self, py: Python, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    fn write(&self, py: Python, buffer: PyBuffer<u8>) -> PyResult<usize> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        let mut total_write = 0;

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: buffer.buf_ptr(),
            size: buffer.len_bytes(),
            pos: 0,
        };

        let mut dest_buffer = Vec::with_capacity(self.write_size);

        while in_buffer.pos < in_buffer.size {
            self.dctx
                .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
                .map_err(|msg| ZstdError::new_err(format!("zstd decompress error: {}", msg)))?;

            if !dest_buffer.is_empty() {
                // TODO avoid buffer copy.
                let chunk = PyBytes::new(py, &dest_buffer);
                self.writer.call_method1(py, "write", (chunk,))?;
                total_write += dest_buffer.len();
                dest_buffer.clear();
            }
        }

        if self.write_return_read {
            Ok(in_buffer.pos)
        } else {
            Ok(total_write)
        }
    }
}
