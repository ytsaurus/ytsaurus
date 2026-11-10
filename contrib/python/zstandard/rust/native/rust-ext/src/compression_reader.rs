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
    pyo3::{
        buffer::PyBuffer,
        exceptions::{PyOSError, PyValueError},
        prelude::*,
        types::{PyBytes, PyList},
    },
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdCompressionReader {
    cctx: Arc<CCtx<'static>>,
    source: Box<dyn InBufferSource + Send>,
    closefd: bool,
    closed: bool,
    entered: bool,
    bytes_compressed: usize,
    finished_output: bool,
}

unsafe impl Sync for ZstdCompressionReader {}

impl ZstdCompressionReader {
    pub fn new(
        py: Python,
        cctx: Arc<CCtx<'static>>,
        reader: &Bound<'_, PyAny>,
        size: u64,
        read_size: usize,
        closefd: bool,
    ) -> PyResult<Self> {
        let source = make_in_buffer_source(py, reader, read_size)?;

        let size = match source.source_size() {
            Some(size) => size as _,
            None => size,
        };

        cctx.set_pledged_source_size(size).or_else(|msg| {
            Err(ZstdError::new_err(format!(
                "error setting source size: {}",
                msg
            )))
        })?;

        Ok(Self {
            cctx,
            source,
            closefd,
            closed: false,
            entered: false,
            bytes_compressed: 0,
            finished_output: false,
        })
    }
}

impl ZstdCompressionReader {
    fn compress_into_buffer(
        &mut self,
        py: Python,
        out_buffer: &mut zstd_sys::ZSTD_outBuffer,
    ) -> PyResult<bool> {
        if let Some(mut in_buffer) = self.source.input_buffer(py)? {
            let old_in_pos = in_buffer.pos;
            let old_out_pos = out_buffer.pos;

            self.cctx
                .compress_buffers(
                    out_buffer,
                    &mut in_buffer,
                    zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
                )
                .map_err(|msg| ZstdError::new_err(format!("zstd compress error: {}", msg)))?;

            self.bytes_compressed += out_buffer.pos - old_out_pos;
            self.source.record_bytes_read(in_buffer.pos - old_in_pos);

            Ok(out_buffer.pos > 0 && out_buffer.pos == out_buffer.size)
        } else {
            Ok(false)
        }
    }

    fn compress_into_vec(&mut self, py: Python, dest_buffer: &mut Vec<u8>) -> PyResult<bool> {
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest_buffer.as_mut_ptr() as *mut _,
            size: dest_buffer.capacity(),
            pos: dest_buffer.len(),
        };

        let res = self.compress_into_buffer(py, &mut out_buffer)?;

        unsafe {
            dest_buffer.set_len(out_buffer.pos);
        }

        Ok(res)
    }
}

#[pymethods]
impl ZstdCompressionReader {
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
        if slf.entered {
            Err(PyValueError::new_err("cannot __enter__ multiple times"))
        } else if slf.closed {
            Err(PyValueError::new_err("stream is closed"))
        } else {
            slf.entered = true;
            Ok(slf)
        }
    }

    fn __exit__<'p>(
        mut slf: PyRefMut<'p, Self>,
        py: Python<'p>,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<bool> {
        slf.entered = false;
        slf.close(py)?;

        // TODO release cctx and reader?

        Ok(false)
    }

    fn readable(&self) -> bool {
        true
    }

    fn writable(&self) -> bool {
        false
    }

    fn seekable(&self) -> bool {
        false
    }

    fn readline(&self, py: Python) -> PyResult<()> {
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

    fn write(&self, _data: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyOSError::new_err("stream is not writable"))
    }

    fn writelines(&self, _data: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyOSError::new_err("stream is not writable"))
    }

    fn isatty(&self) -> bool {
        false
    }

    fn flush(&self) -> PyResult<()> {
        Ok(())
    }

    fn close(&mut self, py: Python) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }

        self.closed = true;

        if let Ok(close) = self.source.source_object().getattr(py, "close") {
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

    fn tell(&self) -> usize {
        self.bytes_compressed
    }

    fn readall<'p>(&mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let chunks = PyList::empty(py);

        loop {
            let chunk = self.read(py, 1048576)?;

            if chunk.len()? == 0 {
                break;
            }

            chunks.append(chunk)?;
        }

        let empty = PyBytes::new(py, &[]);

        empty.call_method1("join", (chunks,))
    }

    #[pyo3(signature = (size=-1))]
    fn read<'p>(&mut self, py: Python<'p>, size: isize) -> PyResult<Bound<'p, PyAny>> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        if size < -1 {
            return Err(PyValueError::new_err(
                "cannot read negative amounts less than -1",
            ));
        }

        if size == -1 {
            return self.readall(py);
        }

        if self.finished_output || size == 0 {
            return Ok(PyBytes::new(py, &[]).into_any());
        }

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(size as _);

        while !self.source.finished() {
            // If the output buffer is full, return its content.
            if self.compress_into_vec(py, &mut dest_buffer)? {
                // TODO avoid buffer copy.
                return Ok(PyBytes::new(py, &dest_buffer).into_any());
            }
            // Else continue to read new input into the compressor.
        }

        // EOF.
        let old_pos = dest_buffer.len();

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null_mut(),
            size: 0,
            pos: 0,
        };

        let zresult = self
            .cctx
            .compress_into_vec(
                &mut dest_buffer,
                &mut in_buffer,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|msg| {
                ZstdError::new_err(format!("error ending compression stream: {}", msg))
            })?;

        self.bytes_compressed += dest_buffer.len() - old_pos;

        if zresult == 0 {
            self.finished_output = true;
        }

        // TODO avoid buffer copy.
        Ok(PyBytes::new(py, &dest_buffer).into_any())
    }

    #[pyo3(signature = (size=-1))]
    fn read1<'p>(&mut self, py: Python<'p>, size: isize) -> PyResult<Bound<'p, PyAny>> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        if size < -1 {
            return Err(PyValueError::new_err(
                "cannot read negative amounts less than -1",
            ));
        }

        if self.finished_output || size == 0 {
            return Ok(PyBytes::new(py, &[]).into_any());
        }

        // -1 returns arbitrary number of bytes.
        let size = if size == -1 {
            zstd_safe::CCtx::out_size()
        } else {
            size as _
        };

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(size);

        // read1() dictates that we can perform at most 1 call to the
        // underlying stream to get input. However, we can't satisfy this
        // restriction with compression because not all input generates output.
        // It is possible to perform a block flush in order to ensure output.
        // But this may not be desirable behavior. So we allow multiple read()
        // to the underlying stream. But unlike our read(), we stop once we
        // have any output.

        // Read data until we exhaust input or have output data.
        while !self.source.finished() && dest_buffer.is_empty() {
            self.compress_into_vec(py, &mut dest_buffer)?;
        }

        // We return immediately if:
        // a) output buffer is full
        // b) output buffer has data and input isn't exhausted.
        if dest_buffer.len() == dest_buffer.capacity()
            || (!dest_buffer.is_empty() && !self.source.finished())
        {
            // TODO avoid buffer copy.
            return Ok(PyBytes::new(py, &dest_buffer).into_any());
        }

        // Input must be exhausted. Finish the compression stream.
        let old_pos = dest_buffer.len();

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null_mut(),
            size: 0,
            pos: 0,
        };

        let zresult = self
            .cctx
            .compress_into_vec(
                &mut dest_buffer,
                &mut in_buffer,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|msg| {
                ZstdError::new_err(format!("error ending compression stream: {}", msg))
            })?;

        self.bytes_compressed += dest_buffer.len() - old_pos;

        if zresult == 0 {
            self.finished_output = true;
        }

        // TODO avoid buffer copy
        Ok(PyBytes::new(py, &dest_buffer).into_any())
    }

    fn readinto(&mut self, py: Python, buffer: PyBuffer<u8>) -> PyResult<usize> {
        if buffer.readonly() {
            return Err(PyValueError::new_err("buffer is not writable"));
        }

        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        if self.finished_output {
            return Ok(0);
        }

        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: buffer.buf_ptr(),
            size: buffer.len_bytes(),
            pos: 0,
        };

        while !self.source.finished() {
            if self.compress_into_buffer(py, &mut out_buffer)? {
                return Ok(out_buffer.pos);
            }
        }

        // EOF.
        let old_pos = out_buffer.pos;

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null_mut(),
            size: 0,
            pos: 0,
        };

        let zresult = self
            .cctx
            .compress_buffers(
                &mut out_buffer,
                &mut in_buffer,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|msg| {
                ZstdError::new_err(format!("error ending compression stream: {}", msg))
            })?;

        self.bytes_compressed += out_buffer.pos - old_pos;

        if zresult == 0 {
            self.finished_output = true;
        }

        Ok(out_buffer.pos)
    }

    fn readinto1(&mut self, py: Python, buffer: PyBuffer<u8>) -> PyResult<usize> {
        if buffer.readonly() {
            return Err(PyValueError::new_err("buffer is not writable"));
        }

        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        if self.finished_output {
            return Ok(0);
        }

        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: buffer.buf_ptr(),
            size: buffer.len_bytes(),
            pos: 0,
        };

        // Read until we get output.
        while out_buffer.pos == 0 && !self.source.finished() {
            self.compress_into_buffer(py, &mut out_buffer)?;
        }

        // If we still have input, return immediately.
        if !self.source.finished() {
            return Ok(out_buffer.pos);
        }

        // EOF.
        let old_pos = out_buffer.pos;

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null_mut(),
            size: 0,
            pos: 0,
        };

        let zresult = self
            .cctx
            .compress_buffers(
                &mut out_buffer,
                &mut in_buffer,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|msg| {
                ZstdError::new_err(format!("error ending compression stream: {}", msg))
            })?;

        self.bytes_compressed += out_buffer.pos - old_pos;

        if zresult == 0 {
            self.finished_output = true;
        }

        Ok(out_buffer.pos)
    }
}
