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
    pyo3::{
        buffer::PyBuffer,
        exceptions::{PyOSError, PyValueError},
        prelude::*,
        types::{PyBytes, PyList},
    },
    std::{cmp::min, sync::Arc},
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdDecompressionReader {
    dctx: Arc<DCtx<'static>>,
    source: Box<dyn InBufferSource + Send>,
    read_across_frames: bool,
    closefd: bool,
    entered: bool,
    closed: bool,
    bytes_decompressed: usize,
    finished_output: bool,
}

unsafe impl Sync for ZstdDecompressionReader {}

impl ZstdDecompressionReader {
    pub fn new(
        py: Python,
        dctx: Arc<DCtx<'static>>,
        reader: &Bound<'_, PyAny>,
        read_size: usize,
        read_across_frames: bool,
        closefd: bool,
    ) -> PyResult<Self> {
        Ok(Self {
            dctx,
            source: make_in_buffer_source(py, reader, read_size)?,
            read_across_frames,
            closefd,
            entered: false,
            closed: false,
            bytes_decompressed: 0,
            finished_output: false,
        })
    }
}

impl ZstdDecompressionReader {
    fn decompress_into_buffer(
        &mut self,
        py: Python,
        out_buffer: &mut zstd_sys::ZSTD_outBuffer,
    ) -> PyResult<bool> {
        let mut in_buffer =
            self.source
                .input_buffer(py)?
                .unwrap_or_else(|| zstd_sys::ZSTD_inBuffer {
                    src: std::ptr::null_mut(),
                    size: 0,
                    pos: 0,
                });

        let old_pos = in_buffer.pos;

        let zresult = self
            .dctx
            .decompress_buffers(out_buffer, &mut in_buffer)
            .map_err(|msg| ZstdError::new_err(format!("zstd decompress error: {}", msg)))?;

        if in_buffer.pos - old_pos > 0 {
            self.source.record_bytes_read(in_buffer.pos - old_pos);
        }

        // Emit data if there is data AND either:
        // a) output buffer is full (read amount is satisfied)
        // b) we're at the end of a frame and not in frame spanning mode
        return Ok(out_buffer.pos != 0
            && (out_buffer.pos == out_buffer.size || zresult == 0 && !self.read_across_frames));
    }
}

#[pymethods]
impl ZstdDecompressionReader {
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

    #[allow(unused_variables)]
    fn __exit__<'p>(
        mut slf: PyRefMut<'p, Self>,
        py: Python<'p>,
        exc_type: &Bound<'_, PyAny>,
        exc_value: &Bound<'_, PyAny>,
        exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        slf.entered = false;
        // TODO release decompressor and source?
        slf.close(py)?;

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

    #[pyo3(signature = (data))]
    #[allow(unused_variables)]
    fn write(&self, py: Python, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
    }

    #[pyo3(signature = (lines))]
    #[allow(unused_variables)]
    fn writelines(&self, py: Python, lines: &Bound<'_, PyAny>) -> PyResult<()> {
        let io = py.import("io")?;
        let exc = io.getattr("UnsupportedOperation")?;

        Err(PyErr::from_value(exc))
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
        self.bytes_decompressed
    }

    fn readall<'p>(&mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let chunks = PyList::empty(py);

        loop {
            let chunk = self.read(py, Some(1048576))?;
            if chunk.len()? == 0 {
                break;
            }

            chunks.append(chunk)?;
        }

        let empty = PyBytes::new(py, &[]);

        empty.call_method1("join", (chunks,))
    }

    #[pyo3(signature = (size=None))]
    fn read<'p>(&mut self, py: Python<'p>, size: Option<isize>) -> PyResult<Bound<'p, PyAny>> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        let size = size.unwrap_or(-1);

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
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest_buffer.as_mut_ptr() as *mut _,
            size: dest_buffer.capacity(),
            pos: 0,
        };

        if self.decompress_into_buffer(py, &mut out_buffer)? {
            self.bytes_decompressed += out_buffer.pos;
            unsafe {
                dest_buffer.set_len(out_buffer.pos);
            }

            // TODO avoid buffer copy.
            let chunk = PyBytes::new(py, &dest_buffer);
            return Ok(chunk.into_any());
        }

        while !self.source.finished() {
            if self.decompress_into_buffer(py, &mut out_buffer)? {
                self.bytes_decompressed += out_buffer.pos;
                unsafe {
                    dest_buffer.set_len(out_buffer.pos);
                }

                // TODO avoid buffer copy.
                let chunk = PyBytes::new(py, &dest_buffer);
                return Ok(chunk.into_any());
            }
        }

        self.bytes_decompressed += out_buffer.pos;
        unsafe {
            dest_buffer.set_len(out_buffer.pos);
        }

        // TODO avoid buffer copy.
        let chunk = PyBytes::new(py, &dest_buffer);
        return Ok(chunk.into_any());
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
            dst: buffer.buf_ptr() as *mut _,
            size: buffer.len_bytes(),
            pos: 0,
        };

        if self.decompress_into_buffer(py, &mut out_buffer)? {
            self.bytes_decompressed += out_buffer.pos;

            return Ok(out_buffer.pos);
        }

        while !self.source.finished() {
            if self.decompress_into_buffer(py, &mut out_buffer)? {
                self.bytes_decompressed += out_buffer.pos;

                return Ok(out_buffer.pos);
            }
        }

        self.bytes_decompressed += out_buffer.pos;

        Ok(out_buffer.pos)
    }

    #[pyo3(signature = (size=None))]
    fn read1<'p>(&mut self, py: Python<'p>, size: Option<isize>) -> PyResult<Bound<'p, PyAny>> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        let size = size.unwrap_or(-1);

        if size < -1 {
            return Err(PyValueError::new_err(
                "cannot read negative amounts less than -1",
            ));
        }

        if self.finished_output || size == 0 {
            return Ok(PyBytes::new(py, &[]).into_any());
        }

        // -1 returns arbitrary number of bytes.
        let size = match size {
            -1 => zstd_safe::DCtx::out_size(),
            size => size as _,
        };

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(size);
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest_buffer.as_mut_ptr() as *mut _,
            size: dest_buffer.capacity(),
            pos: 0,
        };

        // read1() dictates that we can perform at most 1 call to underlying
        // stream to get input. However, we can't satisfy this restriction with
        // decompression because not all input generates output. So we allow
        // multiple read(). But unlike read(), we stop once we have any output.
        while !self.source.finished() {
            self.decompress_into_buffer(py, &mut out_buffer)?;

            if out_buffer.pos > 0 {
                break;
            }
        }

        unsafe {
            dest_buffer.set_len(out_buffer.pos);
        }
        self.bytes_decompressed += out_buffer.pos;

        // TODO avoid buffer copy.
        let chunk = PyBytes::new(py, &dest_buffer);
        Ok(chunk.into_any())
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

        while !self.source.finished() && !self.finished_output {
            self.decompress_into_buffer(py, &mut out_buffer)?;

            if out_buffer.pos > 0 {
                break;
            }
        }

        self.bytes_decompressed += out_buffer.pos;

        Ok(out_buffer.pos)
    }

    #[pyo3(signature = (pos, whence=None))]
    fn seek(&mut self, py: Python, pos: isize, whence: Option<i32>) -> PyResult<usize> {
        if self.closed {
            return Err(PyValueError::new_err("stream is closed"));
        }

        let os = py.import("os")?;

        let seek_set = os.getattr("SEEK_SET")?.extract::<i32>()?;
        let seek_cur = os.getattr("SEEK_CUR")?.extract::<i32>()?;
        let seek_end = os.getattr("SEEK_END")?.extract::<i32>()?;

        let whence = whence.unwrap_or(seek_set);

        let mut read_amount = if whence == seek_set {
            if pos < 0 {
                return Err(PyOSError::new_err(
                    "cannot seek to negative position with SEEK_SET",
                ));
            }

            if pos < self.bytes_decompressed as isize {
                return Err(PyOSError::new_err(
                    "cannot seek zstd decompression stream backwards",
                ));
            }

            pos as usize - self.bytes_decompressed
        } else if whence == seek_cur {
            if pos < 0 {
                return Err(PyOSError::new_err(
                    "cannot seek zstd decompression stream backwards",
                ));
            }

            pos as usize
        } else if whence == seek_end {
            return Err(PyOSError::new_err(
                "zstd decompression streams cannot be seeked with SEEK_END",
            ));
        } else {
            0
        };

        while read_amount > 0 {
            let result = self.read(py, Some(min(read_amount, zstd_safe::DCtx::out_size()) as _))?;

            if result.len()? == 0 {
                break;
            }

            read_amount -= result.len()?;
        }

        Ok(self.bytes_decompressed)
    }
}
