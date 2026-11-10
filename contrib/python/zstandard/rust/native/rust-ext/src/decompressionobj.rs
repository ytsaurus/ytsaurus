// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{exceptions::ZstdError, zstd_safe::DCtx},
    pyo3::{
        buffer::PyBuffer,
        prelude::*,
        types::{PyBytes, PyList},
    },
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdDecompressionObj {
    dctx: Arc<DCtx<'static>>,
    write_size: usize,
    read_across_frames: bool,
    finished: bool,
    unused_data: Vec<u8>,
}

unsafe impl Sync for ZstdDecompressionObj {}

impl ZstdDecompressionObj {
    pub fn new(
        dctx: Arc<DCtx<'static>>,
        write_size: usize,
        read_across_frames: bool,
    ) -> PyResult<Self> {
        Ok(ZstdDecompressionObj {
            dctx,
            write_size,
            read_across_frames,
            finished: false,
            unused_data: vec![],
        })
    }
}

#[pymethods]
impl ZstdDecompressionObj {
    fn decompress<'p>(&mut self, py: Python<'p>, data: PyBuffer<u8>) -> PyResult<Bound<'p, PyAny>> {
        if self.finished {
            return Err(ZstdError::new_err(
                "cannot use a decompressobj multiple times",
            ));
        }

        if data.len_bytes() == 0 {
            return Ok(PyBytes::new(py, &[]).into_any());
        }

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: data.buf_ptr(),
            size: data.len_bytes(),
            pos: 0,
        };

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(self.write_size);

        let chunks = PyList::empty(py);

        loop {
            let zresult = self
                .dctx
                .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
                .map_err(|msg| ZstdError::new_err(format!("zstd decompress error: {}", msg)))?;

            if !dest_buffer.is_empty() {
                // TODO avoid buffer copy.
                let chunk = PyBytes::new(py, &dest_buffer);
                chunks.append(chunk)?;
            }

            if zresult == 0 && !self.read_across_frames {
                self.finished = true;
                // TODO clear out decompressor?

                if let Some(data) = data.as_slice(py) {
                    let unused = &data[in_buffer.pos..in_buffer.size];
                    self.unused_data = unused.iter().map(|x| x.get()).collect::<Vec<_>>();
                }

                break;
            } else if zresult == 0 && self.read_across_frames {
                if in_buffer.pos == in_buffer.size {
                    break;
                } else {
                    dest_buffer.clear();
                }
            } else if in_buffer.pos == in_buffer.size && dest_buffer.len() < dest_buffer.capacity()
            {
                break;
            } else {
                dest_buffer.clear();
            }
        }

        let empty = PyBytes::new(py, &[]);
        empty.call_method1("join", (chunks,))
    }

    #[pyo3(signature = (length=None))]
    #[allow(unused_variables)]
    fn flush<'p>(&self, py: Python<'p>, length: Option<usize>) -> PyResult<Bound<'p, PyBytes>> {
        Ok(PyBytes::new(py, &[]))
    }

    #[getter]
    fn unused_data<'p>(&self, py: Python<'p>) -> Bound<'p, PyBytes> {
        PyBytes::new(py, &self.unused_data)
    }

    #[getter]
    fn unconsumed_tail<'p>(&self, py: Python<'p>) -> Bound<'p, PyBytes> {
        PyBytes::new(py, &[])
    }

    #[getter]
    fn eof(&self) -> bool {
        self.finished
    }
}
