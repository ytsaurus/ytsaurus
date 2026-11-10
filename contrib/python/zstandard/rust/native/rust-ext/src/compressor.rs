// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        buffers::ZstdBufferWithSegmentsCollection,
        compression_chunker::ZstdCompressionChunker,
        compression_dict::ZstdCompressionDict,
        compression_parameters::{CCtxParams, ZstdCompressionParameters},
        compression_reader::ZstdCompressionReader,
        compression_writer::ZstdCompressionWriter,
        compressionobj::ZstdCompressionObj,
        compressor_iterator::ZstdCompressorIterator,
        compressor_multi::multi_compress_to_buffer,
        zstd_safe::CCtx,
        ZstdError,
    },
    pyo3::{buffer::PyBuffer, exceptions::PyValueError, prelude::*, types::PyBytes},
    std::sync::Arc,
};

#[pyclass(module = "zstandard.backend_rust")]
struct ZstdCompressor {
    _threads: i32,
    dict: Option<Py<ZstdCompressionDict>>,
    params: CCtxParams<'static>,
    cctx: Arc<CCtx<'static>>,
}

unsafe impl Sync for ZstdCompressor {}

impl ZstdCompressor {
    pub(crate) fn setup_cctx(&self, py: Python) -> PyResult<()> {
        self.cctx
            .set_parameters(&self.params)
            .or_else(|msg| Err(ZstdError::new_err(msg)))?;

        if let Some(dict) = &self.dict {
            dict.borrow(py).load_into_cctx(&self.cctx)?;
        }

        Ok(())
    }
}

#[pymethods]
impl ZstdCompressor {
    #[new]
    #[pyo3(signature = (
        level=3,
        dict_data=None,
        compression_params=None,
        write_checksum=None,
        write_content_size=None,
        write_dict_id=None,
        threads=0,
    ))]
    fn new(
        py: Python,
        level: i32,
        dict_data: Option<Py<ZstdCompressionDict>>,
        compression_params: Option<Py<ZstdCompressionParameters>>,
        write_checksum: Option<bool>,
        write_content_size: Option<bool>,
        write_dict_id: Option<bool>,
        threads: i32,
    ) -> PyResult<Self> {
        if level > zstd_safe::max_c_level() {
            return Err(PyValueError::new_err(format!(
                "level must be less than {}",
                zstd_safe::max_c_level() as i32 + 1
            )));
        }

        let threads = if threads < 0 {
            num_cpus::get() as i32
        } else {
            threads
        };

        let cctx = Arc::new(CCtx::new().or_else(|msg| Err(PyErr::new::<ZstdError, _>(msg)))?);
        let params = CCtxParams::create()?;

        if let Some(compression_params) = &compression_params {
            if write_checksum.is_some() {
                return Err(PyValueError::new_err(
                    "cannot define compression_params and write_checksum",
                ));
            }
            if write_content_size.is_some() {
                return Err(PyValueError::new_err(
                    "cannot define compression_params and write_content_size",
                ));
            }
            if write_dict_id.is_some() {
                return Err(PyValueError::new_err(
                    "cannot define compression_params and write_dict_id",
                ));
            }
            if threads != 0 {
                return Err(PyValueError::new_err(
                    "cannot define compression_params and threads",
                ));
            }

            params.apply_compression_parameters(py, compression_params)?;

        // TODO set parameters from CompressionParameters
        } else {
            params.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel, level)?;
            params.set_parameter(
                zstd_sys::ZSTD_cParameter::ZSTD_c_contentSizeFlag,
                if write_content_size.unwrap_or(true) {
                    1
                } else {
                    0
                },
            )?;
            params.set_parameter(
                zstd_sys::ZSTD_cParameter::ZSTD_c_checksumFlag,
                if write_checksum.unwrap_or(false) {
                    1
                } else {
                    0
                },
            )?;
            params.set_parameter(
                zstd_sys::ZSTD_cParameter::ZSTD_c_dictIDFlag,
                if write_dict_id.unwrap_or(true) { 1 } else { 0 },
            )?;
            if threads != 0 {
                params.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_nbWorkers, threads)?;
            }
        }

        let compressor = ZstdCompressor {
            _threads: threads,
            dict: dict_data,
            params,
            cctx,
        };

        compressor.setup_cctx(py)?;

        Ok(compressor)
    }

    fn memory_size(&self) -> PyResult<usize> {
        Ok(self.cctx.memory_size())
    }

    fn frame_progression(&self) -> PyResult<(usize, usize, usize)> {
        let progression = self.cctx.get_frame_progression();

        Ok((
            progression.ingested as usize,
            progression.consumed as usize,
            progression.produced as usize,
        ))
    }

    fn compress<'p>(&self, py: Python<'p>, buffer: PyBuffer<u8>) -> PyResult<Bound<'p, PyBytes>> {
        let source: &[u8] =
            unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const _, buffer.len_bytes()) };

        let cctx = &self.cctx;

        // TODO implement 0 copy via Py_SIZE().
        let data = py
            .allow_threads(|| cctx.compress(source))
            .or_else(|msg| Err(ZstdError::new_err(format!("cannot compress: {}", msg))))?;

        Ok(PyBytes::new(py, &data))
    }

    #[pyo3(signature = (size=None, chunk_size=None))]
    fn chunker(
        &self,
        size: Option<u64>,
        chunk_size: Option<usize>,
    ) -> PyResult<ZstdCompressionChunker> {
        self.cctx.reset();

        let size = size.unwrap_or(zstd_safe::CONTENTSIZE_UNKNOWN);
        let chunk_size = chunk_size.unwrap_or_else(|| zstd_safe::CCtx::out_size());

        self.cctx.set_pledged_source_size(size).or_else(|msg| {
            Err(ZstdError::new_err(format!(
                "error setting source size: {}",
                msg
            )))
        })?;

        ZstdCompressionChunker::new(self.cctx.clone(), chunk_size)
    }

    #[pyo3(signature = (size=None))]
    fn compressobj(&self, size: Option<u64>) -> PyResult<ZstdCompressionObj> {
        self.cctx.reset();

        let size = if let Some(size) = size {
            size
        } else {
            zstd_safe::CONTENTSIZE_UNKNOWN
        };

        self.cctx.set_pledged_source_size(size).or_else(|msg| {
            Err(ZstdError::new_err(format!(
                "error setting source size: {}",
                msg
            )))
        })?;

        ZstdCompressionObj::new(self.cctx.clone())
    }

    #[pyo3(signature = (ifh, ofh, size=None, read_size=None, write_size=None))]
    fn copy_stream(
        &self,
        py: Python,
        ifh: &Bound<'_, PyAny>,
        ofh: &Bound<'_, PyAny>,
        size: Option<u64>,
        read_size: Option<usize>,
        write_size: Option<usize>,
    ) -> PyResult<(usize, usize)> {
        let source_size = if let Some(source_size) = size {
            source_size
        } else {
            zstd_safe::CONTENTSIZE_UNKNOWN
        };

        let read_size = read_size.unwrap_or_else(|| zstd_safe::CCtx::in_size());
        let write_size = write_size.unwrap_or_else(|| zstd_safe::CCtx::in_size());

        if !ifh.hasattr("read")? {
            return Err(PyValueError::new_err(
                "first argument must have a read() method",
            ));
        }
        if !ofh.hasattr("write")? {
            return Err(PyValueError::new_err(
                "second argument must have a write() method",
            ));
        }

        self.cctx.reset();
        self.cctx
            .set_pledged_source_size(source_size)
            .or_else(|msg| {
                Err(ZstdError::new_err(format!(
                    "error setting source size: {}",
                    msg
                )))
            })?;

        let mut total_read = 0;
        let mut total_write = 0;

        loop {
            // Try to read from source stream.
            let read_object = ifh.call_method("read", (read_size,), None)?;

            let read_bytes = read_object.downcast::<PyBytes>()?;
            let read_data = read_bytes.as_bytes();

            // If no data was read we are at EOF.
            if read_data.len() == 0 {
                break;
            }

            total_read += read_data.len();

            // Send data to compressor.

            let mut source = read_data;
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
                    .or_else(|msg| {
                        Err(ZstdError::new_err(format!("zstd compress error: {}", msg)))
                    })?;

                source = result.1;

                let chunk = &result.0;

                if !chunk.is_empty() {
                    // TODO avoid buffer copy.
                    let data = PyBytes::new(py, chunk);
                    ofh.call_method("write", (data,), None)?;
                    total_write += chunk.len();
                }
            }
        }

        // We've finished reading. Now flush the compressor stream.
        loop {
            let result = self
                .cctx
                .compress_chunk(&[], zstd_sys::ZSTD_EndDirective::ZSTD_e_end, write_size)
                .or_else(|msg| {
                    Err(ZstdError::new_err(format!(
                        "error ending compression stream: {}",
                        msg
                    )))
                })?;

            let chunk = &result.0;

            if !chunk.is_empty() {
                // TODO avoid buffer copy.
                let data = PyBytes::new(py, &chunk);
                ofh.call_method("write", (data,), None)?;
                total_write += chunk.len();
            }

            if !result.2 {
                break;
            }
        }

        Ok((total_read, total_write))
    }

    #[pyo3(signature = (data, threads=0))]
    fn multi_compress_to_buffer(
        &self,
        py: Python,
        data: &Bound<'_, PyAny>,
        threads: isize,
    ) -> PyResult<ZstdBufferWithSegmentsCollection> {
        multi_compress_to_buffer(py, &self.params, &self.dict, data, threads)
    }

    #[pyo3(signature = (reader, size=None, read_size=None, write_size=None))]
    fn read_to_iter(
        &self,
        py: Python,
        reader: &Bound<'_, PyAny>,
        size: Option<u64>,
        read_size: Option<usize>,
        write_size: Option<usize>,
    ) -> PyResult<ZstdCompressorIterator> {
        let size = size.unwrap_or(zstd_safe::CONTENTSIZE_UNKNOWN);
        let read_size = read_size.unwrap_or_else(|| zstd_safe::CCtx::in_size());
        let write_size = write_size.unwrap_or_else(|| zstd_safe::CCtx::in_size());

        self.cctx.reset();

        ZstdCompressorIterator::new(py, self.cctx.clone(), reader, size, read_size, write_size)
    }

    #[pyo3(signature = (source, size=None, read_size=None, closefd=true))]
    fn stream_reader(
        &self,
        py: Python,
        source: &Bound<'_, PyAny>,
        size: Option<u64>,
        read_size: Option<usize>,
        closefd: bool,
    ) -> PyResult<ZstdCompressionReader> {
        let size = size.unwrap_or(zstd_safe::CONTENTSIZE_UNKNOWN);
        let read_size = read_size.unwrap_or_else(|| zstd_safe::CCtx::in_size());

        self.cctx.reset();

        ZstdCompressionReader::new(py, self.cctx.clone(), source, size, read_size, closefd)
    }

    #[pyo3(signature = (writer, size=None, write_size=None, write_return_read=true, closefd=true))]
    fn stream_writer(
        &self,
        py: Python,
        writer: &Bound<'_, PyAny>,
        size: Option<u64>,
        write_size: Option<usize>,
        write_return_read: bool,
        closefd: bool,
    ) -> PyResult<ZstdCompressionWriter> {
        if !writer.hasattr("write")? {
            return Err(PyValueError::new_err(
                "must pass object with a write() method",
            ));
        }

        self.cctx.reset();

        let size = size.unwrap_or(zstd_sys::ZSTD_CONTENTSIZE_UNKNOWN as _);
        let write_size = write_size.unwrap_or_else(|| unsafe { zstd_sys::ZSTD_CStreamOutSize() });

        ZstdCompressionWriter::new(
            py,
            self.cctx.clone(),
            writer,
            size,
            write_size,
            write_return_read,
            closefd,
        )
    }
}

pub(crate) fn init_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ZstdCompressor>()?;

    Ok(())
}
