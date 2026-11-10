// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        buffers::ZstdBufferWithSegmentsCollection, compression_dict::ZstdCompressionDict,
        decompression_reader::ZstdDecompressionReader,
        decompression_writer::ZstdDecompressionWriter, decompressionobj::ZstdDecompressionObj,
        decompressor_iterator::ZstdDecompressorIterator,
        decompressor_multi::multi_decompress_to_buffer, exceptions::ZstdError, zstd_safe::DCtx,
    },
    pyo3::{
        buffer::PyBuffer,
        exceptions::{PyMemoryError, PyValueError},
        prelude::*,
        types::{PyBytes, PyList},
        wrap_pyfunction,
    },
    std::{ffi::c_ulonglong, sync::Arc},
};

#[pyclass(module = "zstandard.backend_rust")]
struct ZstdDecompressor {
    dict_data: Option<Py<ZstdCompressionDict>>,
    max_window_size: usize,
    format: zstd_sys::ZSTD_format_e,
    dctx: Arc<DCtx<'static>>,
}

unsafe impl Sync for ZstdDecompressor {}

impl ZstdDecompressor {
    fn setup_dctx(&self, py: Python, load_dict: bool) -> PyResult<()> {
        self.dctx.reset().map_err(|msg| {
            ZstdError::new_err(format!("unable to reset decompression context: {}", msg))
        })?;

        if self.max_window_size != 0 {
            self.dctx
                .set_max_window_size(self.max_window_size)
                .map_err(|msg| {
                    ZstdError::new_err(format!("unable to set max window size: {}", msg))
                })?;
        }

        self.dctx
            .set_format(self.format)
            .map_err(|msg| ZstdError::new_err(format!("unable to set decoding format: {}", msg)))?;

        if let Some(dict_data) = &self.dict_data {
            if load_dict {
                dict_data.try_borrow_mut(py)?.load_into_dctx(&self.dctx)?;
            }
        }

        Ok(())
    }
}

#[pymethods]
impl ZstdDecompressor {
    #[new]
    #[pyo3(signature = (dict_data=None, max_window_size=0, format=0))]
    fn new(
        dict_data: Option<Py<ZstdCompressionDict>>,
        max_window_size: usize,
        format: u32,
    ) -> PyResult<Self> {
        let format = if format == zstd_sys::ZSTD_format_e::ZSTD_f_zstd1 as u32 {
            zstd_sys::ZSTD_format_e::ZSTD_f_zstd1
        } else if format == zstd_sys::ZSTD_format_e::ZSTD_f_zstd1_magicless as u32 {
            zstd_sys::ZSTD_format_e::ZSTD_f_zstd1_magicless
        } else {
            return Err(PyValueError::new_err(format!("invalid format value")));
        };

        let dctx = Arc::new(DCtx::new().map_err(|_| PyMemoryError::new_err(()))?);

        Ok(Self {
            dict_data,
            max_window_size,
            format,
            dctx,
        })
    }

    #[pyo3(signature = (ifh, ofh, read_size=None, write_size=None))]
    fn copy_stream(
        &self,
        py: Python,
        ifh: &Bound<'_, PyAny>,
        ofh: &Bound<'_, PyAny>,
        read_size: Option<usize>,
        write_size: Option<usize>,
    ) -> PyResult<(usize, usize)> {
        let read_size = read_size.unwrap_or_else(|| zstd_safe::DCtx::in_size());
        let write_size = write_size.unwrap_or_else(|| zstd_safe::DCtx::out_size());

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

        self.setup_dctx(py, true)?;

        let mut dest_buffer: Vec<u8> = Vec::with_capacity(write_size);

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: std::ptr::null(),
            size: 0,
            pos: 0,
        };

        let mut total_read = 0;
        let mut total_write = 0;

        // Read all available input.
        loop {
            let read_object = ifh.call_method1("read", (read_size,))?;
            let read_bytes = read_object.downcast::<PyBytes>()?;
            let read_data = read_bytes.as_bytes();

            if read_data.len() == 0 {
                break;
            }

            total_read += read_data.len();

            in_buffer.src = read_data.as_ptr() as *const _;
            in_buffer.size = read_data.len();
            in_buffer.pos = 0;

            // Flush all read data to output.
            while in_buffer.pos < in_buffer.size {
                self.dctx
                    .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
                    .map_err(|msg| ZstdError::new_err(format!("zstd decompress error: {}", msg)))?;

                if !dest_buffer.is_empty() {
                    // TODO avoid buffer copy.
                    let data = PyBytes::new(py, &dest_buffer);

                    ofh.call_method1("write", (data,))?;
                    total_write += dest_buffer.len();
                    dest_buffer.clear();
                }
            }
            // Continue loop to keep reading.
        }

        Ok((total_read, total_write))
    }

    #[pyo3(signature = (buffer, max_output_size=0, read_across_frames=false, allow_extra_data=true))]
    fn decompress<'p>(
        &mut self,
        py: Python<'p>,
        buffer: PyBuffer<u8>,
        max_output_size: usize,
        read_across_frames: bool,
        allow_extra_data: bool,
    ) -> PyResult<Bound<'p, PyBytes>> {
        if read_across_frames {
            return Err(ZstdError::new_err(
                "ZstdDecompressor.read_across_frames=True is not yet implemented",
            ));
        }

        self.setup_dctx(py, true)?;

        let mut header: zstd_sys::ZSTD_FrameHeader = unsafe { std::mem::zeroed() };
        let zresult = unsafe {
            zstd_sys::ZSTD_getFrameHeader_advanced(
                &mut header,
                buffer.buf_ptr(),
                buffer.len_bytes(),
                self.format,
            )
        };

        if zresult != 0 {
            return Err(ZstdError::new_err(
                "error determining content size from frame header",
            ));
        }

        let (output_buffer_size, output_size) = if header.frameContentSize == 0 {
            return Ok(PyBytes::new(py, &[]));
        } else if header.frameContentSize == zstd_sys::ZSTD_CONTENTSIZE_UNKNOWN as c_ulonglong {
            if max_output_size == 0 {
                return Err(ZstdError::new_err(
                    "could not determine content size in frame header",
                ));
            }

            (max_output_size, 0)
        } else {
            (header.frameContentSize as _, header.frameContentSize)
        };

        let mut dest_buffer: Vec<u8> = Vec::new();
        dest_buffer
            .try_reserve_exact(output_buffer_size)
            .map_err(|_| PyMemoryError::new_err(()))?;

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: buffer.buf_ptr(),
            size: buffer.len_bytes(),
            pos: 0,
        };

        let zresult = self
            .dctx
            .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
            .map_err(|msg| ZstdError::new_err(format!("decompression error: {}", msg)))?;

        if zresult != 0 {
            Err(ZstdError::new_err(
                "decompression error: did not decompress full frame",
            ))
        } else if output_size != 0 && dest_buffer.len() != output_size as usize {
            Err(ZstdError::new_err(format!(
                "decompression error: decompressed {} bytes; expected {}",
                zresult, output_size
            )))
        } else if !allow_extra_data && in_buffer.pos < in_buffer.size {
            Err(ZstdError::new_err(format!(
                "compressed input contains {} bytes of unused data, which is disallowed",
                in_buffer.size - in_buffer.pos
            )))
        } else {
            // TODO avoid memory copy
            Ok(PyBytes::new(py, &dest_buffer))
        }
    }

    fn decompress_content_dict_chain<'p>(
        &self,
        py: Python<'p>,
        frames: &Bound<'_, PyList>,
    ) -> PyResult<Bound<'p, PyBytes>> {
        if frames.is_empty() {
            return Err(PyValueError::new_err("empty input chain"));
        }

        // First chunk should not be using a dictionary. We handle it specially.
        let chunk = frames.get_item(0)?;

        if !chunk.is_instance_of::<PyBytes>() {
            return Err(PyValueError::new_err("chunk 0 must be bytes"));
        }

        let chunk_buffer: PyBuffer<u8> = PyBuffer::get(&chunk.as_borrowed())?;
        let mut params = zstd_sys::ZSTD_FrameHeader {
            frameContentSize: 0,
            windowSize: 0,
            blockSizeMax: 0,
            frameType: zstd_sys::ZSTD_FrameType_e::ZSTD_frame,
            headerSize: 0,
            dictID: 0,
            checksumFlag: 0,
            _reserved1: 0,
            _reserved2: 0,
        };
        let zresult = unsafe {
            zstd_sys::ZSTD_getFrameHeader(
                &mut params,
                chunk_buffer.buf_ptr() as *const _,
                chunk_buffer.len_bytes(),
            )
        };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            return Err(PyValueError::new_err("chunk 0 is not a valid zstd frame"));
        } else if zresult != 0 {
            return Err(PyValueError::new_err(
                "chunk 0 is too small to contain a zstd frame",
            ));
        }

        if params.frameContentSize == zstd_safe::CONTENTSIZE_UNKNOWN {
            return Err(PyValueError::new_err(
                "chunk 0 missing content size in frame",
            ));
        }

        self.setup_dctx(py, false)?;

        let mut last_buffer: Vec<u8> = Vec::with_capacity(params.frameContentSize as _);

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: chunk_buffer.buf_ptr() as *mut _,
            size: chunk_buffer.len_bytes(),
            pos: 0,
        };

        let zresult = self
            .dctx
            .decompress_into_vec(&mut last_buffer, &mut in_buffer)
            .map_err(|msg| ZstdError::new_err(format!("could not decompress chunk 0: {}", msg)))?;

        if zresult != 0 {
            return Err(ZstdError::new_err("chunk 0 did not decompress full frame"));
        }

        // Special case of chain length 1.
        if frames.len() == 1 {
            // TODO avoid buffer copy.
            let chunk = PyBytes::new(py, &last_buffer);
            return Ok(chunk);
        }

        for (i, chunk) in frames.iter().enumerate().skip(1) {
            if !chunk.is_instance_of::<PyBytes>() {
                return Err(PyValueError::new_err(format!("chunk {} must be bytes", i)));
            }

            let chunk_buffer: PyBuffer<u8> = PyBuffer::get(&chunk.as_borrowed())?;

            let zresult = unsafe {
                zstd_sys::ZSTD_getFrameHeader(
                    &mut params as *mut _,
                    chunk_buffer.buf_ptr(),
                    chunk_buffer.len_bytes(),
                )
            };
            if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
                return Err(PyValueError::new_err(format!(
                    "chunk {} is not a valid zstd frame",
                    i
                )));
            } else if zresult != 0 {
                return Err(PyValueError::new_err(format!(
                    "chunk {} is too small to contain a zstd frame",
                    i
                )));
            }

            if params.frameContentSize == zstd_safe::CONTENTSIZE_UNKNOWN {
                return Err(PyValueError::new_err(format!(
                    "chunk {} missing content size in frame",
                    i
                )));
            }

            let mut dest_buffer: Vec<u8> = Vec::with_capacity(params.frameContentSize as _);

            let mut in_buffer = zstd_sys::ZSTD_inBuffer {
                src: chunk_buffer.buf_ptr(),
                size: chunk_buffer.len_bytes(),
                pos: 0,
            };

            let zresult = self
                .dctx
                .decompress_into_vec(&mut dest_buffer, &mut in_buffer)
                .map_err(|msg| {
                    ZstdError::new_err(format!("could not decompress chunk {}: {}", i, msg))
                })?;

            if zresult != 0 {
                return Err(ZstdError::new_err(format!(
                    "chunk {} did not decompress full frame",
                    i
                )));
            }

            last_buffer = dest_buffer;
        }

        // TODO avoid buffer copy.
        Ok(PyBytes::new(py, &last_buffer))
    }

    #[pyo3(signature = (write_size=None, read_across_frames=false))]
    fn decompressobj(
        &self,
        py: Python,
        write_size: Option<usize>,
        read_across_frames: bool,
    ) -> PyResult<ZstdDecompressionObj> {
        if let Some(write_size) = write_size {
            if write_size < 1 {
                return Err(PyValueError::new_err("write_size must be positive"));
            }
        }

        let write_size = write_size.unwrap_or_else(|| zstd_safe::DCtx::out_size());

        self.setup_dctx(py, true)?;

        ZstdDecompressionObj::new(self.dctx.clone(), write_size, read_across_frames)
    }

    fn memory_size(&self) -> usize {
        self.dctx.memory_size()
    }

    #[pyo3(signature = (frames, decompressed_sizes=None, threads=0))]
    #[allow(unused_variables)]
    fn multi_decompress_to_buffer(
        &self,
        py: Python,
        frames: &Bound<'_, PyAny>,
        decompressed_sizes: Option<&Bound<'_, PyAny>>,
        threads: isize,
    ) -> PyResult<ZstdBufferWithSegmentsCollection> {
        self.setup_dctx(py, true)?;

        multi_decompress_to_buffer(
            py,
            self.dict_data.as_ref(),
            frames,
            decompressed_sizes,
            threads,
        )
    }

    #[pyo3(signature = (reader, read_size=None, write_size=None, skip_bytes=None))]
    fn read_to_iter(
        &self,
        py: Python,
        reader: &Bound<'_, PyAny>,
        read_size: Option<usize>,
        write_size: Option<usize>,
        skip_bytes: Option<usize>,
    ) -> PyResult<ZstdDecompressorIterator> {
        let read_size = read_size.unwrap_or_else(|| zstd_safe::DCtx::in_size());
        let write_size = write_size.unwrap_or_else(|| zstd_safe::DCtx::out_size());
        let skip_bytes = skip_bytes.unwrap_or(0);

        if skip_bytes >= read_size {
            return Err(PyValueError::new_err(
                "skip_bytes must be smaller than read_size",
            ));
        }

        if !reader.hasattr("read")? && !reader.hasattr("__getitem__")? {
            return Err(PyValueError::new_err(
                "must pass an object with a read() method or conforms to buffer protocol",
            ));
        }

        self.setup_dctx(py, true)?;

        ZstdDecompressorIterator::new(
            py,
            self.dctx.clone(),
            reader,
            read_size,
            write_size,
            skip_bytes,
        )
    }

    #[pyo3(signature = (source, read_size=None, read_across_frames=false, closefd=true))]
    fn stream_reader(
        &self,
        py: Python,
        source: &Bound<'_, PyAny>,
        read_size: Option<usize>,
        read_across_frames: bool,
        closefd: bool,
    ) -> PyResult<ZstdDecompressionReader> {
        let read_size = read_size.unwrap_or_else(|| zstd_safe::DCtx::in_size());

        self.setup_dctx(py, true)?;

        ZstdDecompressionReader::new(
            py,
            self.dctx.clone(),
            source,
            read_size,
            read_across_frames,
            closefd,
        )
    }

    #[pyo3(signature = (writer, write_size=None, write_return_read=true, closefd=true))]
    fn stream_writer(
        &self,
        py: Python,
        writer: &Bound<'_, PyAny>,
        write_size: Option<usize>,
        write_return_read: bool,
        closefd: bool,
    ) -> PyResult<ZstdDecompressionWriter> {
        let write_size = write_size.unwrap_or_else(|| zstd_safe::DCtx::out_size());

        self.setup_dctx(py, true)?;

        ZstdDecompressionWriter::new(
            py,
            self.dctx.clone(),
            writer,
            write_size,
            write_return_read,
            closefd,
        )
    }
}

#[pyfunction]
fn estimate_decompression_context_size() -> usize {
    unsafe { zstd_sys::ZSTD_estimateDCtxSize() }
}

pub(crate) fn init_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ZstdDecompressor>()?;
    module.add_function(wrap_pyfunction!(
        estimate_decompression_context_size,
        module
    )?)?;

    Ok(())
}
