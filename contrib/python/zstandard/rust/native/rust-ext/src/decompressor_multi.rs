// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        buffers::{BufferSegment, ZstdBufferWithSegments, ZstdBufferWithSegmentsCollection},
        compression_dict::ZstdCompressionDict,
        exceptions::ZstdError,
        zstd_safe::DCtx,
    },
    pyo3::{
        buffer::PyBuffer,
        exceptions::{PyTypeError, PyValueError},
        prelude::*,
        types::{PyBytes, PyList, PyTuple},
    },
    rayon::prelude::*,
};

struct DataSource<'a> {
    data: &'a [u8],
    decompressed_size: usize,
}

pub fn multi_decompress_to_buffer(
    py: Python,
    dict_data: Option<&Py<ZstdCompressionDict>>,
    frames: &Bound<'_, PyAny>,
    decompressed_sizes: Option<&Bound<'_, PyAny>>,
    threads: isize,
) -> PyResult<ZstdBufferWithSegmentsCollection> {
    let threads = if threads < 0 {
        num_cpus::get()
    } else if threads < 2 {
        1
    } else {
        threads as _
    };

    let frame_sizes: &[u64] = if let Some(frames_sizes) = decompressed_sizes {
        let buffer: PyBuffer<u8> = PyBuffer::get(&frames_sizes.as_borrowed())?;
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const _, buffer.len_bytes() / 8) }
    } else {
        &[]
    };

    let mut sources = vec![];

    if let Ok(buffer) = frames.downcast::<ZstdBufferWithSegments>() {
        if decompressed_sizes.is_some() && frame_sizes.len() != buffer.len()? {
            return Err(PyValueError::new_err(format!(
                "decompressed_sizes size mismatch; expected {}, got {}",
                buffer.len()?,
                frame_sizes.len()
            )));
        }

        let borrow = buffer.borrow();

        sources.reserve_exact(borrow.segments.len());

        for i in 0..borrow.segments.len() {
            let slice = borrow.get_segment_slice(py, i);

            sources.push(DataSource {
                data: slice,
                decompressed_size: *frame_sizes.get(i).unwrap_or(&0) as _,
            });
        }
    } else if let Ok(collection) = frames.downcast::<ZstdBufferWithSegmentsCollection>() {
        let frames_count = collection.len()?;

        if decompressed_sizes.is_some() && frame_sizes.len() != frames_count {
            return Err(PyValueError::new_err(format!(
                "decompressed_sizes size mismatch; expected {}, got {}",
                frames_count,
                frame_sizes.len()
            )));
        }

        sources.reserve_exact(frames_count);

        let mut offset = 0;
        for buffer_obj in &collection.borrow().buffers {
            let buffer = buffer_obj.downcast_bound::<ZstdBufferWithSegments>(py)?;
            let borrow = buffer.borrow();

            for i in 0..borrow.segments.len() {
                let slice = borrow.get_segment_slice(py, i);

                sources.push(DataSource {
                    data: slice,
                    decompressed_size: *frame_sizes.get(offset).unwrap_or(&0) as _,
                });

                offset += 1;
            }
        }
    } else if let Ok(list) = frames.extract::<Bound<'_, PyList>>() {
        if decompressed_sizes.is_some() && frame_sizes.len() != list.len() {
            return Err(PyValueError::new_err(format!(
                "decompressed_sizes size mismatch; expected {}; got {}",
                list.len(),
                frame_sizes.len()
            )));
        }

        sources.reserve_exact(list.len());

        for (i, item) in list.iter().enumerate() {
            let buffer: PyBuffer<u8> = PyBuffer::get(&item.as_borrowed())
                .map_err(|_| PyTypeError::new_err(format!("item {} not a bytes like object", i)))?;

            let slice = unsafe {
                std::slice::from_raw_parts(buffer.buf_ptr() as *const _, buffer.len_bytes())
            };

            sources.push(DataSource {
                data: slice,
                decompressed_size: *frame_sizes.get(i).unwrap_or(&0) as _,
            });
        }
    } else {
        return Err(PyTypeError::new_err(
            "argument must be list of BufferWithSegments",
        ));
    }

    decompress_from_datasources(py, dict_data, sources, threads)
}

#[derive(Debug, PartialEq)]
enum WorkerError {
    None,
    NoSize,
    Zstd(&'static str),
}

/// Holds results of an individual compression operation.
struct WorkerResult {
    source_offset: usize,
    error: WorkerError,
    data: Option<Vec<u8>>,
}

fn decompress_from_datasources(
    py: Python,
    dict_data: Option<&Py<ZstdCompressionDict>>,
    sources: Vec<DataSource>,
    thread_count: usize,
) -> PyResult<ZstdBufferWithSegmentsCollection> {
    // More threads than inputs makes no sense.
    let thread_count = std::cmp::min(thread_count, sources.len());

    // TODO lower thread count when input size is too small and threads
    // would add overhead.

    let mut dctxs = Vec::with_capacity(thread_count);
    let results = std::sync::Mutex::new(Vec::with_capacity(sources.len()));

    // TODO there are tons of inefficiencies in this implementation compared
    // to the C backend.

    for _ in 0..thread_count {
        let dctx = DCtx::new().map_err(ZstdError::new_err)?;

        if let Some(dict_data) = dict_data {
            dict_data.borrow_mut(py).load_into_dctx(&dctx)?;
        }

        dctxs.push(dctx);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .map_err(|err| ZstdError::new_err(format!("error initializing thread pool: {}", err)))?;

    pool.install(|| {
        sources
            .par_iter()
            .enumerate()
            .for_each(|(index, source): (usize, &DataSource)| {
                let thread_index = pool.current_thread_index().unwrap();

                let dctx = &dctxs[thread_index];

                let mut result = WorkerResult {
                    source_offset: index,
                    error: WorkerError::None,
                    data: None,
                };

                let decompressed_size = if source.decompressed_size == 0 {
                    let frame_size = match zstd_safe::get_frame_content_size(source.data) {
                        Err(zstd_safe::ContentSizeError) => {
                            result.error = WorkerError::NoSize;
                            zstd_safe::CONTENTSIZE_ERROR
                        }
                        Ok(None) => {
                            result.error = WorkerError::NoSize;
                            zstd_safe::CONTENTSIZE_UNKNOWN
                        }
                        Ok(Some(frame_size)) => frame_size,
                    };

                    frame_size as _
                } else {
                    source.decompressed_size
                };

                if result.error == WorkerError::None {
                    let mut dest_buffer = Vec::with_capacity(decompressed_size);
                    let mut in_buffer = zstd_sys::ZSTD_inBuffer {
                        src: source.data.as_ptr() as *const _,
                        size: source.data.len(),
                        pos: 0,
                    };

                    match dctx.decompress_into_vec(&mut dest_buffer, &mut in_buffer) {
                        Ok(_) => {
                            result.data = Some(dest_buffer);
                        }
                        Err(msg) => {
                            result.error = WorkerError::Zstd(msg);
                        }
                    }
                }

                results.lock().unwrap().push(result);
            });
    });

    // Need to sort results by their input order or else results aren't
    // deterministic.
    results
        .lock()
        .unwrap()
        .sort_by(|a, b| a.source_offset.cmp(&b.source_offset));

    // TODO this is horribly inefficient due to memory copies.
    let els = PyTuple::new(
        py,
        results
            .lock()
            .unwrap()
            .iter()
            .map(|result| {
                match result.error {
                    WorkerError::None => Ok(()),
                    WorkerError::Zstd(msg) => Err(ZstdError::new_err(format!(
                        "error decompressing item {}: {}",
                        result.source_offset, msg
                    ))),
                    WorkerError::NoSize => Err(PyValueError::new_err(format!(
                        "could not determine decompressed size of item {}",
                        result.source_offset
                    ))),
                }?;

                let data = result.data.as_ref().unwrap();
                let chunk = PyBytes::new(py, data);
                let segments = vec![BufferSegment {
                    offset: 0,
                    length: data.len() as _,
                }];

                let segments = unsafe {
                    PyBytes::from_ptr(
                        py,
                        segments.as_ptr() as *const _,
                        segments.len() * std::mem::size_of::<BufferSegment>(),
                    )
                };
                let segments_buffer = PyBuffer::get(&segments)?;

                Py::new(
                    py,
                    ZstdBufferWithSegments::new(py, &chunk, segments_buffer)?,
                )
            })
            .collect::<PyResult<Vec<_>>>()?,
    )?;

    ZstdBufferWithSegmentsCollection::new(py, &els)
}
