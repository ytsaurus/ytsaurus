// Copyright (c) 2021-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::{
        buffers::{BufferSegment, ZstdBufferWithSegments, ZstdBufferWithSegmentsCollection},
        compression_dict::ZstdCompressionDict,
        compression_parameters::CCtxParams,
        exceptions::ZstdError,
        zstd_safe::CCtx,
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
}

pub fn multi_compress_to_buffer(
    py: Python,
    params: &CCtxParams,
    dict: &Option<Py<ZstdCompressionDict>>,
    data: &Bound<'_, PyAny>,
    threads: isize,
) -> PyResult<ZstdBufferWithSegmentsCollection> {
    let threads = if threads < 0 {
        num_cpus::get()
    } else if threads < 2 {
        1
    } else {
        threads as _
    };

    let mut sources = vec![];
    let mut total_source_size = 0;

    if let Ok(buffer) = data.downcast::<ZstdBufferWithSegments>() {
        sources.reserve_exact(buffer.borrow().segments.len());

        let borrow = buffer.borrow();

        for i in 0..borrow.segments.len() {
            let slice = borrow.get_segment_slice(py, i);

            sources.push(DataSource { data: slice });
            total_source_size += slice.len();
        }
    } else if let Ok(collection) = data.downcast::<ZstdBufferWithSegmentsCollection>() {
        sources.reserve_exact(collection.len()?);

        for buffer_obj in &collection.borrow().buffers {
            let buffer = buffer_obj.downcast_bound::<ZstdBufferWithSegments>(py)?;
            let borrow = buffer.borrow();

            for i in 0..borrow.segments.len() {
                let slice = borrow.get_segment_slice(py, i);

                sources.push(DataSource { data: slice });
                total_source_size += slice.len();
            }
        }
    } else if let Ok(list) = data.extract::<Bound<'_, PyList>>() {
        sources.reserve_exact(list.len());

        for (i, item) in list.iter().enumerate() {
            let buffer: PyBuffer<u8> = PyBuffer::get(&item.as_borrowed())
                .map_err(|_| PyTypeError::new_err(format!("item {} not a bytes like object", i)))?;

            let slice = unsafe {
                std::slice::from_raw_parts(buffer.buf_ptr() as *const _, buffer.len_bytes())
            };

            sources.push(DataSource { data: slice });
            total_source_size += slice.len();
        }
    } else {
        return Err(PyTypeError::new_err(
            "argument must be list of BufferWithSegments",
        ));
    }

    if sources.is_empty() {
        return Err(PyValueError::new_err("no source elements found"));
    }

    if total_source_size == 0 {
        return Err(PyValueError::new_err("source elements are empty"));
    }

    compress_from_datasources(py, params, dict, sources, threads)
}

/// Holds results of an individual compression operation.
struct WorkerResult {
    source_offset: usize,
    error: Option<&'static str>,
    data: Option<Vec<u8>>,
}

fn compress_from_datasources(
    py: Python,
    params: &CCtxParams,
    dict: &Option<Py<ZstdCompressionDict>>,
    sources: Vec<DataSource>,
    thread_count: usize,
) -> PyResult<ZstdBufferWithSegmentsCollection> {
    // More threads than inputs makes no sense.
    let thread_count = std::cmp::min(thread_count, sources.len());

    // TODO lower thread count when input size is too small and threads
    // would add overhead.

    let mut cctxs = Vec::with_capacity(thread_count);
    let results = std::sync::Mutex::new(Vec::with_capacity(sources.len()));

    // TODO there are tons of inefficiencies in this implementation compared
    // to the C backend.

    for _ in 0..thread_count {
        let cctx = CCtx::new().map_err(|msg| ZstdError::new_err(msg))?;

        cctx.set_parameters(params).map_err(|msg| {
            ZstdError::new_err(format!("could not set compression parameters: {}", msg))
        })?;

        if let Some(dict) = dict {
            dict.borrow(py).load_into_cctx(&cctx)?;
        }

        cctxs.push(cctx);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .map_err(|err| ZstdError::new_err(format!("error initializing thread pool: {}", err)))?;

    pool.install(|| {
        sources.par_iter().enumerate().for_each(|(index, source)| {
            let thread_index = pool.current_thread_index().unwrap();

            let cctx = &cctxs[thread_index];

            let mut result = WorkerResult {
                source_offset: index,
                error: None,
                data: None,
            };

            match cctx.compress(source.data) {
                Ok(chunk) => {
                    result.data = Some(chunk);
                }
                Err(msg) => {
                    result.error = Some(msg);
                }
            }

            // TODO we can do better than a shared lock.
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
                if let Some(msg) = result.error {
                    return Err(ZstdError::new_err(format!(
                        "error compressing item {}: {}",
                        result.source_offset, msg
                    )));
                }

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
