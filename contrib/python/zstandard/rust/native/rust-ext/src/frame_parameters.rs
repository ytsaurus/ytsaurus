// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::ZstdError,
    pyo3::{buffer::PyBuffer, exceptions::PyValueError, prelude::*, wrap_pyfunction},
    std::ffi::c_ulonglong,
};

#[pyclass(module = "zstandard.backend_rust")]
struct FrameParameters {
    header: zstd_sys::ZSTD_FrameHeader,
}

unsafe impl Sync for FrameParameters {}

#[pymethods]
impl FrameParameters {
    #[getter]
    fn content_size(&self) -> PyResult<libc::c_ulonglong> {
        Ok(self.header.frameContentSize)
    }

    #[getter]
    fn window_size(&self) -> PyResult<libc::c_ulonglong> {
        Ok(self.header.windowSize)
    }

    #[getter]
    fn dict_id(&self) -> PyResult<libc::c_uint> {
        Ok(self.header.dictID)
    }

    #[getter]
    fn has_checksum(&self) -> PyResult<bool> {
        Ok(match self.header.checksumFlag {
            0 => false,
            _ => true,
        })
    }
}

#[pyfunction]
fn frame_content_size(data: PyBuffer<u8>) -> PyResult<i64> {
    let size = unsafe { zstd_sys::ZSTD_getFrameContentSize(data.buf_ptr(), data.len_bytes()) };

    if size == zstd_sys::ZSTD_CONTENTSIZE_ERROR as c_ulonglong {
        Err(ZstdError::new_err("error when determining content size"))
    } else if size == zstd_sys::ZSTD_CONTENTSIZE_UNKNOWN as c_ulonglong {
        Ok(-1)
    } else {
        Ok(size as _)
    }
}

#[pyfunction]
fn frame_header_size(data: PyBuffer<u8>) -> PyResult<usize> {
    let zresult = unsafe { zstd_sys::ZSTD_frameHeaderSize(data.buf_ptr(), data.len_bytes()) };
    if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
        return Err(ZstdError::new_err(format!(
            "could not determine frame header size: {}",
            zstd_safe::get_error_name(zresult)
        )));
    }

    Ok(zresult)
}

#[pyfunction]
#[pyo3(signature = (buffer, format=zstd_sys::ZSTD_format_e::ZSTD_f_zstd1 as u32))]
fn get_frame_parameters(
    py: Python,
    buffer: PyBuffer<u8>,
    format: u32,
) -> PyResult<Py<FrameParameters>> {
    let raw_data = unsafe {
        std::slice::from_raw_parts::<u8>(buffer.buf_ptr() as *const _, buffer.len_bytes())
    };
    let format = if format == zstd_sys::ZSTD_format_e::ZSTD_f_zstd1 as u32 {
        zstd_sys::ZSTD_format_e::ZSTD_f_zstd1
    } else if format == zstd_sys::ZSTD_format_e::ZSTD_f_zstd1_magicless as u32 {
        zstd_sys::ZSTD_format_e::ZSTD_f_zstd1_magicless
    } else {
        return Err(PyValueError::new_err(format!("invalid format value")));
    };

    let mut header = zstd_sys::ZSTD_FrameHeader {
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
        zstd_sys::ZSTD_getFrameHeader_advanced(
            &mut header,
            raw_data.as_ptr() as *const _,
            raw_data.len(),
            format,
        )
    };

    if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
        Err(ZstdError::new_err(format!(
            "cannot get frame parameters: {}",
            zstd_safe::get_error_name(zresult)
        )))
    } else if zresult != 0 {
        Err(ZstdError::new_err(format!(
            "not enough data for frame parameters; need {} bytes",
            zresult
        )))
    } else {
        Py::new(py, FrameParameters { header })
    }
}

pub(crate) fn init_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<FrameParameters>()?;
    module.add_function(wrap_pyfunction!(frame_content_size, module)?)?;
    module.add_function(wrap_pyfunction!(frame_header_size, module)?)?;
    module.add_function(wrap_pyfunction!(get_frame_parameters, module)?)?;

    Ok(())
}
