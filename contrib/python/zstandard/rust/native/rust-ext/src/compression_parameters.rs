// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {
    crate::ZstdError,
    libc::c_int,
    pyo3::{
        exceptions::{PyMemoryError, PyTypeError, PyValueError},
        prelude::*,
        types::{PyDict, PyTuple, PyType},
    },
    std::marker::PhantomData,
};

/// Safe wrapper for ZSTD_CCtx_params instances.
pub struct CCtxParams<'a>(*mut zstd_sys::ZSTD_CCtx_params, PhantomData<&'a ()>);

impl<'a> Drop for CCtxParams<'a> {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeCCtxParams(self.0);
        }
    }
}

unsafe impl<'a> Send for CCtxParams<'a> {}

unsafe impl<'a> Sync for CCtxParams<'a> {}

impl<'a> CCtxParams<'a> {
    pub(crate) unsafe fn get_raw_ptr(&self) -> *mut zstd_sys::ZSTD_CCtx_params {
        self.0
    }
}

impl<'a> CCtxParams<'a> {
    pub fn create() -> Result<Self, PyErr> {
        let params = unsafe { zstd_sys::ZSTD_createCCtxParams() };
        if params.is_null() {
            return Err(PyMemoryError::new_err("unable to create ZSTD_CCtx_params"));
        }
        Ok(CCtxParams(params, PhantomData))
    }

    pub fn set_parameter(&self, param: zstd_sys::ZSTD_cParameter, value: i32) -> PyResult<()> {
        let zresult = unsafe { zstd_sys::ZSTD_CCtxParams_setParameter(self.0, param, value) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(ZstdError::new_err(format!(
                "unable to set compression context parameter: {}",
                zstd_safe::get_error_name(zresult)
            )))
        } else {
            Ok(())
        }
    }

    fn apply_compression_parameter(
        &self,
        py: Python,
        params: &Py<ZstdCompressionParameters>,
        param: zstd_sys::ZSTD_cParameter,
    ) -> PyResult<()> {
        let value = params.borrow(py).get_parameter(param)?;
        self.set_parameter(param, value)
    }

    pub fn apply_compression_parameters(
        &self,
        py: Python,
        params: &Py<ZstdCompressionParameters>,
    ) -> PyResult<()> {
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_nbWorkers)?;
        // ZSTD_c_format.
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam2,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
        )?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_windowLog)?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_hashLog)?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_chainLog)?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_searchLog)?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_minMatch)?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_targetLength,
        )?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_strategy)?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_contentSizeFlag,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_checksumFlag,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_dictIDFlag,
        )?;
        self.apply_compression_parameter(py, &params, zstd_sys::ZSTD_cParameter::ZSTD_c_jobSize)?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_overlapLog,
        )?;
        // ZSTD_c_forceMaxWindow
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam3,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_overlapLog,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_enableLongDistanceMatching,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashLog,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmMinMatch,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmBucketSizeLog,
        )?;
        self.apply_compression_parameter(
            py,
            &params,
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashRateLog,
        )?;

        Ok(())
    }
}

/// Resolve the value of a compression context parameter.
pub(crate) fn get_cctx_parameter(
    params: *mut zstd_sys::ZSTD_CCtx_params,
    param: zstd_sys::ZSTD_cParameter,
) -> Result<libc::c_int, PyErr> {
    let mut value: libc::c_int = 0;

    let zresult =
        unsafe { zstd_sys::ZSTD_CCtxParams_getParameter(params, param, &mut value as *mut _) };

    if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
        Err(ZstdError::new_err(format!(
            "unable to retrieve parameter: {}",
            zstd_safe::get_error_name(zresult)
        )))
    } else {
        Ok(value)
    }
}

// Surely there is a better way...
pub(crate) fn int_to_strategy(value: u32) -> Result<zstd_sys::ZSTD_strategy, PyErr> {
    if zstd_sys::ZSTD_strategy::ZSTD_fast as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_fast)
    } else if zstd_sys::ZSTD_strategy::ZSTD_dfast as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_dfast)
    } else if zstd_sys::ZSTD_strategy::ZSTD_greedy as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_greedy)
    } else if zstd_sys::ZSTD_strategy::ZSTD_lazy as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_lazy)
    } else if zstd_sys::ZSTD_strategy::ZSTD_lazy2 as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_lazy2)
    } else if zstd_sys::ZSTD_strategy::ZSTD_btlazy2 as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_btlazy2)
    } else if zstd_sys::ZSTD_strategy::ZSTD_btopt as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_btopt)
    } else if zstd_sys::ZSTD_strategy::ZSTD_btultra as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_btultra)
    } else if zstd_sys::ZSTD_strategy::ZSTD_btultra2 as u32 == value {
        Ok(zstd_sys::ZSTD_strategy::ZSTD_btultra2)
    } else {
        Err(PyValueError::new_err("unknown compression strategy"))
    }
}

#[pyclass(module = "zstandard.backend_rust")]
pub struct ZstdCompressionParameters {
    pub(crate) params: *mut zstd_sys::ZSTD_CCtx_params,
}

unsafe impl Sync for ZstdCompressionParameters {}

impl Drop for ZstdCompressionParameters {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeCCtxParams(self.params);
        }
    }
}

unsafe impl Send for ZstdCompressionParameters {}

impl ZstdCompressionParameters {
    pub(crate) fn get_parameter(&self, param: zstd_sys::ZSTD_cParameter) -> PyResult<c_int> {
        let mut value: c_int = 0;

        let zresult = unsafe {
            zstd_sys::ZSTD_CCtxParams_getParameter(self.params, param, &mut value as *mut _)
        };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            return Err(ZstdError::new_err(format!(
                "unable to retrieve parameter: {}",
                zstd_safe::get_error_name(zresult)
            )));
        }

        Ok(value)
    }

    fn set_parameter(&self, param: zstd_sys::ZSTD_cParameter, value: i32) -> PyResult<()> {
        let zresult = unsafe { zstd_sys::ZSTD_CCtxParams_setParameter(self.params, param, value) };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            return Err(ZstdError::new_err(format!(
                "unable to set compression context parameter: {}",
                zstd_safe::get_error_name(zresult)
            )));
        }

        Ok(())
    }

    /// Set parameters from a dictionary of options.
    fn set_parameters(&self, kwargs: &Bound<'_, PyDict>) -> PyResult<()> {
        unsafe {
            zstd_sys::ZSTD_CCtxParams_reset(self.params);
        }

        let mut format = 0;
        let mut compression_level = 0;
        let mut window_log = 0;
        let mut hash_log = 0;
        let mut chain_log = 0;
        let mut search_log = 0;
        let mut min_match = 0;
        let mut target_length = 0;
        let mut strategy = -1;
        let mut write_content_size = 1;
        let mut write_checksum = 0;
        let mut write_dict_id = 0;
        let mut job_size = 0;
        let mut overlap_log = -1;
        let mut force_max_window = 0;
        let mut enable_ldm = 0;
        let mut ldm_hash_log = 0;
        let mut ldm_min_match = 0;
        let mut ldm_bucket_size_log = 0;
        let mut ldm_hash_rate_log = -1;
        let mut threads = 0;

        for (key, value) in kwargs.iter() {
            let key = key.extract::<String>()?;

            match key.as_ref() {
                "format" => format = value.extract::<_>()?,
                "compression_level" => compression_level = value.extract::<_>()?,
                "window_log" => window_log = value.extract::<_>()?,
                "hash_log" => hash_log = value.extract::<_>()?,
                "chain_log" => chain_log = value.extract::<_>()?,
                "search_log" => search_log = value.extract::<_>()?,
                "min_match" => min_match = value.extract::<_>()?,
                "target_length" => target_length = value.extract::<_>()?,
                "strategy" => strategy = value.extract::<_>()?,
                "write_content_size" => write_content_size = value.extract::<_>()?,
                "write_checksum" => write_checksum = value.extract::<_>()?,
                "write_dict_id" => write_dict_id = value.extract::<_>()?,
                "job_size" => job_size = value.extract::<_>()?,
                "overlap_log" => overlap_log = value.extract::<_>()?,
                "force_max_window" => force_max_window = value.extract::<_>()?,
                "enable_ldm" => enable_ldm = value.extract::<_>()?,
                "ldm_hash_log" => ldm_hash_log = value.extract::<_>()?,
                "ldm_min_match" => ldm_min_match = value.extract::<_>()?,
                "ldm_bucket_size_log" => ldm_bucket_size_log = value.extract::<_>()?,
                "ldm_hash_rate_log" => ldm_hash_rate_log = value.extract::<_>()?,
                "threads" => threads = value.extract::<_>()?,
                key => {
                    return Err(PyTypeError::new_err(format!(
                        "'{}' is an invalid keyword argument",
                        key
                    )));
                }
            }
        }

        if threads < 0 {
            threads = num_cpus::get() as _;
        }

        // We need to set ZSTD_c_nbWorkers before ZSTD_c_jobSize and ZSTD_c_overlapLog
        // because setting ZSTD_c_nbWorkers resets the other parameters.
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_nbWorkers, threads)?;

        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam2, format)?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
            compression_level,
        )?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_windowLog, window_log)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_hashLog, hash_log)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_chainLog, chain_log)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_searchLog, search_log)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_minMatch, min_match)?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_targetLength,
            target_length,
        )?;

        if strategy == -1 {
            strategy = 0;
        }

        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_strategy, strategy)?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_contentSizeFlag,
            write_content_size,
        )?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_checksumFlag,
            write_checksum,
        )?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_dictIDFlag, write_dict_id)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_jobSize, job_size)?;

        if overlap_log == -1 {
            overlap_log = 0;
        }

        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_overlapLog, overlap_log)?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam3,
            force_max_window,
        )?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_enableLongDistanceMatching,
            enable_ldm,
        )?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashLog, ldm_hash_log)?;
        self.set_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmMinMatch, ldm_min_match)?;
        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmBucketSizeLog,
            ldm_bucket_size_log,
        )?;

        if ldm_hash_rate_log == -1 {
            ldm_hash_rate_log = 0;
        }

        self.set_parameter(
            zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashRateLog,
            ldm_hash_rate_log,
        )?;

        Ok(())
    }
}

#[pymethods]
impl ZstdCompressionParameters {
    #[classmethod]
    #[pyo3(signature = (* args, * * kwargs))]
    fn from_level(
        _cls: Bound<'_, PyType>,
        py: Python,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        if args.len() != 1 {
            return Err(PyTypeError::new_err(format!(
                "from_level() takes exactly 1 argument ({} given)",
                args.len()
            )));
        }

        let kwargs = if let Some(v) = kwargs {
            v.copy()?
        } else {
            PyDict::new(py)
        };

        let level = args.get_item(0)?.extract::<i32>()?;

        let source_size = if let Some(value) = kwargs.get_item("source_size")? {
            kwargs.del_item("source_size")?;
            value.extract::<u64>()?
        } else {
            0
        };

        let dict_size = if let Some(value) = kwargs.get_item("dict_size")? {
            kwargs.del_item("dict_size")?;
            value.extract::<usize>()?
        } else {
            0
        };

        let compression_params =
            unsafe { zstd_sys::ZSTD_getCParams(level, source_size, dict_size) };

        if !kwargs.contains("window_log")? {
            kwargs.set_item("window_log", compression_params.windowLog)?;
        }
        if !kwargs.contains("chain_log")? {
            kwargs.set_item("chain_log", compression_params.chainLog)?;
        }
        if !kwargs.contains("hash_log")? {
            kwargs.set_item("hash_log", compression_params.hashLog)?;
        }
        if !kwargs.contains("search_log")? {
            kwargs.set_item("search_log", compression_params.searchLog)?;
        }
        if !kwargs.contains("min_match")? {
            kwargs.set_item("min_match", compression_params.minMatch)?;
        }
        if !kwargs.contains("target_length")? {
            kwargs.set_item("target_length", compression_params.targetLength)?;
        }
        if !kwargs.contains("strategy")? {
            kwargs.set_item("strategy", compression_params.strategy as u32)?;
        }

        Self::new(py, &PyTuple::empty(py), Some(&kwargs))
    }

    #[new]
    #[pyo3(signature = (* _args, * * kwargs))]
    fn new(
        py: Python,
        _args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let params = unsafe { zstd_sys::ZSTD_createCCtxParams() };
        if params.is_null() {
            return Err(PyMemoryError::new_err("unable to create ZSTD_CCtx_params"));
        }

        let instance = ZstdCompressionParameters { params };

        let kwargs = if let Some(v) = kwargs {
            v.copy()?
        } else {
            PyDict::new(py)
        };

        instance.set_parameters(&kwargs)?;

        Ok(instance)
    }

    #[getter]
    fn format(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam2)
    }

    #[getter]
    fn compression_level(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel)
    }

    #[getter]
    fn window_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_windowLog)
    }

    #[getter]
    fn hash_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_hashLog)
    }

    #[getter]
    fn chain_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_chainLog)
    }

    #[getter]
    fn search_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_searchLog)
    }

    #[getter]
    fn min_match(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_minMatch)
    }

    #[getter]
    fn target_length(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_targetLength)
    }

    #[getter]
    fn strategy(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_strategy)
    }

    #[getter]
    fn write_content_size(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_contentSizeFlag)
    }

    #[getter]
    fn write_checksum(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_checksumFlag)
    }

    #[getter]
    fn write_dict_id(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_dictIDFlag)
    }

    #[getter]
    fn overlap_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_overlapLog)
    }

    #[getter]
    fn force_max_window(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_experimentalParam3)
    }

    #[getter]
    fn enable_ldm(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_enableLongDistanceMatching)
    }

    #[getter]
    fn ldm_hash_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashLog)
    }

    #[getter]
    fn ldm_min_match(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmMinMatch)
    }

    #[getter]
    fn ldm_bucket_size_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmBucketSizeLog)
    }

    #[getter]
    fn ldm_hash_rate_log(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_ldmHashRateLog)
    }

    #[getter]
    fn threads(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_nbWorkers)
    }

    #[getter]
    fn job_size(&self) -> PyResult<c_int> {
        self.get_parameter(zstd_sys::ZSTD_cParameter::ZSTD_c_jobSize)
    }

    fn estimated_compression_context_size(&self) -> PyResult<usize> {
        let size = unsafe { zstd_sys::ZSTD_estimateCCtxSize_usingCCtxParams(self.params) };

        Ok(size)
    }
}

pub(crate) fn init_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ZstdCompressionParameters>()?;

    Ok(())
}
