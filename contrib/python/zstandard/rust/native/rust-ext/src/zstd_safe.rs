// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use {crate::compression_parameters::CCtxParams, std::marker::PhantomData};

/// Safe wrapper for ZSTD_CDict instances.
pub struct CDict<'a> {
    ptr: *mut zstd_sys::ZSTD_CDict,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> CDict<'a> {
    // TODO annotate lifetime of data to ensure outlives Self
    pub fn from_data(
        data: &[u8],
        content_type: zstd_sys::ZSTD_dictContentType_e,
        params: zstd_sys::ZSTD_compressionParameters,
    ) -> Result<Self, &'static str> {
        let ptr = unsafe {
            zstd_sys::ZSTD_createCDict_advanced(
                data.as_ptr() as *const _,
                data.len(),
                zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byRef,
                content_type,
                params,
                zstd_sys::ZSTD_customMem {
                    customAlloc: None,
                    customFree: None,
                    opaque: std::ptr::null_mut(),
                },
            )
        };
        if ptr.is_null() {
            Err("unable to precompute dictionary")
        } else {
            Ok(Self {
                ptr,
                _phantom: PhantomData,
            })
        }
    }
}

impl<'a> Drop for CDict<'a> {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeCDict(self.ptr);
        }
    }
}

unsafe impl<'a> Send for CDict<'a> {}

unsafe impl<'a> Sync for CDict<'a> {}

/// Safe wrapper for ZSTD_DDict instances.
pub struct DDict<'a> {
    ptr: *mut zstd_sys::ZSTD_DDict,
    _phantom: PhantomData<&'a ()>,
}

unsafe impl<'a> Send for DDict<'a> {}
unsafe impl<'a> Sync for DDict<'a> {}

impl<'a> Drop for DDict<'a> {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeDDict(self.ptr);
        }
    }
}

impl<'a> DDict<'a> {
    // TODO lifetime of data should be annotated to ensure it outlives Self
    pub fn from_data(
        data: &[u8],
        content_type: zstd_sys::ZSTD_dictContentType_e,
    ) -> Result<Self, &'static str> {
        let ptr = unsafe {
            zstd_sys::ZSTD_createDDict_advanced(
                data.as_ptr() as *const _,
                data.len(),
                zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byRef,
                content_type,
                zstd_sys::ZSTD_customMem {
                    customAlloc: None,
                    customFree: None,
                    opaque: std::ptr::null_mut(),
                },
            )
        };
        if ptr.is_null() {
            Err("could not create compression dict")
        } else {
            Ok(Self {
                ptr,
                _phantom: PhantomData,
            })
        }
    }
}

pub struct CCtx<'a>(*mut zstd_sys::ZSTD_CCtx, PhantomData<&'a ()>);

impl<'a> Drop for CCtx<'a> {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeCCtx(self.0);
        }
    }
}

unsafe impl<'a> Send for CCtx<'a> {}
unsafe impl<'a> Sync for CCtx<'a> {}

impl<'a> CCtx<'a> {
    pub fn new() -> Result<Self, &'static str> {
        let cctx = unsafe { zstd_sys::ZSTD_createCCtx() };
        if cctx.is_null() {
            return Err("could not allocate ZSTD_CCtx instance");
        }

        Ok(Self(cctx, PhantomData))
    }

    pub fn cctx(&self) -> *mut zstd_sys::ZSTD_CCtx {
        self.0
    }

    pub fn set_parameters(&self, params: &CCtxParams) -> Result<(), String> {
        let zresult = unsafe {
            zstd_sys::ZSTD_CCtx_setParametersUsingCCtxParams(self.0, params.get_raw_ptr())
        };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            return Err(zstd_safe::get_error_name(zresult).to_string());
        }

        Ok(())
    }

    pub fn memory_size(&self) -> usize {
        unsafe { zstd_sys::ZSTD_sizeof_CCtx(self.0 as *const _) }
    }

    pub fn reset(&self) -> usize {
        unsafe {
            zstd_sys::ZSTD_CCtx_reset(
                self.0,
                zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only,
            )
        }
    }

    pub fn set_pledged_source_size(&self, size: u64) -> Result<(), &'static str> {
        let zresult = unsafe { zstd_sys::ZSTD_CCtx_setPledgedSrcSize(self.0, size) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn load_computed_dict<'b: 'a>(&'a self, cdict: &'b CDict) -> Result<(), &'static str> {
        let zresult = unsafe { zstd_sys::ZSTD_CCtx_refCDict(self.0, cdict.ptr) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn load_dict_data<'b: 'a>(
        &'a self,
        data: &'b [u8],
        content_type: zstd_sys::ZSTD_dictContentType_e,
    ) -> Result<(), &'static str> {
        let zresult = unsafe {
            zstd_sys::ZSTD_CCtx_loadDictionary_advanced(
                self.0,
                data.as_ptr() as *const _,
                data.len(),
                zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byRef,
                content_type,
            )
        };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn get_frame_progression(&self) -> zstd_sys::ZSTD_frameProgression {
        unsafe { zstd_sys::ZSTD_getFrameProgression(self.0) }
    }

    pub fn compress(&self, source: &[u8]) -> Result<Vec<u8>, &'static str> {
        self.reset();

        let dest_len = unsafe { zstd_sys::ZSTD_compressBound(source.len()) };

        let mut dest: Vec<u8> = Vec::with_capacity(dest_len);

        self.set_pledged_source_size(dest_len as _)?;

        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: source.as_ptr() as *const _,
            size: source.len(),
            pos: 0,
        };

        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest.as_mut_ptr() as *mut _,
            size: dest.capacity(),
            pos: 0,
        };

        // By avoiding ZSTD_compress(), we don't necessarily write out content
        // size. This means the parameters to control frame parameters are honored.
        let zresult = unsafe {
            zstd_sys::ZSTD_compressStream2(
                self.0,
                &mut out_buffer as *mut _,
                &mut in_buffer as *mut _,
                zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
        };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else if zresult > 0 {
            Err("unexpected partial frame flush")
        } else {
            unsafe { dest.set_len(out_buffer.pos) }

            Ok(dest)
        }
    }

    /// Compress input data as part of a stream.
    ///
    /// Returns a tuple of the emitted compressed data, a slice of unconsumed input,
    /// and whether there is more work to be done.
    pub fn compress_chunk(
        &self,
        source: &'a [u8],
        end_mode: zstd_sys::ZSTD_EndDirective,
        output_size: usize,
    ) -> Result<(Vec<u8>, &'a [u8], bool), &'static str> {
        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: source.as_ptr() as *const _,
            size: source.len() as _,
            pos: 0,
        };

        let mut dest: Vec<u8> = Vec::with_capacity(output_size);

        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest.as_mut_ptr() as *mut _,
            size: dest.capacity(),
            pos: 0,
        };

        let zresult = unsafe {
            zstd_sys::ZSTD_compressStream2(
                self.0,
                &mut out_buffer as *mut _,
                &mut in_buffer as *mut _,
                end_mode,
            )
        };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            return Err(zstd_safe::get_error_name(zresult));
        }

        unsafe {
            dest.set_len(out_buffer.pos);
        }

        let remaining = &source[in_buffer.pos..source.len()];

        Ok((dest, remaining, zresult != 0))
    }

    pub fn compress_buffers(
        &self,
        out_buffer: &mut zstd_sys::ZSTD_outBuffer,
        in_buffer: &mut zstd_sys::ZSTD_inBuffer,
        end_mode: zstd_sys::ZSTD_EndDirective,
    ) -> Result<usize, &'static str> {
        let zresult = unsafe {
            zstd_sys::ZSTD_compressStream2(
                self.0,
                out_buffer as *mut _,
                in_buffer as *mut _,
                end_mode,
            )
        };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(zresult)
        }
    }

    /// Compress data into a destination vector.
    ///
    /// The vector will be appended to, up to its currently allocated capacity.
    /// The vector's length will be adjusted to account for written data.
    pub fn compress_into_vec(
        &self,
        dest_buffer: &mut Vec<u8>,
        in_buffer: &mut zstd_sys::ZSTD_inBuffer,
        end_mode: zstd_sys::ZSTD_EndDirective,
    ) -> Result<usize, &'static str> {
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest_buffer.as_mut_ptr() as *mut _,
            size: dest_buffer.capacity(),
            pos: dest_buffer.len(),
        };

        let zresult = self.compress_buffers(&mut out_buffer, in_buffer, end_mode)?;

        unsafe {
            dest_buffer.set_len(out_buffer.pos);
        }

        Ok(zresult)
    }
}

pub struct DCtx<'a>(*mut zstd_sys::ZSTD_DCtx, PhantomData<&'a ()>);

impl<'a> Drop for DCtx<'a> {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeDCtx(self.0);
        }
    }
}

unsafe impl<'a> Send for DCtx<'a> {}
unsafe impl<'a> Sync for DCtx<'a> {}

impl<'a> DCtx<'a> {
    pub fn new() -> Result<Self, &'static str> {
        let dctx = unsafe { zstd_sys::ZSTD_createDCtx() };
        if dctx.is_null() {
            return Err("could not allocate ZSTD_DCtx instance");
        }

        Ok(Self(dctx, PhantomData))
    }

    pub fn dctx(&self) -> *mut zstd_sys::ZSTD_DCtx {
        self.0
    }

    pub fn memory_size(&self) -> usize {
        unsafe { zstd_sys::ZSTD_sizeof_DCtx(self.0) }
    }

    pub fn reset(&self) -> Result<(), &'static str> {
        let zresult = unsafe {
            zstd_sys::ZSTD_DCtx_reset(
                self.0,
                zstd_sys::ZSTD_ResetDirective::ZSTD_reset_session_only,
            )
        };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn set_max_window_size(&self, size: usize) -> Result<(), &'static str> {
        let zresult = unsafe { zstd_sys::ZSTD_DCtx_setMaxWindowSize(self.0, size) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn set_format(&self, format: zstd_sys::ZSTD_format_e) -> Result<(), &'static str> {
        let zresult = unsafe { zstd_sys::ZSTD_DCtx_setFormat(self.0, format) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn load_prepared_dict<'b: 'a>(&'a self, dict: &'b DDict) -> Result<(), &'static str> {
        let zresult = unsafe { zstd_sys::ZSTD_DCtx_refDDict(self.0, dict.ptr) };
        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(())
        }
    }

    pub fn decompress_buffers(
        &self,
        out_buffer: &mut zstd_sys::ZSTD_outBuffer,
        in_buffer: &mut zstd_sys::ZSTD_inBuffer,
    ) -> Result<usize, &'static str> {
        let zresult = unsafe {
            zstd_sys::ZSTD_decompressStream(self.0, out_buffer as *mut _, in_buffer as *mut _)
        };

        if unsafe { zstd_sys::ZSTD_isError(zresult) } != 0 {
            Err(zstd_safe::get_error_name(zresult))
        } else {
            Ok(zresult)
        }
    }

    pub fn decompress_into_vec(
        &self,
        dest_buffer: &mut Vec<u8>,
        in_buffer: &mut zstd_sys::ZSTD_inBuffer,
    ) -> Result<usize, &'static str> {
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: dest_buffer.as_mut_ptr() as *mut _,
            size: dest_buffer.capacity(),
            pos: dest_buffer.len(),
        };

        let zresult = self.decompress_buffers(&mut out_buffer, in_buffer)?;

        unsafe {
            dest_buffer.set_len(out_buffer.pos);
        }

        Ok(zresult)
    }
}

pub fn train_dictionary_fastcover(
    dict_buffer: &mut Vec<u8>,
    samples_buffer: &[u8],
    samples_sizes: &[usize],
    params: &mut zstd_sys::ZDICT_fastCover_params_t,
) -> Result<(), &'static str> {
    let zresult = unsafe {
        zstd_sys::ZDICT_optimizeTrainFromBuffer_fastCover(
            dict_buffer.as_mut_ptr() as *mut _,
            dict_buffer.capacity(),
            samples_buffer.as_ptr() as *const _,
            samples_sizes.as_ptr(),
            samples_sizes.len() as _,
            params,
        )
    };
    if unsafe { zstd_sys::ZDICT_isError(zresult) } != 0 {
        Err(zstd_safe::get_error_name(zresult))
    } else {
        unsafe {
            dict_buffer.set_len(zresult);
        }

        Ok(())
    }
}
