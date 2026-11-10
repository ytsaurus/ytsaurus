// Copyright (c) 2020-present, Gregory Szorc
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

use pyo3::{prelude::*, types::PyBytes};

pub(crate) const COMPRESSOBJ_FLUSH_FINISH: i32 = 0;
pub(crate) const COMPRESSOBJ_FLUSH_BLOCK: i32 = 1;

pub(crate) fn init_module(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version", super::VERSION)?;
    module.add("__doc__", "Rust backend for zstandard bindings")?;

    module.add("FLUSH_BLOCK", 0)?;
    module.add("FLUSH_FRAME", 1)?;

    module.add("COMPRESSOBJ_FLUSH_FINISH", COMPRESSOBJ_FLUSH_FINISH)?;
    module.add("COMPRESSOBJ_FLUSH_BLOCK", COMPRESSOBJ_FLUSH_BLOCK)?;

    module.add(
        "ZSTD_VERSION",
        (
            zstd_safe::VERSION_MAJOR,
            zstd_safe::VERSION_MINOR,
            zstd_safe::VERSION_RELEASE,
        ),
    )?;
    module.add("FRAME_HEADER", PyBytes::new(py, b"\x28\xb5\x2f\xfd"))?;

    module.add("CONTENTSIZE_UNKNOWN", zstd_safe::CONTENTSIZE_UNKNOWN)?;
    module.add("CONTENTSIZE_ERROR", zstd_safe::CONTENTSIZE_ERROR)?;

    module.add("MAX_COMPRESSION_LEVEL", zstd_safe::max_c_level())?;
    module.add(
        "COMPRESSION_RECOMMENDED_INPUT_SIZE",
        zstd_safe::CCtx::in_size(),
    )?;
    module.add(
        "COMPRESSION_RECOMMENDED_OUTPUT_SIZE",
        zstd_safe::CCtx::out_size(),
    )?;
    module.add(
        "DECOMPRESSION_RECOMMENDED_INPUT_SIZE",
        zstd_safe::DCtx::in_size(),
    )?;
    module.add(
        "DECOMPRESSION_RECOMMENDED_OUTPUT_SIZE",
        zstd_safe::DCtx::out_size(),
    )?;

    module.add("MAGIC_NUMBER", zstd_safe::MAGICNUMBER)?;
    module.add("BLOCKSIZELOG_MAX", zstd_safe::BLOCKSIZELOG_MAX)?;
    module.add("BLOCKSIZE_MAX", zstd_safe::BLOCKSIZE_MAX)?;
    module.add("WINDOWLOG_MIN", zstd_safe::WINDOWLOG_MIN)?;

    let windowlog_max = if cfg!(target_pointer_width = "32") {
        zstd_safe::WINDOWLOG_MAX_32
    } else {
        zstd_safe::WINDOWLOG_MAX_64
    };

    module.add("WINDOWLOG_MAX", windowlog_max)?;
    module.add("CHAINLOG_MIN", zstd_safe::CHAINLOG_MIN)?;
    module.add(
        "CHAINLOG_MAX",
        if cfg!(target_pointer_width = "32") {
            zstd_safe::CHAINLOG_MAX_32
        } else {
            zstd_safe::CHAINLOG_MAX_64
        },
    )?;
    module.add("HASHLOG_MIN", zstd_safe::HASHLOG_MIN)?;
    module.add(
        "HASHLOG_MAX",
        if windowlog_max < 30 {
            windowlog_max
        } else {
            30
        },
    )?;
    module.add("SEARCHLOG_MIN", zstd_safe::SEARCHLOG_MIN)?;
    module.add("SEARCHLOG_MAX", windowlog_max - 1)?;
    module.add("MINMATCH_MIN", zstd_sys::ZSTD_MINMATCH_MIN)?;
    module.add("MINMATCH_MAX", zstd_sys::ZSTD_MINMATCH_MAX)?;
    // TODO SEARCHLENGTH_* is deprecated.
    module.add("SEARCHLENGTH_MIN", zstd_sys::ZSTD_MINMATCH_MIN)?;
    module.add("SEARCHLENGTH_MAX", zstd_sys::ZSTD_MINMATCH_MAX)?;
    module.add("TARGETLENGTH_MIN", zstd_safe::TARGETLENGTH_MIN)?;
    module.add("TARGETLENGTH_MAX", zstd_safe::TARGETLENGTH_MAX)?;
    module.add("LDM_MINMATCH_MIN", zstd_safe::LDM_MINMATCH_MIN)?;
    module.add("LDM_MINMATCH_MAX", zstd_safe::LDM_MINMATCH_MAX)?;
    module.add("LDM_BUCKETSIZELOG_MAX", zstd_safe::LDM_BUCKETSIZELOG_MAX)?;

    module.add("STRATEGY_FAST", zstd_safe::Strategy::ZSTD_fast as u32)?;
    module.add("STRATEGY_DFAST", zstd_safe::Strategy::ZSTD_dfast as u32)?;
    module.add("STRATEGY_GREEDY", zstd_safe::Strategy::ZSTD_greedy as u32)?;
    module.add("STRATEGY_LAZY", zstd_safe::Strategy::ZSTD_lazy as u32)?;
    module.add("STRATEGY_LAZY2", zstd_safe::Strategy::ZSTD_lazy2 as u32)?;
    module.add("STRATEGY_BTLAZY2", zstd_safe::Strategy::ZSTD_btlazy2 as u32)?;
    module.add("STRATEGY_BTOPT", zstd_safe::Strategy::ZSTD_btopt as u32)?;
    module.add("STRATEGY_BTULTRA", zstd_safe::Strategy::ZSTD_btultra as u32)?;
    module.add(
        "STRATEGY_BTULTRA2",
        zstd_safe::Strategy::ZSTD_btultra2 as u32,
    )?;

    module.add(
        "DICT_TYPE_AUTO",
        zstd_sys::ZSTD_dictContentType_e::ZSTD_dct_auto as u32,
    )?;
    module.add(
        "DICT_TYPE_RAWCONTENT",
        zstd_sys::ZSTD_dictContentType_e::ZSTD_dct_rawContent as u32,
    )?;
    module.add(
        "DICT_TYPE_FULLDICT",
        zstd_sys::ZSTD_dictContentType_e::ZSTD_dct_fullDict as u32,
    )?;

    module.add("FORMAT_ZSTD1", zstd_sys::ZSTD_format_e::ZSTD_f_zstd1 as u32)?;
    module.add(
        "FORMAT_ZSTD1_MAGICLESS",
        zstd_sys::ZSTD_format_e::ZSTD_f_zstd1_magicless as u32,
    )?;

    Ok(())
}
