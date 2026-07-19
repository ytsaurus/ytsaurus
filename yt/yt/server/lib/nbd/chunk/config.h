#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/config.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

//! Configuration for the write-back page cache of TChunkBlockDevice.
struct TPageCacheConfig
    : public NYTree::TYsonStruct
{
    //! Page size in bytes.
    //! Must be a positive multiple of 4096.
    i64 PageSize;

    //! Total capacity of the page cache in bytes.
    //! Must be a positive multiple of PageSize.
    i64 Capacity;

    //! Period for periodic background writeback of dirty pages.
    //! When not set (nullopt), periodic writeback is disabled.
    //! This does not imply issuing NBD_CMD_FLUSH; it only writes dirty pages
    //! to the backend with WRITE requests.
    std::optional<TDuration> WritebackPeriod;

    //! Fraction of Capacity that triggers asynchronous writeback when dirty data
    //! reaches it. Must be in (0, 1] and <= DirtyDataHardLimitCapacityFraction.
    //! The effective byte limit is rounded down to a multiple of PageSize.
    double DirtyDataSoftLimitCapacityFraction;

    //! Fraction of Capacity that throttles writes and performs synchronous writeback
    //! when dirty data reaches it. Must be in (0, 1].
    //! The effective byte limit is rounded down to a multiple of PageSize.
    double DirtyDataHardLimitCapacityFraction;

    //! Fraction of Capacity that one background/periodic writeback invocation attempts
    //! to write. This is a per-run budget. Must be in (0, 1].
    //! The effective byte budget is rounded down to a multiple of PageSize.
    double MaxDirtyDataPerWritebackCapacityFraction;

    //! Upper bound in bytes on a single merged WRITE request payload.
    //! A run of adjacent dirty pages larger than this is split into several WRITE
    //! requests so one long contiguous dirty region does not become a single
    //! unbounded allocation/RPC payload.
    //! Must be a positive multiple of PageSize.
    i64 MaxDirtyDataPerWrite;

    //! Maximum number of WRITE requests that may be in flight concurrently during
    //! one writeback run. Requests are issued in a sliding window of this width.
    //! Must be positive.
    int MaxInflightWriteRequests;

    REGISTER_YSON_STRUCT(TPageCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPageCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    i64 Size;
    int MediumIndex;
    EFilesystemType FsType;
    TDuration KeepSessionAlivePeriod;
    TDuration DataNodeNbdServiceRpcTimeout;
    //! Time to create chunk and make filesystem in it.
    TDuration DataNodeNbdServiceMakeTimeout;
    //! Number of TCP connections to use for NBD RPC requests.
    int MultiplexingParallelism;

    //! Write-back page cache configuration.
    //! When null, page cache is disabled.
    TPageCacheConfigPtr PageCache;

    REGISTER_YSON_STRUCT(TChunkBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
