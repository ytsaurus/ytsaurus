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

    //! Period for background flush of dirty pages.
    //! When not set (nullopt), periodic flush is disabled.
    std::optional<TDuration> FlushPeriod;

    //! Amount of dirty data (in bytes) that a single background or periodic flush
    //! attempts to write out, expressed in bytes and used internally as a page count
    //! (MaxDirtyDataPerFlush / PageSize). This value plays three roles:
    //!   - the granularity of one WriteBatch task (RPC) when a flush is split;
    //!   - the dirty-data watermark that triggers a background flush after a write;
    //!   - the per-invocation budget of background/periodic flushes.
    //! Must be a positive multiple of PageSize.
    i64 MaxDirtyDataPerFlush;

    //! Upper bound in bytes on the size of a single merged WriteBatch subrequest.
    //! A run of adjacent dirty pages larger than this is split into several
    //! subrequests so one long contiguous dirty region does not become a single
    //! unbounded allocation and RPC payload.
    //! Must be a positive multiple of PageSize.
    i64 MaxDirtyDataPerWrite;

    //! Maximum number of WriteBatch RPCs that may be in flight concurrently
    //! during a single flush. Batches are issued in a sliding window of this
    //! width instead of strictly one-at-a-time, reducing flush wall-time.
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
