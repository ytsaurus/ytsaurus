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

    //! Total size of the page cache in bytes.
    //! Must be a positive multiple of PageSize.
    i64 Size;

    //! Period for background flush of dirty pages.
    //! When not set (nullopt), periodic flush is disabled.
    std::optional<TDuration> FlushPeriod;

    //! Maximum number of concurrent Write RPCs issued during a single flush.
    //! Dirty pages are flushed in batches of this size to avoid overwhelming
    //! the data node's request queue.
    int FlushBatchSize;

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
