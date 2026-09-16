#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <util/generic/string.h>

#include <util/system/types.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TNbdReadSubrequest
{
    i64 Offset;
    i64 Length;
};

struct INbdChunkHandler
    : public virtual TRefCounted
{
    virtual TFuture<void> Create() = 0;

    virtual TFuture<void> Destroy() = 0;

    virtual TFuture<NChunkClient::TBlock> Read(i64 offset, i64 length, ui64 cookie) = 0;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBatch(
        const std::vector<TNbdReadSubrequest>& subrequests,
        ui64 cookie) = 0;

    virtual TFuture<NIO::TIOCounters> Write(i64 offset, const NChunkClient::TBlock& block, ui64 cookie) = 0;

    //! Flush dirty data to disk (fsync).
    virtual TFuture<void> Flush(ui64 cookie) = 0;

    //! Flush a specific range of data to disk (sync_file_range).
    virtual TFuture<void> FlushRange(i64 offset, i64 size) = 0;
};

DEFINE_REFCOUNTED_TYPE(INbdChunkHandler)

////////////////////////////////////////////////////////////////////////////////

INbdChunkHandlerPtr CreateNbdChunkHandler(
    i64 chunkSize,
    TChunkId chunkId,
    TWorkloadDescriptor workloadDescriptor,
    TStoreLocationPtr storeLocation,
    IInvokerPtr ioInvoker,
    NConcurrency::IThroughputThrottlerPtr readNetThrottler,
    NConcurrency::IThroughputThrottlerPtr writeNetThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
