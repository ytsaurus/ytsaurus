#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <util/generic/string.h>

#include <util/system/types.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct INbdChunkHandler
    : public virtual TRefCounted
{
    virtual TFuture<void> Create() = 0;

    virtual TFuture<void> Destroy() = 0;

    virtual TFuture<NChunkClient::TBlock> Read(i64 offset, i64 length) = 0;

    virtual TFuture<NIO::TIOCounters> Write(i64 offset, const NChunkClient::TBlock& block) = 0;
};

DEFINE_REFCOUNTED_TYPE(INbdChunkHandler)

////////////////////////////////////////////////////////////////////////////////

INbdChunkHandlerPtr CreateNbdChunkHandler(
    i64 chunkSize,
    TChunkId chunkId,
    TWorkloadDescriptor workloadDescriptor,
    TStoreLocationPtr storeLocation,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
