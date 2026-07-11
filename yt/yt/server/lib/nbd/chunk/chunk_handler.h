#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

//! A single sub-request for WriteBatch and ReadBatch.
struct TReadBatchSubrequest
{
    i64 Offset;
    i64 Length;
};

struct TWriteBatchSubrequest
{
    i64 Offset;
    TSharedRef Data;
};

struct IChunkHandler
    : public virtual TRefCounted
{
    virtual TFuture<void> Initialize() = 0;
    virtual TFuture<void> Finalize() = 0;
    virtual TFuture<void> Flush() = 0;
    virtual TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) = 0;
    virtual TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) = 0;

    //! Read multiple non-contiguous ranges in a single RPC round-trip.
    virtual TFuture<std::vector<TReadResponse>> ReadBatch(
        const std::vector<TReadBatchSubrequest>& subrequests,
        const TReadOptions& options) = 0;

    //! Write multiple non-contiguous ranges in a single RPC round-trip.
    virtual TFuture<TWriteResponse> WriteBatch(
        const std::vector<TWriteBatchSubrequest>& subrequests,
        const TWriteOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkHandler)

////////////////////////////////////////////////////////////////////////////////

IChunkHandlerPtr CreateChunkHandler(
    IBlockDevice* blockDevice,
    TChunkBlockDeviceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr channel,
    NChunkClient::TSessionId sessionId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TSessionId GenerateSessionId(int mediumIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
