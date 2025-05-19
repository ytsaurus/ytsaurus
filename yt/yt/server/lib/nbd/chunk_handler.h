#pragma once

#include "public.h"

#include "block_device.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct IChunkHandler
    : public virtual TRefCounted
{
    virtual TFuture<void> Initialize() = 0;
    virtual TFuture<void> Finalize() = 0;
    virtual TFuture<TSharedRef> Read(i64 offset, i64 length, const TReadOptions& options) = 0;
    virtual TFuture<void> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkHandler)

////////////////////////////////////////////////////////////////////////////////

IChunkHandlerPtr CreateChunkHandler(
    TChunkBlockDeviceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr channel,
    std::optional<NChunkClient::TSessionId> sessionId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TSessionId GenerateSessionId(int mediumIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
