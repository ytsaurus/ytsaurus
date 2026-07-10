#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateChunkBlockDevice(
    std::string exportId,
    TChunkBlockDeviceConfigPtr config,
    NConcurrency::IThroughputThrottlerPtr readThrottler,
    NConcurrency::IThroughputThrottlerPtr writeThrottler,
    IInvokerPtr invoker,
    NRpc::IChannelPtr channel,
    NChunkClient::TSessionId sessionId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
