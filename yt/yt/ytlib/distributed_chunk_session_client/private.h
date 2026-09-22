#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/public.h>

#include <limits>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

using TCreateControllerCallback = TCallback<IDistributedChunkSessionControllerPtr()>;
using TSendChunkSealRequestCallback = TCallback<TFuture<void>(NChunkClient::TChunkId)>;

struct TDistributedChunkSessionPoolTestingOptions
{
    TCreateControllerCallback CreateController;
    TSendChunkSealRequestCallback SendChunkSealRequest;
    IDistributedChunkSessionSealMonitorPtr SealMonitor;
    int SlotCount = std::numeric_limits<int>::max();
};

IDistributedChunkSessionPoolPtr CreateDistributedChunkSessionPoolForTesting(
    TDistributedChunkSessionPoolConfigPtr config,
    TDistributedChunkSessionPoolTestingOptions options,
    IInvokerPtr invoker,
    NLogging::TLogger logger = DistributedChunkSessionLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
