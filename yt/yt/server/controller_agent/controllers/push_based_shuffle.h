#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/push_based_shuffle_chunk_pool.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/public.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct TPushBasedShuffleParameters
{
    TIncarnationId IncarnationId;
    TOperationId OperationId;
    int PartitionCount = 0;
    NObjectClient::TTransactionId TransactionId;

    NScheduler::TSortOperationSpecBasePtr Spec;
    TPushBasedShuffleOptionsPtr PushBasedShuffleOptions;
};

////////////////////////////////////////////////////////////////////////////////

struct IPushBasedShuffle
    : public virtual TRefCounted
{
    virtual NChunkPools::IPushBasedShuffleChunkPoolPtr GetChunkPool() const = 0;

    virtual std::vector<NDistributedChunkSessionClient::TReadySession> GetReadySessions() const = 0;

    virtual void FinalizeSessions() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPushBasedShuffle)

////////////////////////////////////////////////////////////////////////////////

IPushBasedShufflePtr CreatePushBasedShuffle(
    TPushBasedShuffleParameters parameters,
    TPushBasedShuffleRegistryPtr shuffleRegistry,
    NDistributedChunkSessionClient::IDistributedChunkSessionSealMonitorPtr sealMonitor,
    NApi::NNative::IClientPtr client,
    IInvokerPtr controllerInvoker,
    TCallback<void(std::function<void()>)> invokeSafely,
    TCallback<void(const TError&)> onSessionFailed,
    TCallback<void(int)> onChunkPoolUpdated,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
