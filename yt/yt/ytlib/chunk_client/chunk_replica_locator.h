#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaLocator
    : public TRefCounted
{
public:
    TChunkReplicaLocator(
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        TChunkId chunkId,
        TDuration expirationTime,
        const TChunkReplicaList& initialReplicas,
        NLogging::TLogger logger);

    TFuture<TAllyReplicasInfo> GetReplicasFuture();
    void DiscardReplicas(const TFuture<TAllyReplicasInfo>& future);

    TFuture<TAllyReplicasInfo> MaybeResetReplicas(
        const TAllyReplicasInfo& candidateReplicas,
        const TFuture<TAllyReplicasInfo>& future);

    DEFINE_SIGNAL(void(const TAllyReplicasInfo& replicas), ReplicasLocated);

private:
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TChunkId ChunkId_;
    const TDuration ExpirationTime_;
    const NRpc::IChannelPtr Channel_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    TInstant Timestamp_;
    TPromise<TAllyReplicasInfo> ReplicasPromise_;

    void LocateChunk();
    void OnChunkLocated(const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicaLocator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
