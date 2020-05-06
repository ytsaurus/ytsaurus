#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/actions/signal.h>
#include <yt/core/actions/future.h>

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
        TChunkReplicaList initialReplicas,
        NLogging::TLogger logger);

    TFuture<TChunkReplicaList> GetReplicas();
    void DiscardReplicas(const TFuture<TChunkReplicaList>& future);

    DEFINE_SIGNAL(void(const TChunkReplicaList& replicas), ReplicasLocated);

private:
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TChunkId ChunkId_;
    const TDuration ExpirationTime_;
    const NRpc::IChannelPtr Channel_;
    const NLogging::TLogger Logger;

    TSpinLock Lock_;
    TInstant Timestamp_;
    TPromise<TChunkReplicaList> ReplicasPromise_;

    void LocateChunk();
    void OnChunkLocated(const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicaLocator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
