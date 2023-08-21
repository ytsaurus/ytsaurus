#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkReplicaCache
    : public virtual TRefCounted
{
public:
    virtual std::vector<TErrorOr<TAllyReplicasInfo>> FindReplicas(
        const std::vector<TChunkId>& chunkIds) = 0;

    virtual std::vector<TFuture<TAllyReplicasInfo>> GetReplicas(
        const std::vector<TChunkId>& chunkIds) = 0;

    virtual void DiscardReplicas(
        TChunkId chunkId,
        const TFuture<TAllyReplicasInfo>& future) = 0;

    virtual void UpdateReplicas(
        TChunkId chunkId,
        const TAllyReplicasInfo& replicas) = 0;

    virtual void RegisterReplicas(
        TChunkId chunkId,
        const NChunkClient::TChunkReplicaWithMediumList& replicas) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaCache)

IChunkReplicaCachePtr CreateChunkReplicaCache(NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
