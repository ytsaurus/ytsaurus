#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>


namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! TODO(achulkov2): Comment.
struct IChunkReplicaDestroyer
    : public TRefCounted
{
    virtual TFuture<void> Destroy() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaDestroyer)

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaDestroyerPtr CreateChunkReplicaDestroyer(
    const TChunkId& chunkId,
    const TChunkReplicaWithMedium& chunkReplica,
    const TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
