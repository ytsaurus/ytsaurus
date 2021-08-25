#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/chunk_replica.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaAllocationRequest
{
    TChunkId ChunkId;

    int MediumIndex;

    int DesiredReplicaCount;
    int MinReplicaCount;

    std::vector<TString> ForbiddenAddresses;

    std::optional<TString> PreferredHostName;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkReplicaAllocator
    : public TRefCounted
{
    virtual TNodePtrWithIndexesList AllocateReplicas(const TReplicaAllocationRequest& request) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaAllocator)

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaAllocatorPtr CreateChunkReplicaAllocator(IReplicatorStatePtr replicatorState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
