#include "chunk_replica_allocator.h"

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaAllocator
    : public IChunkReplicaAllocator
{
public:
    explicit TChunkReplicaAllocator(IReplicatorStatePtr replicatorState)
        : ReplicatorState_(std::move(replicatorState))
    {
        Y_UNUSED(ReplicatorState_);
    }

    TNodePtrWithIndexesList AllocateReplicas(const TReplicaAllocationRequest& request) override
    {
        // NB: Stale dual cluster state is OK here, so SyncWithUpstream is not required.

        Y_UNUSED(request);
        YT_UNIMPLEMENTED();
    }

private:
    const IReplicatorStatePtr ReplicatorState_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaAllocatorPtr CreateChunkReplicaAllocator(IReplicatorStatePtr replicatorState)
{
    return New<TChunkReplicaAllocator>(std::move(replicatorState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
