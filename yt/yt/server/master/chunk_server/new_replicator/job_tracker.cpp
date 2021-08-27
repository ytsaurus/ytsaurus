#include "job_tracker.h"

#include "replicator_state.h"

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

class TJobTracker
    : public IJobTracker
{
public:
    explicit TJobTracker(IReplicatorStatePtr replicatorState)
        : ReplicatorState_(std::move(replicatorState))
    { }

    void ProcessJobHeartbeat(const TCtxJobHeartbeatPtr& context) override
    {
        // Node should be replicated to chunk thread, so SyncWithUpstream is required here.
        ReplicatorState_->SyncWithUpstream();

        Y_UNUSED(context);
    }

private:
    const IReplicatorStatePtr ReplicatorState_;
};

////////////////////////////////////////////////////////////////////////////////

IJobTrackerPtr CreateJobTracker(IReplicatorStatePtr replicatorState)
{
    return New<TJobTracker>(std::move(replicatorState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
