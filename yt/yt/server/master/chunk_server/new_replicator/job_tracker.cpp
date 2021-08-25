#include "job_tracker.h"

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

class TJobTracker
    : public IJobTracker
{
public:
    explicit TJobTracker(IReplicatorStatePtr replicatorState)
        : ReplicatorState_(std::move(replicatorState))
    { }

    virtual void ProcessJobHeartbeat(const TCtxJobHeartbeatPtr& context) override
    {
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
