#pragma once

#include "public.h"

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct IJobTracker
    : public TRefCounted
{
    using TCtxJobHeartbeat = NRpc::TTypedServiceContext<
        NJobTrackerClient::NProto::TReqHeartbeat,
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TCtxJobHeartbeatPtr = TIntrusivePtr<TCtxJobHeartbeat>;

    virtual void ProcessJobHeartbeat(const TCtxJobHeartbeatPtr& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobTracker)

////////////////////////////////////////////////////////////////////////////////

IJobTrackerPtr CreateJobTracker(IReplicatorStatePtr replicatorState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
