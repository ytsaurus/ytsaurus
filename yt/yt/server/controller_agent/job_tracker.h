#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////

class TJobTracker
    : public TRefCounted
{
public:
    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NProto::TReqHeartbeat,
        NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;

    explicit TJobTracker(TBootstrap* bootstrap);

    void ProcessHeartbeat(const TCtxHeartbeatPtr& context);

private:
    TBootstrap* const Bootstrap_;
    NProfiling::TCounter HeartbeatStatisticsBytes_;
    NProfiling::TCounter HeartbeatDataStatisticsBytes_;
    NProfiling::TCounter HeartbeatJobResultBytes_;
    NProfiling::TCounter HeartbeatProtoMessageBytes_;
    NProfiling::TGauge HeartbeatEnqueuedControllerEvents_;
    std::atomic<i64> EnqueuedControllerEventCount_ = 0;
    NProfiling::TCounter HeartbeatCount_;

    void ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request);
    void AccountEnqueuedControllerEvent(int delta);
};

DEFINE_REFCOUNTED_TYPE(TJobTracker)

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
