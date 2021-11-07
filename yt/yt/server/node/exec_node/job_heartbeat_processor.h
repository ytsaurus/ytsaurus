#pragma once

#include <yt/yt/server/node/job_agent/job_controller.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerJobHeartbeatProcessor
    : public NJobAgent::TJobController::TJobHeartbeatProcessorBase
{
public:
    using TJobHeartbeatProcessorBase::TJobHeartbeatProcessorBase;

    void PrepareRequest(
        NObjectClient::TCellTag cellTag,
        const NJobAgent::TJobController::TReqHeartbeatPtr& request) final;
    void ProcessResponse(
        const NJobAgent::TJobController::TRspHeartbeatPtr& response) final;

    virtual void ScheduleHeartbeat(NJobTrackerClient::TJobId jobId) final;

private:
    THashSet<NObjectClient::TJobId> JobIdsToConfirm_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
