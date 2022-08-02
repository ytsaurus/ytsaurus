#pragma once

#include <yt/yt/server/node/job_agent/job_controller.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TMasterJobHeartbeatProcessor
    : public NJobAgent::TJobController::TJobHeartbeatProcessorBase
{
    using TJobHeartbeatProcessorBase::TJobHeartbeatProcessorBase;

    void PrepareRequest(
        NObjectClient::TCellTag cellTag,
        const TString& jobTrackerAddress,
        const NJobAgent::TJobController::TReqHeartbeatPtr& request) final;
    void ProcessResponse(
        const TString& jobTrackerAddress,
        const NJobAgent::TJobController::TRspHeartbeatPtr& response) final;

    virtual void ScheduleHeartbeat(const NJobAgent::IJobPtr& job) final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
