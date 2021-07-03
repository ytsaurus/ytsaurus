#pragma once

#include <yt/yt/server/node/job_agent/job_controller.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerJobHeartbeatProcessor
    : public NJobAgent::TJobController::TJobHeartbeatProcessorBase
{
    using TJobHeartbeatProcessorBase::TJobHeartbeatProcessorBase;

    virtual void PrepareRequest(
        NObjectClient::TCellTag cellTag,
        const NJobAgent::TJobController::TReqHeartbeatPtr& request) final;
    virtual void ProcessResponse(
        const NJobAgent::TJobController::TRspHeartbeatPtr& response) final;

    THashSet<NObjectClient::TJobId> JobIdsToConfirm_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
