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
        const TString& jobTrackerAddress,
        const NJobAgent::TJobController::TReqHeartbeatPtr& request) final;
    void ProcessResponse(
        const TString& jobTrackerAddress,
        const NJobAgent::TJobController::TRspHeartbeatPtr& response) final;

    virtual void ScheduleHeartbeat(const NJobAgent::IJobPtr& job) final;

private:
    THashSet<NObjectClient::TJobId> JobIdsToConfirm_;
    // For converting vcpu to cpu back after getting response from scheduler.
    // It is needed because cpu_to_vcpu_factor can change between preparing request and processing response.
    double LastHeartbeatCpuToVCpuFactor_ = 1.0;

    void ReplaceCpuWithVCpu(NNodeTrackerClient::NProto::TNodeResources& resources) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
