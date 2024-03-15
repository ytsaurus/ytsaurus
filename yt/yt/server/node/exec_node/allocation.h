#pragma once

#include "job.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TAllocation
    : public NJobAgent::TResourceHolder
{
public:
    // TODO(pogorelov): Implement one-shot signals and use it here.
    DEFINE_SIGNAL(void(TAllocationPtr allocation, TDuration waitingForResourcesTimeout), AllocationPrepared);
    DEFINE_SIGNAL(void(TAllocationPtr allocation), AllocationFinished);

    DEFINE_SIGNAL(void(TJobPtr job), JobSettled);
    DEFINE_SIGNAL(void(TJobPtr job), JobPrepared);
    DEFINE_SIGNAL(void(TJobPtr job), JobFinished);

public:
    TAllocation(
        TAllocationId id,
        TOperationId operationId,
        const NClusterNode::TJobResources& resourceUsage,
        TControllerAgentDescriptor agentDescriptor,
        IBootstrap* bootstrap);
    ~TAllocation();

    TAllocationId GetId() const noexcept;
    TGuid GetIdAsGuid() const noexcept override;
    TOperationId GetOperationId() const noexcept;

    NScheduler::EAllocationState GetState() const noexcept;

    const TError& GetFinishError() const noexcept;

    int GetRequestedGpu() const noexcept;
    double GetRequestedCpu() const noexcept;
    i64 GetRequestedMemory() const noexcept;

    void Start();
    void Cleanup();

    TJobPtr EvictJob();
    const TJobPtr& GetJob() const;

    void UpdateControllerAgentDescriptor(TControllerAgentDescriptor agentDescriptor);
    const TControllerAgentDescriptor& GetControllerAgentDescriptor() const;

    NClusterNode::TJobResources GetResourceUsage() const noexcept;

    const NClusterNode::ISlotPtr& GetUserSlot() const noexcept;
    const std::vector<NClusterNode::ISlotPtr>& GetGpuSlots() const noexcept;

    void Abort(TError error);
    void Complete();
    void Preempt(
        TDuration timeout,
        TString preemptionReason,
        const std::optional<NScheduler::TPreemptedFor>& preemptedFor);

    bool IsResourceUsageOverdrafted() const;

    bool IsEmpty() const noexcept;

private:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    IBootstrap* const Bootstrap_;

    const TAllocationId Id_;
    const TOperationId OperationId_;

    const int RequestedGpu_;
    const double RequestedCpu_;
    const i64 RequestedMemory_;

    TControllerAgentDescriptor ControllerAgentDescriptor_;
    // TODO before commit: maybe strong?
    TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector> ControllerAgentConnector_;

    EAllocationState State_ = EAllocationState::Waiting;

    TJobPtr Job_;

    bool Preempted_ = false;
    TError FinishError_;

    void SettleJob();

    void OnSettledJobReceived(
        TErrorOr<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>&& jobInfoOrError);

    void CreateAndSettleJob(
        TJobId jobId,
        NControllerAgent::NProto::TJobSpec&& jobSpec);

    void OnResourcesAcquired() noexcept final;

    void OnAllocationFinished();

    void OnJobPrepared(TJobPtr job);
    void OnJobFinished(TJobPtr job);

    friend void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TAllocationPtr& allocation);
};

DEFINE_REFCOUNTED_TYPE(TAllocation)

TAllocationPtr CreateAllocation(
    TAllocationId id,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceUsage,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap);

void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TAllocationPtr& allocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
