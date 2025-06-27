#pragma once

#include "job.h"
#include "helpers.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/library/gpu/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void InitAllocationProfiler(const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocationFinishReason,
    (Aborted)
    (Preempted)
    (MultipleJobsDisabled)
    (NoNewJobSettled)
    (AgentDisconnected)
    (JobFinishedUnsuccessfully)
);

class TAllocation
    : public NJobAgent::TResourceOwner
{
    class TEvent
    {
    public:
        void Fire() noexcept;
        bool Consume() noexcept;

    private:
        bool Fired_ = false;
    };

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
        NScheduler::TAllocationAttributes attributes,
        std::optional<NGpu::TNetworkPriority> networkPriority,
        TControllerAgentDescriptor agentDescriptor,
        IBootstrap* bootstrap);
    ~TAllocation();

    TAllocationId GetId() const noexcept;
    TOperationId GetOperationId() const noexcept;

    NScheduler::EAllocationState GetState() const noexcept;

    const TError& GetFinishError() const noexcept;

    int GetRequestedGpu() const noexcept;
    double GetRequestedCpu() const noexcept;
    i64 GetRequestedMemory() const noexcept;

    std::optional<NGpu::TNetworkPriority> GetNetworkPriority() const noexcept;

    void Start();
    void Cleanup();

    TJobPtr EvictJob() noexcept;
    const TJobPtr& GetJob() const;

    void UpdateControllerAgentDescriptor(TControllerAgentDescriptor agentDescriptor);
    const TControllerAgentDescriptor& GetControllerAgentDescriptor() const;

    NClusterNode::TJobResources GetResourceUsage(bool excludeReleasing = false) const noexcept;

    void Abort(TError error);
    void Complete(EAllocationFinishReason finishReason);
    void Preempt(
        TDuration timeout,
        TString preemptionReason,
        const std::optional<NScheduler::TPreemptedFor>& preemptedFor);

    bool IsResourceUsageOverdraftOccurred() const;

    bool IsEmpty() const noexcept;

    void OnResourcesAcquired() noexcept;

    const NJobAgent::TResourceHolderPtr& GetResourceHolder() const noexcept;

    NYTree::IYPathServicePtr GetOrchidService();

    bool IsRunning() const noexcept;
    bool IsFinished() const noexcept;

    void AbortJob(TError error, bool graceful, bool requestNewJob);
    void InterruptJob(NScheduler::EInterruptionReason interruptionReason, TDuration interruptionTimeout);

private:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    IBootstrap* const Bootstrap_;

    const TAllocationId Id_;
    const TOperationId OperationId_;

    NLogging::TLogger Logger;

    const int RequestedGpu_;
    const double RequestedCpu_;
    const i64 RequestedMemory_;

    const NClusterNode::TJobResources InitialResourceDemand_;

    NScheduler::TAllocationAttributes Attributes_;

    TControllerAgentAffiliationInfo ControllerAgentInfo_;

    const std::optional<NGpu::TNetworkPriority> NetworkPriority_;

    // TODO(pogorelov): Maybe strong ref?
    TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector> ControllerAgentConnector_;

    EAllocationState State_ = EAllocationState::Waiting;

    std::optional<TJobId> LastJobId_;

    TJobPtr Job_;

    bool Preempted_ = false;
    TError FinishError_;

    TEvent SettlementNewJobOnAbortRequested_;

    int TotalJobCount_ = 0;

    const TAllocationConfigPtr& GetConfig() const noexcept;

    void SettleJob(bool isJobFirst);

    void OnSettledJobReceived(
        const NProfiling::TWallTimer& timer,
        bool isJobFirst,
        TErrorOr<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>&& jobInfoOrError);

    void CreateAndSettleJob(
        TJobId jobId,
        NControllerAgent::NProto::TJobSpec&& jobSpec);

    void OnAllocationFinished(EAllocationFinishReason finishReason);

    void OnJobPrepared(TJobPtr job);
    void OnJobFinished(TJobPtr job);

    void TransferResourcesToJob();

    void PrepareAllocation();

    NYTree::IYPathServicePtr GetStaticOrchidService();

    friend void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TAllocationPtr& allocation);
};

DEFINE_REFCOUNTED_TYPE(TAllocation)

TAllocationPtr CreateAllocation(
    TAllocationId id,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceDemand,
    NScheduler::TAllocationAttributes attributes,
    std::optional<NGpu::TNetworkPriority> networkPriority,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap);

void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TAllocationPtr& allocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
