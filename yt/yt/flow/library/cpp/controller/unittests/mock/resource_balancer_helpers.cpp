#include "resource_balancer_helpers.h"

namespace NYT::NFlow::NBalancer::NTesting {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! No-op storage handler for TFlowView initialization.

class TNoopStorageHandler : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPartitionId MakePartitionId(int n)
{
    return TPartitionId(TGuid::FromString(Format("%08x-%08x-%08x-%08x", 0, 0, 0, n)));
}

TJobId MakeUniqueJobId()
{
    static std::atomic<int> jobIdCounter{1};
    int n = jobIdCounter.fetch_add(1);
    return TJobId(TGuid::FromString(Format("%08x-%08x-%08x-%08x", 0, 0, 1, n)));
}

TComputationId MakeComputationId(const std::string& name)
{
    return TComputationId(name);
}

TResourceId MakeResourceId(const std::string& name)
{
    return TResourceId(name);
}

TWorkerGroupId MakeWorkerGroup(const std::string& name)
{
    return TWorkerGroupId(name);
}

TFlowViewPtr MakeEmptyFlowView()
{
    auto flowView = New<TFlowView>();

    auto storageHandler = New<TNoopStorageHandler>();
    auto control = New<TPersistedStateControl<std::string>>(storageHandler);
    flowView->State->AttachToControl(control);
    control->Recover();

    flowView->Feedback = New<TFlowFeedback>();

    auto pipelineSpec = New<TPipelineSpec>();
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    flowView->CurrentSpec->TrySetValue(pipelineSpec, TestVersionProvider());
    flowView->CurrentDynamicSpec->TrySetValue(dynamicSpec, TestVersionProvider());
    flowView->State->ExecutionSpec->PipelineSpec->TrySetValue(pipelineSpec, TestVersionProvider());
    flowView->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(dynamicSpec, TestVersionProvider());
    flowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(pipelineSpec), TestVersionProvider());

    return flowView;
}

TDynamicJobBalancerSpecPtr MakeBalancerSpec(
    double planningHorizonSeconds,
    double zeroQueueLatencySeconds)
{
    auto spec = New<TDynamicJobBalancerSpec>();
    spec->PlanningHorizon = TDuration::Seconds(static_cast<ui64>(planningHorizonSeconds));
    spec->ZeroQueueLatency = TDuration::Seconds(static_cast<ui64>(zeroQueueLatencySeconds));
    return spec;
}

void AddPartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const TComputationId& computationId,
    double rps,
    std::optional<TWorkerId> workerAddress)
{
    auto partition = New<TPartition>();
    partition->PartitionId = partitionId;
    partition->ComputationId = computationId;
    partition->State = EPartitionState::Executing;
    partition->StateTimestamp = TInstant::Now();

    flowView->State->StartMutation();
    flowView->State->ExecutionSpec->Layout->CreatePartition(partition);
    flowView->State->CommitMutation();

    if (workerAddress) {
        auto job = New<TJob>();
        job->JobId = MakeUniqueJobId();
        job->PartitionId = partitionId;
        job->WorkerAddress = *workerAddress;

        flowView->State->StartMutation();
        flowView->State->ExecutionSpec->Layout->CreateJob(job);
        flowView->State->CommitMutation();

        auto partitionJobStatus = New<TPartitionJobStatus>();
        partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
        partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);
        auto inputMetrics = New<TNodeInputMetrics>();
        inputMetrics->Global.MessagesPerSecond = rps;
        partitionJobStatus->CurrentJobStatus->InputMetrics = inputMetrics;
        flowView->Feedback->PartitionJobStatuses[partitionId] = partitionJobStatus;
    }
}

void SetWorkerResourceStatus(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId,
    double putRate,
    double fetchRate,
    double queueSize,
    double queueGrowthRate)
{
    auto& workerStatus = flowView->Feedback->WorkerStatuses[address];
    if (!workerStatus) {
        workerStatus = New<TWorkerStatus>();
    }
    auto resourceStatus = New<TWorkerResourceStatus>();
    resourceStatus->QueuePushRate10m = putRate;
    resourceStatus->QueueFetchRate10m = fetchRate;
    resourceStatus->QueueSize10m = queueSize;
    resourceStatus->QueueGrowthRate10m = queueGrowthRate;
    workerStatus->ResourceStatuses[resourceId] = resourceStatus;
}

void SetPreloadCompleted(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId)
{
    auto& workerStatus = flowView->Feedback->WorkerStatuses[address];
    if (!workerStatus) {
        workerStatus = New<TWorkerStatus>();
    }
    workerStatus->PreloadedResourceStates[resourceId] = EPreloadedResourceState::Preloaded;
}

void SetPreloadIssued(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId)
{
    flowView->State->StartMutation();
    auto workerSpec = New<TWorkerSpec>();
    workerSpec->PreloadResources.insert(resourceId);
    // Merge with existing if any.
    auto existingIt = flowView->State->ExecutionSpec->Layout->WorkerSpecs.find(address);
    if (existingIt != flowView->State->ExecutionSpec->Layout->WorkerSpecs.end()) {
        for (const auto& r : existingIt->second->PreloadResources) {
            workerSpec->PreloadResources.insert(r);
        }
    }
    flowView->State->ExecutionSpec->Layout->WorkerSpecs.insert_or_assign(address, workerSpec);
    flowView->State->CommitMutation();
}

void ClearPreloadIssued(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId)
{
    flowView->State->StartMutation();
    auto& workerSpecs = flowView->State->ExecutionSpec->Layout->WorkerSpecs;
    auto it = workerSpecs.find(address);
    if (it != workerSpecs.end()) {
        auto workerSpec = New<TWorkerSpec>();
        for (const auto& r : it->second->PreloadResources) {
            if (r != resourceId) {
                workerSpec->PreloadResources.insert(r);
            }
        }
        workerSpecs.insert_or_assign(address, workerSpec);
    }
    flowView->State->CommitMutation();
}

THashMap<TPartitionId, TWorkerId> GetAddActions(const TRebalanceResult& result)
{
    THashMap<TPartitionId, TWorkerId> adds;
    for (const auto& action : result.Actions) {
        if (action.Type == ERebalanceActionType::Add) {
            adds[action.PartitionId] = action.WorkerAddress;
        }
    }
    return adds;
}

THashSet<TPartitionId> GetDelActions(const TRebalanceResult& result)
{
    THashSet<TPartitionId> dels;
    for (const auto& action : result.Actions) {
        if (action.Type == ERebalanceActionType::Del) {
            dels.insert(action.PartitionId);
        }
    }
    return dels;
}

std::vector<TWorkerPreloadResultAction> GetPreloadAddActions(const TRebalanceResult& result)
{
    std::vector<TWorkerPreloadResultAction> adds;
    for (const auto& action : result.PreloadResourceActions) {
        if (action.Type == ERebalanceActionType::Add) {
            adds.push_back(action);
        }
    }
    return adds;
}

std::vector<TWorkerPreloadResultAction> GetPreloadDelActions(const TRebalanceResult& result)
{
    std::vector<TWorkerPreloadResultAction> dels;
    for (const auto& action : result.PreloadResourceActions) {
        if (action.Type == ERebalanceActionType::Del) {
            dels.push_back(action);
        }
    }
    return dels;
}

TResourceSpecPtr MakeResourceSpec(
    THashMap<std::string, ssize_t> requiredCaps,
    bool preloadRequired)
{
    auto spec = New<TResourceSpec>();
    spec->RequiredCapabilities = std::move(requiredCaps);
    spec->PreloadRequired = preloadRequired;
    return spec;
}

TComputationSpecPtr MakeComputationSpec(
    const TWorkerGroupId& workerGroup,
    const std::vector<TResourceId>& resourceIds)
{
    auto spec = New<TComputationSpec>();
    spec->WorkerGroup = workerGroup;
    for (const auto& resourceId : resourceIds) {
        spec->RequiredResourceIds[resourceId] = New<TResourceDescription>();
    }
    return spec;
}

void AddWorker(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TWorkerGroupId& group,
    THashMap<std::string, ssize_t> capabilities)
{
    auto worker = New<TWorker>();
    worker->RpcAddress = address;
    worker->Groups = {group};
    worker->Capabilities = std::move(capabilities);
    flowView->State->Workers[address] = worker;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer::NTesting
