#pragma once

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/controller/job_balancer_resource_queue.h>
#include <yt/yt/flow/library/cpp/controller/job_balancer_result.h>

namespace NYT::NFlow::NBalancer::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Helpers for the resource_queue balancer tests: build a minimal TFlowView from
//! scratch and read the balancer result. The view consists of:
//!   - FlowView->State->Workers                            — worker registry
//!   - FlowView->State->ExecutionSpec->Layout->Partitions  — partition registry
//!   - FlowView->State->ExecutionSpec->Layout->Jobs        — job registry (partition→worker)
//!   - FlowView->State->ExecutionSpec->Layout->WorkerSpecs — preload state issued by controller
//!   - FlowView->CurrentSpec                               — pipeline spec (computations + resources)
//!   - FlowView->Feedback->WorkerStatuses                  — per-worker resource queue stats
//!   - FlowView->Feedback->PartitionJobStatuses            — per-partition Rps

using TWorkerId = std::string;

//! ID helpers.

TPartitionId MakePartitionId(int n);

TJobId MakeUniqueJobId();

TComputationId MakeComputationId(const std::string& name);

TResourceId MakeResourceId(const std::string& name);

TWorkerGroupId MakeWorkerGroup(const std::string& name = "default");

//! FlowView builder.

TFlowViewPtr MakeEmptyFlowView();

//! Spec builders.

TResourceSpecPtr MakeResourceSpec(
    THashMap<std::string, ssize_t> requiredCaps = {},
    bool preloadRequired = false);

TComputationSpecPtr MakeComputationSpec(
    const TWorkerGroupId& workerGroup,
    const std::vector<TResourceId>& resourceIds = {});

TDynamicJobBalancerSpecPtr MakeBalancerSpec(
    double planningHorizonSeconds = 60.0,
    double zeroQueueLatencySeconds = 1.0);

//! Worker builder.

void AddWorker(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TWorkerGroupId& group,
    THashMap<std::string, ssize_t> capabilities = {});

//! Partition + Job builders.

void AddPartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const TComputationId& computationId,
    double rps = 1.0,
    std::optional<TWorkerId> workerAddress = std::nullopt);

//! Worker resource status.

void SetWorkerResourceStatus(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId,
    double putRate,
    double fetchRate,
    double queueSize = 0.0,
    double queueGrowthRate = 0.0);

//! Preload state helpers.

void SetPreloadCompleted(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId);

void SetPreloadIssued(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId);

//! Reverts SetPreloadIssued for one resource.
void ClearPreloadIssued(
    const TFlowViewPtr& flowView,
    const TWorkerId& address,
    const TResourceId& resourceId);

//! Result helpers.

//! Returns map: partitionId → workerAddress for Add actions.
THashMap<TPartitionId, TWorkerId> GetAddActions(const TRebalanceResult& result);

//! Returns set of partitionIds from Del actions.
THashSet<TPartitionId> GetDelActions(const TRebalanceResult& result);

//! Returns preload actions as vector for inspection.
std::vector<TWorkerPreloadResultAction> GetPreloadAddActions(const TRebalanceResult& result);

//! Returns preload Del actions as vector for inspection.
std::vector<TWorkerPreloadResultAction> GetPreloadDelActions(const TRebalanceResult& result);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer::NTesting
