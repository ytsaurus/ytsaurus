#include "private.h"

#include "job_balancer_common.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

bool ComputationBelongsToGroup(const TComputationSpecPtr& computationSpec, const TWorkerGroupId& workerGroup)
{
    return computationSpec->WorkerGroup == workerGroup;
}

//! Length of the worker's long metrics window; the switch from a partition's history to its new
//! job's own metrics takes one more such window after the 10-minute rate appears.
static constexpr TDuration JobMetricsLongWindow = TDuration::Minutes(10);
//! Workers of a restarted pipeline register over a minute or so; a job placed on the first of them
//! has invested nothing worth protecting for this long.
static constexpr TDuration FreshJobAge = TDuration::Minutes(2);

bool AreJobMetricsSteady(const TJobStatusPtr& status)
{
    // A first iteration longer than the window produces a 10-minute rate of initialization work.
    return status && status->PerformanceMetrics && status->PerformanceMetrics->CpuUsage10m &&
        !status->PerformanceMetrics->MetricsSteadyPending.value_or(false);
}

double GetJobMetricsMaturity(const TJobStatusPtr& status, TInstant now)
{
    if (!AreJobMetricsSteady(status)) {
        return 0.;
    }
    const auto& metrics = status->PerformanceMetrics;
    if (!metrics->MetricsStartTime) {
        // An older worker: nothing to blend against, switch immediately.
        return 1.;
    }
    auto rateAppearedAt = *metrics->MetricsStartTime + JobMetricsLongWindow;
    return std::clamp((now - rateAppearedAt).SecondsFloat() / JobMetricsLongWindow.SecondsFloat(), 0., 1.);
}

bool IsJobFreshlyStarted(const TJobStatusPtr& status, TInstant now)
{
    return status && status->StartTime != TInstant::Zero() && now - status->StartTime < FreshJobAge;
}

bool IsPartitionMovable(const TFlowViewPtr& flowView, const TPartitionId& partitionId, bool warmupProtectionActive)
{
    if (!warmupProtectionActive) {
        return true;
    }
    auto* partitionPtr = flowView->State->ExecutionSpec->Layout->Partitions.FindPtr(partitionId);
    if (!partitionPtr || !(*partitionPtr)->CurrentJobId) {
        return true;
    }
    // The controller drops the status whenever the partition's job changes, so this status is the current job's.
    const auto& status = flowView->Feedback->GetCurrentJobStatus(partitionId);
    auto now = TInstant::Now();
    return IsJobFreshlyStarted(status, now) || GetJobMetricsMaturity(status, now) >= 1.;
}

double GetIdleWorkerShare(const TFlowViewPtr& flowView, const TWorkerGroupId& workerGroup)
{
    const auto& workers = flowView->State->Workers;
    THashSet<std::string> groupWorkers;
    for (const auto& [address, worker] : workers) {
        if (WorkerBelongsToGroup(worker, workerGroup)) {
            groupWorkers.insert(address);
        }
    }
    if (groupWorkers.empty()) {
        return 0.;
    }
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& computations = flowView->CurrentSpec->GetValue()->Computations;
    THashSet<std::string> busyWorkers;
    for (const auto& [jobId, job] : layout->Jobs) {
        if (!groupWorkers.contains(job->WorkerAddress) || busyWorkers.contains(job->WorkerAddress)) {
            continue;
        }
        auto* partitionPtr = layout->Partitions.FindPtr(job->PartitionId);
        if (!partitionPtr) {
            continue;
        }
        auto* computationSpec = computations.FindPtr((*partitionPtr)->ComputationId);
        if (computationSpec && ComputationBelongsToGroup(*computationSpec, workerGroup)) {
            busyWorkers.insert(job->WorkerAddress);
        }
    }
    return 1. - static_cast<double>(busyWorkers.size()) / groupWorkers.size();
}

bool IsWarmupProtectionActive(const TFlowViewPtr& flowView, const TDynamicJobBalancerSpecPtr& balancerSpec, const TWorkerGroupId& workerGroup)
{
    if (!balancerSpec->BalanceWarmupProtection) {
        return false;
    }
    return GetIdleWorkerShare(flowView, workerGroup) < balancerSpec->BalanceWarmupIdleWorkerShare;
}

void RemoveJobKeepingMetrics(const TFlowViewPtr& flowView, const TJobId& jobId, EJobFinishReason jobFinishReason)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    auto job = GetOrCrash(layout->Jobs, jobId);
    const auto& status = flowView->Feedback->GetCurrentJobStatus(job->PartitionId);
    // Steady but immature metrics still carry the tail of the initialization in their windows.
    if (jobFinishReason != EJobFinishReason::Stopped && GetJobMetricsMaturity(status, TInstant::Now()) >= 1.) {
        const auto& metrics = status->PerformanceMetrics;
        auto history = New<TPartitionMetricsHistory>();
        history->WorkerAddress = job->WorkerAddress;
        history->CpuUsage = *metrics->CpuUsage10m;
        history->MessagesPerSecond = metrics->MessagesPerSecond10m;
        history->FlowCoreVersion = metrics->FlowCoreVersion;
        history->PipelineSpecVersion = metrics->PipelineSpecVersion;
        StorePartitionHistory(flowView, job->PartitionId, std::move(history));
    }
    layout->RemoveJob(jobId, jobFinishReason);
}

void StorePartitionHistory(const TFlowViewPtr& flowView, const TPartitionId& partitionId, TPartitionMetricsHistoryPtr history)
{
    auto& histories = flowView->State->BalancerState->PartitionHistories;
    auto limit = flowView->CurrentDynamicSpec->GetValue()->JobManager->PartitionHistoryLimit;
    if (!histories.contains(partitionId) && std::ssize(histories) >= limit) {
        // The histories live in one persisted document that has a size limit: keep the heaviest
        // partitions, the light ones weigh about the same as the computation average anyway.
        auto lightest = histories.end();
        for (auto it = histories.begin(); it != histories.end(); ++it) {
            if (lightest == histories.end() || it->second->CpuUsage < lightest->second->CpuUsage) {
                lightest = it;
            }
        }
        if (lightest == histories.end() || lightest->second->CpuUsage >= history->CpuUsage) {
            return;
        }
        histories.erase(lightest);
    }
    histories[partitionId] = std::move(history);
}

void PrunePartitionHistories(const TFlowViewPtr& flowView, const TDynamicJobBalancerSpecPtr& balancerSpec, const TWorkerGroupId& workerGroup)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& computations = flowView->CurrentSpec->GetValue()->Computations;
    const auto now = TInstant::Now();
    EraseNodesIf(flowView->State->BalancerState->PartitionHistories, [&] (const auto& item) {
        auto* partitionPtr = layout->Partitions.FindPtr(item.first);
        if (!partitionPtr) {
            return true;
        }
        const auto& partition = *partitionPtr;
        auto* computationSpec = computations.FindPtr(partition->ComputationId);
        if (!computationSpec) {
            return true;
        }
        if (!ComputationBelongsToGroup(*computationSpec, workerGroup)) {
            return false;
        }
        if (balancerSpec->BalancerType != EJobBalancerType::CpuAware) {
            return true;
        }
        if (!partition->CurrentJobId || GetJobMetricsMaturity(flowView->Feedback->GetCurrentJobStatus(item.first), now) < 1.) {
            return false;
        }
        // The balancer runs on the job's own metrics from now on; a partition that has moved still
        // owes the estimator an observation, which #UpdateWorkerCoefs takes and drops the entry.
        const auto& job = GetOrCrash(layout->Jobs, *partition->CurrentJobId);
        return item.second->WorkerAddress == job->WorkerAddress || balancerSpec->WorkerCoefMode != EWorkerCoefMode::Probing;
    });
}

void PruneBalancerGroups(const TFlowViewPtr& flowView, const THashSet<TWorkerGroupId>& groups)
{
    EraseNodesIf(flowView->State->BalancerState->Groups, [&] (const auto& item) {
        return !groups.contains(item.first);
    });
}

bool WorkerBelongsToGroup(const TWorkerPtr& worker, const TWorkerGroupId& workerGroup)
{
    if (worker->Groups.empty()) {
        return workerGroup.Underlying().empty();
    }
    return FindPtr(worker->Groups, workerGroup);
}

////////////////////////////////////////////////////////////////////////////////

void CompactRebalanceActions(std::vector<TRebalanceResultAction>& actions)
{
    // O(N) algorithm:
    //
    // Walk actions left-to-right. For each Add, append it to `result` and
    // record its index in `lastAddIndex`. For each Del that matches a previous
    // Add (same partition, same worker), mark that Add slot as cancelled
    // (tombstone) instead of erasing it — avoiding any index shifting.
    // Unmatched Dels are appended normally.
    // A final erase-remove pass removes all tombstones in O(N).

    // Sentinel: a cancelled (tombstoned) slot is represented by an action
    // whose Type is neither Add nor Del. We repurpose the unused value by
    // checking a separate bitset to avoid touching the enum.
    // Simpler: use a parallel bool vector.

    std::vector<TRebalanceResultAction> result;
    result.reserve(actions.size());

    std::vector<bool> cancelled; // parallel to `result`; true = tombstoned
    cancelled.reserve(actions.size());

    // lastAddIndex[partitionId] = index in `result` of the most recent Add
    // for that partition (may point to a tombstoned slot).
    THashMap<TPartitionId, size_t> lastAddIndex;

    for (auto& action : actions) {
        if (action.Type == ERebalanceActionType::Add) {
            lastAddIndex[action.PartitionId] = result.size();
            result.push_back(std::move(action));
            cancelled.push_back(false);
        } else { // Del
            auto it = lastAddIndex.find(action.PartitionId);
            if (it != lastAddIndex.end() &&
                !cancelled[it->second] &&
                result[it->second].WorkerAddress == action.WorkerAddress)
            {
                // Cancel the matching Add — mark as tombstone, skip this Del.
                cancelled[it->second] = true;
                lastAddIndex.erase(it);
            } else {
                result.push_back(std::move(action));
                cancelled.push_back(false);
            }
        }
    }

    // Remove tombstoned slots in a single O(N) pass.
    size_t write = 0;
    for (size_t read = 0; read < result.size(); ++read) {
        if (!cancelled[read]) {
            if (write != read) {
                result[write] = std::move(result[read]);
            }
            ++write;
        }
    }
    result.resize(write);

    actions = std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
