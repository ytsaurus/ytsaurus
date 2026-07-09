#include "private.h"

#include "job_balancer_common.h"
#include "job_balancer_resource_queue.h"

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/misc/linear_system.h>

namespace NYT::NFlow::NBalancer {

static const auto& Logger = NController::BalancerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

using TWorkerId = std::string;

// Partitions that have been running for less than this duration are considered
// to be in a warm-up phase. Their RPS is linearly interpolated from the
// per-computation weighted average toward the measured value as they warm up.
static constexpr TDuration PartitionWarmupPeriod = TDuration::Minutes(10);

struct TPartitionInfo
{
    TComputationId ComputationId;
    double Rps{1};
    std::optional<TWorkerId> WorkerId;
    TDuration TimeSinceStart;
};

struct TWorkerInfo
{
    THashSet<TResourceId> PreloadResourceIssued;
    THashSet<TResourceId> PreloadResourceCompleted;
    THashMap<std::string, ssize_t> Capabilities;
    THashMap<std::string, ssize_t> RemainingCapabilities;
    THashSet<TResourceId> DeployedResources;
    bool Underloaded{};
    double TotalQueueSize{};
    double TotalCapacity{};
    THashMap<TComputationId, double> ComputationLoad;
    double TotalLoad{};
};

struct TComputationInfo
{
    THashMap<TResourceId, double> ResourceConsumptionMultiplier;
    THashSet<TWorkerId> Workers;
    std::vector<TWorkerId> TechnicallyPossibleWorkers;
    std::vector<TPartitionId> StrayPartitions;
    double TotalConsumptionMultiplier{};
    double Consumption{};
};

struct TResourceInfo
{
    THashMap<std::string, ssize_t> RequiredCapabilities;
    bool IsPreloadable{};
};

struct TResourceBalanceContext
{
    THashMap<TPartitionId, TPartitionInfo> Partitions;
    THashMap<TWorkerId, TWorkerInfo> Workers;
    THashMap<TComputationId, TComputationInfo> Computations;
    THashMap<TResourceId, TResourceInfo> Resources;
};

//! Collect current state.
TResourceBalanceContext CollectResourceContext(
    const TFlowViewPtr& flowView,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& pipelineSpec = flowView->CurrentSpec->GetValue();

    TResourceBalanceContext context;

    struct TTmpResourceStat
    {
        double PutRate{};
        double FetchRate{};
        double QueueSize{};
        double QueueGrowthRate{};
    };

    struct TTmpWorkerInfo
    {
        THashMap<TResourceId, TTmpResourceStat> ResourceStats;
        TTmpResourceStat TotalResourceStats;
    };

    THashMap<TWorkerId, TTmpWorkerInfo> tmpWorkerInfoSet;
    auto now = TInstant::Now();

    // Collect partition info.
    {
        // Run through layout->Partitions.
        THashSet<TPartitionId> partitionsWithUnknownRps;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            if (!partition->IsWorking())
            {
                continue;
            }
            if (!ComputationBelongsToGroup(GetOrCrash(pipelineSpec->Computations, partition->ComputationId), workerGroup)) {
                continue;
            }
            TPartitionInfo partitionInfo;
            partitionInfo.ComputationId = partition->ComputationId;

            bool foundRps = false;
            if (const auto& status = flowView->Feedback->GetCurrentJobStatus(partitionId)) {
                if (status->InputMetrics) {
                    partitionInfo.Rps = status->InputMetrics->Global.MessagesPerSecond;
                    foundRps = true;
                }
                partitionInfo.TimeSinceStart = now - status->StartTime;
                if (status->StartTime == TInstant::Zero()) {
                    partitionInfo.TimeSinceStart = TDuration::Zero();
                }
            }
            if (!foundRps) {
                partitionsWithUnknownRps.insert(partitionId);
            }

            context.Partitions[partitionId] = std::move(partitionInfo);
        }

        // Fill WorkerId from layout jobs.
        for (const auto& [jobId, job] : layout->Jobs) {
            auto it = context.Partitions.find(job->PartitionId);
            if (it == context.Partitions.end()) {
                continue;
            }
            it->second.WorkerId = job->WorkerAddress;
        }

        // Compute per-computation weighted-average Rps.
        //
        // Each partition's weight = min(TimeSinceStart / PartitionWarmupPeriod, 1).
        // Partitions with unknown Rps are excluded from the average.
        // This ensures that newly-moved partitions (with near-zero measured Rps)
        // do not pull the average down.
        //
        // Falls back to global weighted average if a computation has no known partitions,
        // and to 1.0 if there are no known partitions at all.
        const double warmupPeriodSeconds = PartitionWarmupPeriod.SecondsFloat();

        THashMap<TComputationId, double> computationWeightedRpsSum;
        THashMap<TComputationId, double> computationWeightSum;
        double globalWeightedRpsSum = 0.;
        double globalWeightSum = 0.;

        for (const auto& [partitionId, partitionInfo] : context.Partitions) {
            if (partitionsWithUnknownRps.contains(partitionId)) {
                continue;
            }
            double weight = std::min(partitionInfo.TimeSinceStart.SecondsFloat() / warmupPeriodSeconds, 1.0);
            computationWeightedRpsSum[partitionInfo.ComputationId] += weight * partitionInfo.Rps;
            computationWeightSum[partitionInfo.ComputationId] += weight;
            globalWeightedRpsSum += weight * partitionInfo.Rps;
            globalWeightSum += weight;
        }

        double globalAvgRps = globalWeightSum > 0. ? globalWeightedRpsSum / globalWeightSum : 1.;

        // Per-computation weighted average Rps (falls back to global if unavailable).
        THashMap<TComputationId, double> computationAvgRps;
        for (const auto& [computationId, weightSum] : computationWeightSum) {
            computationAvgRps[computationId] = weightSum > 0.
                ? computationWeightedRpsSum[computationId] / weightSum
                : globalAvgRps;
        }

        // Fill Rps for partitions where it was not available.
        // Use per-computation weighted average if possible, otherwise global weighted average.
        for (const auto& partitionId : partitionsWithUnknownRps) {
            auto& partitionInfo = context.Partitions[partitionId];
            auto avgIt = computationAvgRps.find(partitionInfo.ComputationId);
            partitionInfo.Rps = (avgIt != computationAvgRps.end())
                ? avgIt->second
                : globalAvgRps;
        }

        // Apply warm-up correction to all partitions with known Rps.
        //
        // For a partition that has been running for t seconds:
        //   weight = min(t / warmupPeriod, 1)
        //   effectiveRps = avgRps + weight * (measuredRps - avgRps)
        //                = lerp(avgRps, measuredRps, weight)
        //
        // At t=0  → effectiveRps = avgRps  (partition just started, use average as proxy)
        // At t>=warmupPeriod → effectiveRps = measuredRps  (fully warmed up, use measured)
        for (auto& [partitionId, partitionInfo] : context.Partitions) {
            if (partitionsWithUnknownRps.contains(partitionId)) {
                continue; // Already set to average above.
            }
            double avgRps = globalAvgRps;
            auto avgIt = computationAvgRps.find(partitionInfo.ComputationId);
            if (avgIt != computationAvgRps.end()) {
                avgRps = avgIt->second;
            }
            double weight = std::min(partitionInfo.TimeSinceStart.SecondsFloat() / warmupPeriodSeconds, 1.0);
            partitionInfo.Rps = avgRps + weight * (partitionInfo.Rps - avgRps);
        }
    }

    // Ensure all workers in the group are present in context.Workers,
    // even if they have no feedback status yet, and collect their resource stats from feedback.
    for (const auto& [workerAddress, worker] : flowView->State->Workers) {
        if (!WorkerBelongsToGroup(worker, workerGroup)) {
            continue;
        }
        auto& workerInfo = context.Workers[workerAddress];
        auto& tmpWorkerInfo = tmpWorkerInfoSet[workerAddress];
        workerInfo.Capabilities = worker->Capabilities;
        // Fill PreloadResourceIssued from WorkerSpecs in the layout (what the controller issued to this worker).
        auto workerSpecIt = layout->WorkerSpecs.find(workerAddress);
        if (workerSpecIt != layout->WorkerSpecs.end()) {
            workerInfo.PreloadResourceIssued = workerSpecIt->second->PreloadResources;
        }

        // Collect worker resource stats from feedback.
        auto statusIt = flowView->Feedback->WorkerStatuses.find(workerAddress);
        if (statusIt == flowView->Feedback->WorkerStatuses.end()) {
            continue;
        }
        const auto& workerStatus = statusIt->second;
        for (const auto& [resourceId, resourceStatus] : workerStatus->ResourceStatuses) {
            TTmpResourceStat stat;
            stat.PutRate = resourceStatus->QueuePushRate10m.value_or(
                resourceStatus->QueuePushRate30s.value_or(0.));
            stat.FetchRate = resourceStatus->QueueFetchRate10m.value_or(
                resourceStatus->QueueFetchRate30s.value_or(0.));
            stat.QueueSize = resourceStatus->QueueSize10m.value_or(
                resourceStatus->QueueSize30s.value_or(0.));
            stat.QueueGrowthRate = resourceStatus->QueueGrowthRate10m.value_or(
                resourceStatus->QueueGrowthRate30s.value_or(0.));

            tmpWorkerInfo.TotalResourceStats.PutRate += stat.PutRate;
            tmpWorkerInfo.TotalResourceStats.FetchRate += stat.FetchRate;
            tmpWorkerInfo.TotalResourceStats.QueueSize += stat.QueueSize;
            tmpWorkerInfo.TotalResourceStats.QueueGrowthRate += stat.QueueGrowthRate;

            tmpWorkerInfo.ResourceStats[resourceId] = stat;

            YT_LOG_DEBUG("ResourceQueue: Worker resource feedback "
                "(Worker: %v, Resource: %v, PutRate: %v, FetchRate: %v, "
                "QueueSize: %v, QueueGrowthRate: %v)",
                workerAddress,
                resourceId,
                stat.PutRate,
                stat.FetchRate,
                stat.QueueSize,
                stat.QueueGrowthRate);
        }
        // Fill PreloadResourceIssued from worker feedback.
        for (const auto& [resourceId, preloadState] : workerStatus->PreloadedResourceStates) {
            if (preloadState == EPreloadedResourceState::Preloaded) {
                workerInfo.PreloadResourceCompleted.insert(resourceId);
            }
        }
    }

    // Collect resource info.
    for (const auto& [resourceId, resourceSpec] : pipelineSpec->Resources) {
        TResourceInfo resourceInfo;
        resourceInfo.RequiredCapabilities = resourceSpec->RequiredCapabilities;
        resourceInfo.IsPreloadable = resourceSpec->PreloadRequired;
        context.Resources[resourceId] = std::move(resourceInfo);
    }

    // Collect computation info for all computations that have at least one partition.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        const auto& computationId = partitionInfo.ComputationId;
        if (context.Computations.contains(computationId)) {
            continue;
        }
        TComputationInfo computationInfo;
        const auto& computationSpec = GetOrCrash(pipelineSpec->Computations, computationId);
        for (const auto& [resourceId, resourceDescription] : computationSpec->RequiredResourceIds) {
            computationInfo.ResourceConsumptionMultiplier.emplace(resourceId, 0.);
        }
        context.Computations[computationId] = std::move(computationInfo);
    }

    // Fill ResourceConsumptionMultiplier via least-squares.
    //
    // For each worker w and each resource r, we have one equation:
    //   Sum_{c uses r} Multiplier[c,r] * (Sum of Rps of c's partitions on w) = PutRate[w,r]
    //
    // Variables are keyed by (ComputationId, ResourceId).
    {
        // Pre-compute per-worker per-computation total Rps.
        // workerComputationRps[workerAddress][computationId] = sum of Rps of partitions of c on w.
        THashMap<TWorkerId, THashMap<TComputationId, double>> workerComputationRps;
        for (const auto& [partitionId, partitionInfo] : context.Partitions) {
            if (!partitionInfo.WorkerId) {
                continue;
            }
            workerComputationRps[*partitionInfo.WorkerId][partitionInfo.ComputationId] += partitionInfo.Rps;
        }

        // Collect all resource ids used by any computation in context.
        THashSet<TResourceId> allResourceIds;
        for (const auto& [computationId, computationInfo] : context.Computations) {
            for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                allResourceIds.insert(resourceId);
            }
        }

        // Track which (computationId, resourceId) pairs had at least one worker with
        // both non-zero rps for that computation AND non-zero put_rate for that resource.
        // These are the "valid" data points for the linear system.
        THashMap<TComputationId, THashSet<TResourceId>> validMultiplierKeys;

        auto isValidMultiplier = [&] (const TComputationId& computationId, const TResourceId& resourceId) {
            auto it = validMultiplierKeys.find(computationId);
            return it != validMultiplierKeys.end() && it->second.contains(resourceId);
        };

        // Build and solve one linear system per resource.
        for (const auto& resourceId : allResourceIds) {
            TKeyedLinearSystem<TComputationId> system;

            for (const auto& [workerAddress, workerInfo] : tmpWorkerInfoSet) {
                auto workerRpsIt = workerComputationRps.find(workerAddress);

                double putRate = 0.;
                auto resourceStatIt = workerInfo.ResourceStats.find(resourceId);
                if (resourceStatIt != workerInfo.ResourceStats.end()) {
                    putRate = resourceStatIt->second.PutRate;
                }

                // Build the row: coefficients for each computation that uses this resource.
                std::vector<std::pair<TComputationId, double>> row;
                for (const auto& [computationId, computationInfo] : context.Computations) {
                    if (!computationInfo.ResourceConsumptionMultiplier.contains(resourceId)) {
                        continue;
                    }
                    double rpsSum = 0.;
                    if (workerRpsIt != workerComputationRps.end()) {
                        auto compRpsIt = workerRpsIt->second.find(computationId);
                        if (compRpsIt != workerRpsIt->second.end()) {
                            rpsSum = compRpsIt->second;
                        }
                    }
                    row.emplace_back(computationId, rpsSum);

                    // Mark as valid if this worker contributes useful data for this pair.
                    if (rpsSum > 0. && putRate > 0.) {
                        validMultiplierKeys[computationId].insert(resourceId);
                    }
                }

                system.AddRow(std::move(row), putRate);
            }

            auto solution = system.Solve();
            for (const auto& [computationId, multiplier] : solution) {
                context.Computations[computationId].ResourceConsumptionMultiplier[resourceId] = multiplier;
            }
        }

        // Apply fallback for invalid (computationId, resourceId) multipliers.
        //
        // A multiplier is invalid when no worker had both non-zero rps for that computation
        // AND non-zero put_rate for that resource (the linear system had no useful data).
        // In that case, replace the multiplier with a fallback in priority order:
        //   1. Average of valid multipliers for the same resource across other computations.
        //   2. Average of valid multipliers for the same computation across other resources.
        //   3. Average of all valid multipliers.
        //   4. 1.0 (last resort).
        {
            // Pre-compute averages of valid multipliers grouped by resource and by computation.
            THashMap<TResourceId, double> validSumByResource;
            THashMap<TResourceId, int> validCountByResource;
            THashMap<TComputationId, double> validSumByComputation;
            THashMap<TComputationId, int> validCountByComputation;
            double validSumGlobal = 0.;
            int validCountGlobal = 0;

            for (const auto& [computationId, computationInfo] : context.Computations) {
                for (const auto& [resourceId, multiplier] : computationInfo.ResourceConsumptionMultiplier) {
                    if (!isValidMultiplier(computationId, resourceId)) {
                        continue;
                    }
                    validSumByResource[resourceId] += multiplier;
                    ++validCountByResource[resourceId];
                    validSumByComputation[computationId] += multiplier;
                    ++validCountByComputation[computationId];
                    validSumGlobal += multiplier;
                    ++validCountGlobal;
                }
            }

            double globalAvgMultiplier = validCountGlobal > 0 ? validSumGlobal / validCountGlobal : 1.0;

            for (auto& [computationId, computationInfo] : context.Computations) {
                for (auto& [resourceId, multiplier] : computationInfo.ResourceConsumptionMultiplier) {
                    if (isValidMultiplier(computationId, resourceId)) {
                        continue; // Already valid.
                    }
                    // Priority 1: average over other computations using the same resource.
                    auto byResIt = validCountByResource.find(resourceId);
                    if (byResIt != validCountByResource.end() && byResIt->second > 0) {
                        multiplier = validSumByResource[resourceId] / byResIt->second;
                        continue;
                    }
                    // Priority 2: average over other resources of the same computation.
                    auto byCompIt = validCountByComputation.find(computationId);
                    if (byCompIt != validCountByComputation.end() && byCompIt->second > 0) {
                        multiplier = validSumByComputation[computationId] / byCompIt->second;
                        continue;
                    }
                    // Priority 3: global average.
                    // Priority 4: 1.0 (already set as globalAvgMultiplier when validCountGlobal == 0).
                    multiplier = globalAvgMultiplier;
                }
            }
        }

        // Compute TotalConsumptionMultiplier as sum of ConsumptionMultiplier over all resources.
        for (auto& [computationId, computationInfo] : context.Computations) {
            double total = 0.;
            for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                total += consumptionMultiplier;
            }
            computationInfo.TotalConsumptionMultiplier = total;
        }
    }

    // Compute Underloaded and TotalCapacity for each worker.
    //
    // rate = (TotalPutRate + TotalFetchRate) / 2
    // Underloaded = TotalQueueSize < ZeroQueueLatency * rate
    //
    // If !Underloaded, TotalCapacity is derived from:
    //   Sum_{c,r} (SumRps[c on w] * Multiplier[c,r]) = TotalCapacity + TotalQueueGrowthRate
    // => TotalCapacity = Sum_{c,r}(SumRps[c on w] * Multiplier[c,r]) - TotalQueueGrowthRate
    {
        // Pre-compute per-worker per-computation total Rps (reuse same logic as above).
        THashMap<TWorkerId, THashMap<TComputationId, double>> workerComputationRps;
        for (const auto& [partitionId, partitionInfo] : context.Partitions) {
            if (!partitionInfo.WorkerId) {
                continue;
            }
            workerComputationRps[*partitionInfo.WorkerId][partitionInfo.ComputationId] += partitionInfo.Rps;
        }

        double zeroQueueLatencySeconds = balancerSpec->ZeroQueueLatency.SecondsFloat();

        // Pre-compute set of workers that have at least one partition assigned.
        THashSet<TWorkerId> workersWithPartitions;
        for (const auto& [partitionId, partitionInfo] : context.Partitions) {
            if (partitionInfo.WorkerId) {
                workersWithPartitions.insert(*partitionInfo.WorkerId);
            }
        }

        for (auto& [workerAddress, workerInfo] : context.Workers) {
            auto& tmpWorkerInfo = GetOrCrash(tmpWorkerInfoSet, workerAddress);
            const auto& total = tmpWorkerInfo.TotalResourceStats;
            // Populate TotalQueueSize from aggregated resource stats.
            workerInfo.TotalQueueSize = total.QueueSize;
            double rate = (total.PutRate + total.FetchRate) / 2.0;
            workerInfo.Underloaded = !workersWithPartitions.contains(workerAddress) ||
                total.QueueSize < zeroQueueLatencySeconds * rate;

            double incomingLoad = 0.;
            auto workerRpsIt = workerComputationRps.find(workerAddress);
            if (workerRpsIt != workerComputationRps.end()) {
                for (const auto& [computationId, computationInfo] : context.Computations) {
                    auto compRpsIt = workerRpsIt->second.find(computationId);
                    if (compRpsIt == workerRpsIt->second.end()) {
                        continue;
                    }
                    double rpsSum = compRpsIt->second;
                    for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                        incomingLoad += rpsSum * consumptionMultiplier;
                    }
                }
            }
            workerInfo.TotalCapacity = incomingLoad - total.QueueGrowthRate;
        }

        // Fix TotalCapacity for Underloaded workers.
        // For each underloaded worker, use the average TotalCapacity of !Underloaded workers
        // with the same Capabilities. If none exist, use the global average of all !Underloaded workers.

        // Build per-capabilities-set average capacity from !Underloaded workers.
        // Key: sorted vector of (capName, capValue) pairs to identify capability sets.
        using TCapKey = std::vector<std::pair<std::string, ssize_t>>;
        auto makeCapKey = [] (const THashMap<std::string, ssize_t>& caps) {
            TCapKey key(caps.begin(), caps.end());
            std::sort(key.begin(), key.end());
            return key;
        };

        std::map<TCapKey, double> capKeyCapacitySum;
        std::map<TCapKey, int> capKeyCapacityCount;
        double globalCapacitySum = 0.;
        int globalCapacityCount = 0;

        for (const auto& [workerAddress, workerInfo] : context.Workers) {
            if (workerInfo.Underloaded) {
                continue;
            }
            auto key = makeCapKey(workerInfo.Capabilities);
            capKeyCapacitySum[key] += workerInfo.TotalCapacity;
            ++capKeyCapacityCount[key];
            globalCapacitySum += workerInfo.TotalCapacity;
            ++globalCapacityCount;
        }

        double globalAvgCapacity;
        if (globalCapacityCount > 0) {
            globalAvgCapacity = globalCapacitySum / globalCapacityCount;
        } else {
            // No !Underloaded workers to derive capacity from.
            // Estimate as total pipeline load (Sum Rps * Multiplier) divided by worker count.

            // Step 1: group total Rps by computation.
            THashMap<TComputationId, double> computationTotalRps;
            for (const auto& [partitionId, partitionInfo] : context.Partitions) {
                computationTotalRps[partitionInfo.ComputationId] += partitionInfo.Rps;
            }

            // Step 2: multiply by multipliers and sum.
            double totalPipelineLoad = 0.;
            for (const auto& [computationId, totalRps] : computationTotalRps) {
                const auto& computationInfo = GetOrCrash(context.Computations, computationId);
                for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                    totalPipelineLoad += totalRps * consumptionMultiplier;
                }
            }

            int workerCount = static_cast<int>(context.Workers.size());
            globalAvgCapacity = workerCount > 0 ? totalPipelineLoad / workerCount : 0.;
        }

        for (auto& [workerAddress, workerInfo] : context.Workers) {
            if (!workerInfo.Underloaded) {
                continue;
            }
            auto key = makeCapKey(workerInfo.Capabilities);
            auto countIt = capKeyCapacityCount.find(key);
            double capacityEstimation;
            if (countIt != capKeyCapacityCount.end() && countIt->second > 0) {
                capacityEstimation = GetOrCrash(capKeyCapacitySum, key) / countIt->second;
            } else {
                capacityEstimation = globalAvgCapacity;
            }
            workerInfo.TotalCapacity = std::max(capacityEstimation, workerInfo.TotalCapacity);
        }

        // A worker can be "underloaded" (queue below ZeroQueueLatency * rate) yet still carry a
        // meaningful standing backlog. Its capacity computed above is self-referential (≈ current load,
        // or peer-borrowed) and overstates what it can actually drain, so the queue projection
        // (computeProjectedAvgQueue) and the per-computation capacity check (MaxPossibleCapacity /
        // AllocatedCapacity) believe a saturated worker suffices — it never sheds the backlog, and a
        // computation stuck on it never gets more workers. Cap such a worker's FINAL capacity at its
        // measured FetchRate (real drain rate). This must run AFTER the std::max above: for the
        // backlogged workers we target, the base capacity (incomingLoad) exceeds FetchRate, so capping
        // the estimate before the max would just be discarded by it. Near-empty workers are untouched,
        // so cold-start spreading onto idle workers is unaffected.
        {
            constexpr double kBackloggedQueueFraction = 0.25;
            for (auto& [workerAddress, workerInfo] : context.Workers) {
                const auto& total = GetOrCrash(tmpWorkerInfoSet, workerAddress).TotalResourceStats;
                double rate = (total.PutRate + total.FetchRate) / 2.0;
                if (workerInfo.TotalQueueSize > kBackloggedQueueFraction * zeroQueueLatencySeconds * rate) {
                    workerInfo.TotalCapacity = std::min(workerInfo.TotalCapacity, total.FetchRate);
                }
            }
        }
    }

    // Fill DeployedResources for each worker.
    // A resource is deployed if:
    //   (a) it is required by a computation that has at least one partition on this worker, OR
    //   (b) it appears in PreloadResourceIssued or PreloadResourceCompleted.

    // Seed from preload state (issued + completed).
    for (auto& [workerAddress, workerInfo] : context.Workers) {
        for (const auto& resourceId : workerInfo.PreloadResourceIssued) {
            workerInfo.DeployedResources.insert(resourceId);
        }
        for (const auto& resourceId : workerInfo.PreloadResourceCompleted) {
            workerInfo.DeployedResources.insert(resourceId);
        }
    }

    // Seed from partitions: for each partition on a worker, add all resources
    // required by that partition's computation.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        if (!partitionInfo.WorkerId) {
            continue;
        }
        const auto& computationInfo = GetOrCrash(context.Computations, partitionInfo.ComputationId);
        auto& workerInfo = GetOrCrash(context.Workers, *partitionInfo.WorkerId);
        for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
            workerInfo.DeployedResources.insert(resourceId);
        }
    }

    // Fill RemainingCapabilities for each worker:
    // worker's Capabilities minus the sum of RequiredCapabilities
    // of all deployed resources (each resource counted once, not per computation).
    for (auto& [workerAddress, workerInfo] : context.Workers) {
        workerInfo.RemainingCapabilities = workerInfo.Capabilities;
        for (const auto& resourceId : workerInfo.DeployedResources) {
            const auto& resource = GetOrCrash(context.Resources, resourceId);
            for (const auto& [capName, capValue] : resource.RequiredCapabilities) {
                workerInfo.RemainingCapabilities[capName] -= capValue;
            }
        }
    }

    // Fill Consumption for each computation:
    // total capacity needed = Sum(p.Rps for all partitions of c) * c.TotalConsumptionMultiplier.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        const auto& computationInfo = GetOrCrash(context.Computations, partitionInfo.ComputationId);
        GetOrCrash(context.Computations, partitionInfo.ComputationId).Consumption +=
            partitionInfo.Rps * computationInfo.TotalConsumptionMultiplier;
    }

    // Fill ComputationLoad and TotalLoad for each worker.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        if (!partitionInfo.WorkerId) {
            continue;
        }
        const auto& computationInfo = GetOrCrash(context.Computations, partitionInfo.ComputationId);
        auto& workerInfo = GetOrCrash(context.Workers, *partitionInfo.WorkerId);
        workerInfo.ComputationLoad[partitionInfo.ComputationId] +=
            partitionInfo.Rps * computationInfo.TotalConsumptionMultiplier;
    }
    for (auto& [workerAddress, workerInfo] : context.Workers) {
        double total = 0.;
        for (const auto& [_, load] : workerInfo.ComputationLoad) {
            total += load;
        }
        workerInfo.TotalLoad = total;
    }

    // Compute TechnicallyPossibleWorkers for each computation.
    //
    // A worker is technically possible for a computation if all resources required
    // by the computation can be placed on that worker. A resource can be placed on
    // a worker if:
    //   1. All required capability keys of the resource are present in the worker's capabilities.
    //   2. The sum of required capability values across ALL resources of the computation
    //      does not exceed the worker's capability value for each capability key.
    //
    // Note: a resource is placed on a worker once regardless of how many computations
    // or partitions need it, so we sum required capabilities over all resources of the
    // computation (not per-partition).
    {
        for (auto& [computationId, computationInfo] : context.Computations) {
            // Compute the aggregate required capabilities for this computation:
            // sum of RequiredCapabilities over all its resources.
            THashMap<std::string, ssize_t> aggregatedRequired;
            for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                auto resourceInfo = GetOrCrash(context.Resources, resourceId);
                for (const auto& [capName, capValue] : resourceInfo.RequiredCapabilities) {
                    aggregatedRequired[capName] += capValue;
                }
            }

            for (const auto& [workerAddress, workerInfo] : context.Workers) {
                bool possible = true;
                for (const auto& [capName, requiredValue] : aggregatedRequired) {
                    auto workerCapIt = workerInfo.Capabilities.find(capName);
                    if (workerCapIt == workerInfo.Capabilities.end() ||
                        workerCapIt->second < requiredValue)
                    {
                        possible = false;
                        break;
                    }
                }
                if (possible) {
                    computationInfo.TechnicallyPossibleWorkers.push_back(workerAddress);
                }
            }
        }
    }

    // Fill Workers for each computation: set of workers that currently have at least
    // one partition of this computation assigned.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        if (!partitionInfo.WorkerId) {
            continue;
        }
        GetOrCrash(context.Computations, partitionInfo.ComputationId).Workers.insert(*partitionInfo.WorkerId);
    }

    // Fill StrayPartitions for each computation: partitions with no WorkerId assigned.
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        if (!partitionInfo.WorkerId) {
            GetOrCrash(context.Computations, partitionInfo.ComputationId).StrayPartitions.push_back(partitionId);
        }
    }

    // -------------------------------------------------------------------------
    // Log collected context summary.
    // -------------------------------------------------------------------------
    YT_LOG_INFO("ResourceQueue: context collected "
        "(WorkerGroup: %v, Workers: %v, Partitions: %v, Computations: %v, Resources: %v)",
        workerGroup,
        context.Workers.size(),
        context.Partitions.size(),
        context.Computations.size(),
        context.Resources.size());

    for (const auto& [workerAddress, workerInfo] : context.Workers) {
        YT_LOG_DEBUG("ResourceQueue: Worker state "
            "(Worker: %v, TotalCapacity: %v, TotalLoad: %v, Underloaded: %v, "
            "DeployedResources: %v, Computations: %v)",
            workerAddress,
            workerInfo.TotalCapacity,
            workerInfo.TotalLoad,
            workerInfo.Underloaded,
            workerInfo.DeployedResources.size(),
            workerInfo.ComputationLoad.size());

        if (const auto* tmp = tmpWorkerInfoSet.FindPtr(workerAddress)) {
            YT_LOG_DEBUG("ResourceQueue: Worker resource totals "
                "(Worker: %v, TotalPutRate: %v, TotalFetchRate: %v, "
                "TotalQueueSize: %v, TotalQueueGrowthRate: %v)",
                workerAddress,
                tmp->TotalResourceStats.PutRate,
                tmp->TotalResourceStats.FetchRate,
                tmp->TotalResourceStats.QueueSize,
                tmp->TotalResourceStats.QueueGrowthRate);
        }
    }

    for (const auto& [computationId, computationInfo] : context.Computations) {
        YT_LOG_DEBUG("ResourceQueue: Computation state "
            "(Computation: %v, Consumption: %v, TotalConsumptionMultiplier: %v, "
            "Workers: %v, TechnicallyPossibleWorkers: %v, StrayPartitions: %v)",
            computationId,
            computationInfo.Consumption,
            computationInfo.TotalConsumptionMultiplier,
            computationInfo.Workers.size(),
            computationInfo.TechnicallyPossibleWorkers.size(),
            computationInfo.StrayPartitions.size());
        for (const auto& [resourceId, multiplier] : computationInfo.ResourceConsumptionMultiplier) {
            YT_LOG_DEBUG("ResourceQueue: Computation resource multiplier "
                "(Computation: %v, Resource: %v, Multiplier: %v)",
                computationId,
                resourceId,
                multiplier);
        }
    }

    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        YT_LOG_DEBUG("ResourceQueue: Partition state "
            "(Partition: %v, Computation: %v, Rps: %v, Worker: %v, TimeSinceStart: %v)",
            partitionId,
            partitionInfo.ComputationId,
            partitionInfo.Rps,
            partitionInfo.WorkerId.value_or("<none>"),
            partitionInfo.TimeSinceStart);
    }

    return context;
}

} // anonymous namespace

TRebalanceResult DoBalanceResourceQueue(
    const TFlowViewPtr& flowView,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup)
{
    const double planningHorizonSeconds = balancerSpec->PlanningHorizon.SecondsFloat();
    TRebalanceResult rebalanceResult;

    // Collect current context (partitions, workers, computations with multipliers).
    auto context = CollectResourceContext(flowView, balancerSpec, workerGroup);

    // =========================================================================
    // Step 1: Determine which computations need more workers.
    // =========================================================================
    //
    // A computation c needs more workers if, even after optimally redistributing
    // load among its current workers, it still cannot get enough capacity.
    //
    // maxPossibleCapacity[c] = Sum over workers w of c:
    //   min(w.TotalCapacity, workerInfo.ComputationLoad[c] + freeCapacity[w])
    // where freeCapacity[w] = max(0, w.TotalCapacity - workerInfo.TotalLoad).
    //
    // If maxPossibleCapacity[c] < computationInfo.Consumption, c needs more workers.
    //
    // starvingComputations: sorted by starvation degree descending.
    // starvation[c] = 1 - allocatedCapacity[c] / consumption[c], clamped to [0,1].

    // freeCapacity[w]: unused capacity on worker w.
    THashMap<TWorkerId, double> workerFreeCapacity;
    for (const auto& [workerAddress, workerInfo] : context.Workers) {
        double free = workerInfo.TotalCapacity - workerInfo.TotalLoad;
        workerFreeCapacity[workerAddress] = std::max(0., free);
    }

    // maxPossibleCapacity[c]: maximum capacity c could get from its current workers.
    // For each worker w of c, c can claim its own load plus the full free capacity on w.
    // This is correct because other computations on w can reduce their share (via Step 3
    // redistribution), so c can potentially claim all of the free capacity on w.
    THashMap<TComputationId, double> computationMaxPossibleCapacity;
    for (const auto& [computationId, computationInfo] : context.Computations) {
        double maxPossible = 0.;
        for (const auto& workerAddress : computationInfo.Workers) {
            const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
            double loadOnWorker = GetOrDefault(workerInfo.ComputationLoad, computationId, 0.);
            double free = workerFreeCapacity[workerAddress];
            maxPossible += std::min(workerInfo.TotalCapacity, loadOnWorker + free);
        }
        computationMaxPossibleCapacity[computationId] = maxPossible;
    }

    // Starving computations: those where AllocatedCapacity < Consumption.
    // Max-heap ordered by starvation degree: 1 - allocatedCapacity / consumption, clamped to [0, 1].
    using TStarvationEntry = std::pair<double, TComputationId>;
    std::priority_queue<TStarvationEntry> starvingQueue;

    // =========================================================================
    // Step 2: Iteratively add workers for starving computations.
    // =========================================================================
    //
    // Treat each computation as a continuous mass of consumption to spread across
    // workers. Pop the most-starving computation, find the best new worker for it,
    // add that worker to computationInfo.Workers, update state, recompute starvation,
    // and push back if still starving. Stop when the queue is empty.
    //
    // A worker w is a candidate for computation c if:
    //   1. w is in c.TechnicallyPossibleWorkers.
    //   2. w is NOT already in computationInfo.Workers.
    //   3. For each resource r required by c that is NOT yet in workerInfo.DeployedResources,
    //      workerInfo.RemainingCapabilities has enough capacity for r.RequiredCapabilities.
    //
    // Among candidates, pick the one with the lowest relative load:
    //   relativeLoad[w] = workerInfo.TotalLoad / max(w.TotalCapacity, epsilon)
    //
    // After adding w to c:
    //   - Update workerInfo.DeployedResources with new resources.
    //   - Update workerInfo.RemainingCapabilities by subtracting new resources' capabilities.
    //   - Assume c's consumption is spread evenly across all its workers (including w). Virtual
    //     allocation only — moves no partitions: Step 6 places c's STRAY partitions on w, while
    //     already-assigned ones migrate only via Step 8 (gated by the Step 9 threshold). So with no
    //     strays, w may stay empty though "satisfied" here.
    //   - Recompute allocatedCapacity[c] and starvation[c].

    static constexpr double kEpsilon = 1e-9;

    // Helper: check if worker w can host computation c (capability check for new resources).
    auto canHostComputation = [&] (const TWorkerId& workerAddress, const TComputationId& computationId) -> bool {
        const auto& computationInfo = GetOrCrash(context.Computations, computationId);
        const auto& deployed = GetOrCrash(context.Workers, workerAddress).DeployedResources;
        const auto& remaining = GetOrCrash(context.Workers, workerAddress).RemainingCapabilities;

        for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
            if (deployed.contains(resourceId)) {
                continue; // Already deployed, no extra capability needed.
            }
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end()) {
                continue;
            }
            for (const auto& [capName, capValue] : resourceIt->second.RequiredCapabilities) {
                auto remainIt = remaining.find(capName);
                ssize_t available = (remainIt != remaining.end()) ? remainIt->second : 0;
                if (available < capValue) {
                    return false;
                }
            }
        }
        return true;
    };

    // Helper: add computation c to worker w — update deployed resources and remaining caps.
    auto addComputationToWorker = [&] (const TWorkerId& workerAddress, const TComputationId& computationId) {
        const auto& computationInfo = GetOrCrash(context.Computations, computationId);
        auto& deployed = GetOrCrash(context.Workers, workerAddress).DeployedResources;
        auto& remaining = GetOrCrash(context.Workers, workerAddress).RemainingCapabilities;

        for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
            if (deployed.contains(resourceId)) {
                continue;
            }
            deployed.insert(resourceId);
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end()) {
                continue;
            }
            for (const auto& [capName, capValue] : resourceIt->second.RequiredCapabilities) {
                remaining[capName] -= capValue;
            }
        }
        GetOrCrash(context.Computations, computationId).Workers.insert(workerAddress);
    };

    // Helper: remove computation c from worker w — free deployed resources that are no longer
    // needed by any other computation still assigned to that worker (or by preload state).
    // Does NOT update computationInfo.Workers — the caller must do that.
    auto removeComputationFromWorker = [&] (const TWorkerId& workerAddress, const TComputationId& computationId) {
        // Collect resources still needed on this worker by other computations.
        THashSet<TResourceId> stillNeeded;
        for (const auto& [otherComputationId, otherComputationInfo] : context.Computations) {
            if (otherComputationId == computationId) {
                continue; // Skip the computation being removed.
            }
            if (!otherComputationInfo.Workers.contains(workerAddress)) {
                continue;
            }
            for (const auto& [resourceId, consumptionMultiplier] : otherComputationInfo.ResourceConsumptionMultiplier) {
                stillNeeded.insert(resourceId);
            }
        }

        // Also keep resources pinned by preload state (issued or completed).
        const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
        for (const auto& resourceId : workerInfo.PreloadResourceIssued) {
            stillNeeded.insert(resourceId);
        }
        for (const auto& resourceId : workerInfo.PreloadResourceCompleted) {
            stillNeeded.insert(resourceId);
        }

        // Free capabilities for resources that are no longer needed on this worker.
        auto& deployed = GetOrCrash(context.Workers, workerAddress).DeployedResources;
        auto& remaining = GetOrCrash(context.Workers, workerAddress).RemainingCapabilities;
        const auto& computationInfo = GetOrCrash(context.Computations, computationId);
        for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
            if (stillNeeded.contains(resourceId)) {
                continue; // Still required by another computation or preload.
            }
            if (!deployed.contains(resourceId)) {
                continue; // Not deployed (defensive check).
            }
            deployed.erase(resourceId);
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end()) {
                continue;
            }
            for (const auto& [capName, capValue] : resourceIt->second.RequiredCapabilities) {
                remaining[capName] += capValue;
            }
        }
    };

    // Helper: recompute allocatedCapacity[c] given current computationInfo.Workers.
    // Virtual allocation: assume c's consumption is spread proportionally across its workers
    // based on each worker's TotalCapacity share. Then apply the contention formula per worker:
    // if the worker is overloaded (workerInfo.TotalLoad > TotalCapacity), c only gets a proportional
    // share of the worker's capacity. This correctly accounts for other computations sharing
    // the same workers.
    //
    // Virtual load of c on worker w:
    //   virtualLoad[c,w] = consumption * w.TotalCapacity / totalWorkerCapacity
    //
    // Allocated portion from worker w:
    //   effectiveTotalLoad = workerInfo.TotalLoad - existingLoad[c,w] + virtualLoad[c,w]
    //   if effectiveTotalLoad <= w.TotalCapacity: allocated += virtualLoad[c,w]
    //   else: allocated += virtualLoad[c,w] / effectiveTotalLoad * w.TotalCapacity
    auto recomputeAllocatedCapacity = [&] (const TComputationId& computationId) -> double {
        double consumption = GetOrCrash(context.Computations, computationId).Consumption;
        if (consumption <= 0.) {
            return 0.;
        }
        const auto& workers = GetOrCrash(context.Computations, computationId).Workers;
        if (workers.empty()) {
            return 0.;
        }

        // Sum of TotalCapacity across workers of c (used to distribute c's consumption).
        double totalWorkerCapacity = 0.;
        for (const auto& workerAddress : workers) {
            const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
            totalWorkerCapacity += workerInfo.TotalCapacity;
        }
        if (totalWorkerCapacity <= kEpsilon) {
            return 0.;
        }

        double allocated = 0.;
        for (const auto& workerAddress : workers) {
            const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
            double workerCapacity = workerInfo.TotalCapacity;

            // Virtual load that c would place on this worker (proportional to capacity share).
            double virtualLoad = consumption * workerCapacity / totalWorkerCapacity;

            // Existing actual load of c on this worker (from real partitions, may be 0 for
            // newly added virtual workers).
            double existingLoad = GetOrDefault(workerInfo.ComputationLoad, computationId, 0.);

            // Effective total load on this worker: replace c's existing load with virtual load.
            double otherLoad = workerInfo.TotalLoad - existingLoad;
            double effectiveTotalLoad = otherLoad + virtualLoad;

            double allocatedFromWorker;
            if (effectiveTotalLoad <= workerCapacity || effectiveTotalLoad <= kEpsilon) {
                allocatedFromWorker = virtualLoad;
            } else {
                allocatedFromWorker = virtualLoad / effectiveTotalLoad * workerCapacity;
            }
            allocated += allocatedFromWorker;
        }
        return allocated;
    };

    // Helper: recompute AllocatedCapacity for all computations and build a fresh starvation queue.
    // Includes all computations where AllocatedCapacity < Consumption.
    // Computations with no workers are unconditionally starving (starvation = 1.0).
    auto buildStarvingQueue = [&] {
        std::priority_queue<TStarvationEntry> queue;
        for (auto& [computationId, computationInfo] : context.Computations) {
            if (computationInfo.Workers.empty()) {
                // No workers assigned yet — unconditionally starving.
                queue.emplace(1.0, computationId);
                continue;
            }
            if (computationInfo.Consumption <= 0.) {
                continue;
            }
            double allocated = recomputeAllocatedCapacity(computationId);
            if (allocated >= computationInfo.Consumption) {
                continue;
            }
            double starvation = std::clamp(1. - allocated / computationInfo.Consumption, 0., 1.);
            queue.emplace(starvation, computationId);
        }
        return queue;
    };

    // Seed the starvation queue for Step 2: only computations that cannot be satisfied
    // even with optimal redistribution among current workers (maxPossible < Consumption).
    // Computations with no workers are unconditionally starving (starvation = 1.0).
    for (auto& [computationId, computationInfo] : context.Computations) {
        if (computationInfo.Workers.empty()) {
            YT_LOG_DEBUG("ResourceQueue: Step 1 computation has no workers, unconditionally starving "
                "(Computation: %v, StrayPartitions: %v, TechnicallyPossibleWorkers: %v)",
                computationId,
                computationInfo.StrayPartitions.size(),
                computationInfo.TechnicallyPossibleWorkers.size());
            starvingQueue.emplace(1.0, computationId);
            continue;
        }
        if (computationInfo.Consumption <= 0.) {
            continue;
        }
        double allocated = recomputeAllocatedCapacity(computationId);
        double maxPossible = GetOrDefault(computationMaxPossibleCapacity, computationId, 0.);
        YT_LOG_DEBUG("ResourceQueue: Step 1 computation capacity check "
            "(Computation: %v, Consumption: %v, AllocatedCapacity: %v, MaxPossibleCapacity: %v, "
            "CurrentWorkers: %v, TechnicallyPossibleWorkers: %v)",
            computationId,
            computationInfo.Consumption,
            allocated,
            maxPossible,
            computationInfo.Workers.size(),
            computationInfo.TechnicallyPossibleWorkers.size());
        if (maxPossible >= computationInfo.Consumption) {
            YT_LOG_DEBUG("ResourceQueue: Step 1 computation satisfied by existing workers, skipping Step 2 "
                "(Computation: %v)",
                computationId);
            continue; // Can be satisfied by existing workers — skip Step 2 for this computation.
        }
        double starvation = std::clamp(1. - allocated / computationInfo.Consumption, 0., 1.);
        YT_LOG_DEBUG("ResourceQueue: Step 1 computation seeded into starvation queue "
            "(Computation: %v, Starvation: %v)",
            computationId,
            starvation);
        starvingQueue.emplace(starvation, computationId);
    }

    YT_LOG_DEBUG("ResourceQueue: Step 2 starting "
        "(WorkerGroup: %v, StarvingComputations: %v)",
        workerGroup,
        starvingQueue.size());

    while (!starvingQueue.empty()) {
        auto [starvation, computationId] = starvingQueue.top();
        starvingQueue.pop();

        const auto& computationInfo = GetOrCrash(context.Computations, computationId);

        // A computation with no workers is unconditionally starving until it gets its first worker.
        // Skip the Consumption check and starvation recheck in that case.
        double currentStarvation = 1.0;
        if (!computationInfo.Workers.empty()) {
            if (computationInfo.Consumption <= 0.) {
                continue;
            }

            // Recheck starvation (state may have changed since this entry was pushed).
            double currentAllocated = recomputeAllocatedCapacity(computationId);
            currentStarvation = std::clamp(1. - currentAllocated / computationInfo.Consumption, 0., 1.);
            if (currentStarvation <= 0.) {
                YT_LOG_DEBUG("ResourceQueue: Step 2 computation no longer starving, skipping "
                    "(Computation: %v)",
                    computationId);
                continue; // No longer starving.
            }
        }

        // Find the best candidate worker.
        const auto& currentWorkers = computationInfo.Workers;

        std::optional<TWorkerId> bestWorker;
        double bestRelativeLoad = std::numeric_limits<double>::max();

        for (const auto& workerAddress : computationInfo.TechnicallyPossibleWorkers) {
            if (currentWorkers.contains(workerAddress)) {
                continue; // Already hosting this computation.
            }
            if (!canHostComputation(workerAddress, computationId)) {
                continue; // Not enough remaining capabilities.
            }
            const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
            double relativeLoad = workerInfo.TotalLoad / std::max(workerInfo.TotalCapacity, kEpsilon);
            if (relativeLoad < bestRelativeLoad) {
                bestRelativeLoad = relativeLoad;
                bestWorker = workerAddress;
            }
        }

        if (!bestWorker) {
            YT_LOG_DEBUG("ResourceQueue: Step 2 no feasible worker found for starving computation "
                "(Computation: %v, Starvation: %v, CurrentWorkers: %v, TechnicallyPossibleWorkers: %v)",
                computationId,
                currentStarvation,
                computationInfo.Workers.size(),
                computationInfo.TechnicallyPossibleWorkers.size());
            continue; // No feasible worker found — skip this computation.
        }

        // Add the best worker to this computation.
        addComputationToWorker(*bestWorker, computationId);

        // Recompute starvation after adding the worker.
        // Note: computationInfo.Workers was updated by addComputationToWorker above,
        // so Workers is now non-empty. Use Consumption if available, otherwise 0.
        double newStarvation = 0.;
        if (computationInfo.Consumption > 0.) {
            double newAllocated = recomputeAllocatedCapacity(computationId);
            newStarvation = std::clamp(1. - newAllocated / computationInfo.Consumption, 0., 1.);
        }

        YT_LOG_DEBUG("ResourceQueue: Step 2 added worker to starving computation "
            "(Computation: %v, Worker: %v, BestRelativeLoad: %v, "
            "StarvationBefore: %v, StarvationAfter: %v)",
            computationId,
            *bestWorker,
            bestRelativeLoad,
            currentStarvation,
            newStarvation);

        if (newStarvation > 0.) {
            starvingQueue.emplace(newStarvation, computationId);
        }
    }

    // =========================================================================
    // Step 3: Steal a worker from a non-starving computation.
    // =========================================================================
    //
    // For each still-starving computation cStarving:
    //   Find a worker w in TechnicallyPossibleWorkers[cStarving] where:
    //     - A non-starving computation cSurplus is present (AllocatedCapacity >= Consumption).
    //     - After removing w from cSurplus, cSurplus would not starve more than cStarving does now.
    //   If found:
    //     - Remove w from cSurplus.Workers (no replacement worker given to cSurplus).
    //     - Add w to cStarving.Workers.
    //   Prefer the cSurplus with the smallest load on w (cheapest to move).
    //   Bound: stop after O(numWorkers) total moves.

    // Rebuild the starvation queue for Step 3: all computations where AllocatedCapacity < Consumption.
    starvingQueue = buildStarvingQueue();

    YT_LOG_DEBUG("ResourceQueue: Step 3 starting "
        "(WorkerGroup: %v, StarvingComputations: %v, MaxMoves: %v)",
        workerGroup,
        starvingQueue.size(),
        context.Workers.size());

    {
        int maxMoves = static_cast<int>(context.Workers.size());
        int movesDone = 0;

        while (!starvingQueue.empty() && movesDone < maxMoves) {
            auto [starvation, cStarving] = starvingQueue.top();
            starvingQueue.pop();

            const auto& starvingComputationInfo = GetOrCrash(context.Computations, cStarving);
            double starvingAllocated = recomputeAllocatedCapacity(cStarving);
            if (starvingAllocated >= starvingComputationInfo.Consumption) {
                continue; // Already satisfied.
            }
            double starvingStarvation = 1. - starvingAllocated / starvingComputationInfo.Consumption;

            std::optional<TWorkerId> bestWorker;
            std::optional<TComputationId> bestSurplus;
            double bestSurplusLoad = std::numeric_limits<double>::max();

            for (const auto& workerAddress : starvingComputationInfo.TechnicallyPossibleWorkers) {
                if (starvingComputationInfo.Workers.contains(workerAddress)) {
                    continue; // Already hosting cStarving.
                }
                if (!canHostComputation(workerAddress, cStarving)) {
                    continue;
                }

                for (const auto& [cSurplus, surplusComputationInfo] : context.Computations) {
                    if (cSurplus == cStarving) {
                        continue;
                    }
                    if (!surplusComputationInfo.Workers.contains(workerAddress)) {
                        continue;
                    }
                    // cSurplus must currently be non-starving.
                    double surplusAllocated = recomputeAllocatedCapacity(cSurplus);
                    if (surplusAllocated < surplusComputationInfo.Consumption) {
                        continue;
                    }

                    // Check that after losing w, cSurplus would not starve more than cStarving does now.
                    // Temporarily remove w from cSurplus to estimate its new allocated capacity.
                    GetOrCrash(context.Computations, cSurplus).Workers.erase(workerAddress);
                    double surplusAllocatedAfter = recomputeAllocatedCapacity(cSurplus);
                    GetOrCrash(context.Computations, cSurplus).Workers.insert(workerAddress);

                    double surplusStarvationAfter = (surplusComputationInfo.Consumption > 0.)
                        ? std::max(0., 1. - surplusAllocatedAfter / surplusComputationInfo.Consumption)
                        : 0.;
                    if (surplusStarvationAfter > starvingStarvation) {
                        continue; // cSurplus would starve more than cStarving — skip.
                    }

                    double surplusLoadOnWorker = GetOrDefault(
                        GetOrCrash(context.Workers, workerAddress).ComputationLoad,
                        cSurplus,
                        0.);
                    if (surplusLoadOnWorker < bestSurplusLoad) {
                        bestSurplusLoad = surplusLoadOnWorker;
                        bestWorker = workerAddress;
                        bestSurplus = cSurplus;
                    }
                }
            }

            if (!bestWorker || !bestSurplus) {
                YT_LOG_DEBUG("ResourceQueue: Step 3 no viable steal found for starving computation "
                    "(Computation: %v, Starvation: %v)",
                    cStarving,
                    starvingStarvation);
                continue; // No viable steal found.
            }

            YT_LOG_DEBUG("ResourceQueue: Step 3 stealing worker from surplus computation "
                "(StarvingComputation: %v, StarvingStarvation: %v, "
                "SurplusComputation: %v, Worker: %v, SurplusLoadOnWorker: %v)",
                cStarving,
                starvingStarvation,
                *bestSurplus,
                *bestWorker,
                bestSurplusLoad);

            // Remove w from cSurplus (no replacement).
            removeComputationFromWorker(*bestWorker, *bestSurplus);
            GetOrCrash(context.Computations, *bestSurplus).Workers.erase(*bestWorker);

            // Add w to cStarving.
            addComputationToWorker(*bestWorker, cStarving);

            ++movesDone;
        }

        YT_LOG_DEBUG("ResourceQueue: Step 3 finished (MovesDone: %v, MaxMoves: %v)",
            movesDone,
            maxMoves);
    }

    // =========================================================================
    // Step 4: Steal a worker from any computation (with replacement).
    // =========================================================================
    //
    // For each still-starving computation cStarving:
    //   Find a worker w in TechnicallyPossibleWorkers[cStarving] where:
    //     - Any computation cSurplus (including starving ones) is present.
    //     - cSurplus has an alternative worker w2 it can move to.
    //   If found:
    //     - Remove w from cSurplus.Workers.
    //     - Add w2 to cSurplus.Workers (replacement).
    //     - Add w to cStarving.Workers.
    //   Prefer the cSurplus with the smallest load on w (cheapest to move).
    //   Bound: stop after O(numWorkers) total moves.

    starvingQueue = buildStarvingQueue();

    YT_LOG_DEBUG("ResourceQueue: Step 4 starting "
        "(WorkerGroup: %v, StarvingComputations: %v, MaxMoves: %v)",
        workerGroup,
        starvingQueue.size(),
        context.Workers.size());

    {
        int maxMoves = static_cast<int>(context.Workers.size());
        int movesDone = 0;

        while (!starvingQueue.empty() && movesDone < maxMoves) {
            auto [starvation, cStarving] = starvingQueue.top();
            starvingQueue.pop();

            const auto& starvingComputationInfo = GetOrCrash(context.Computations, cStarving);
            if (recomputeAllocatedCapacity(cStarving) >= starvingComputationInfo.Consumption) {
                continue; // Already satisfied.
            }

            std::optional<TWorkerId> bestWorker;
            std::optional<TComputationId> bestSurplus;
            std::optional<TWorkerId> bestAltWorker;
            double bestSurplusLoad = std::numeric_limits<double>::max();

            for (const auto& workerAddress : starvingComputationInfo.TechnicallyPossibleWorkers) {
                if (starvingComputationInfo.Workers.contains(workerAddress)) {
                    continue; // Already hosting cStarving.
                }
                if (!canHostComputation(workerAddress, cStarving)) {
                    continue;
                }

                for (const auto& [cSurplus, surplusComputationInfo] : context.Computations) {
                    if (cSurplus == cStarving) {
                        continue;
                    }
                    if (!surplusComputationInfo.Workers.contains(workerAddress)) {
                        continue;
                    }

                    // Find an alternative worker w2 for cSurplus.
                    std::optional<TWorkerId> altWorker;
                    double altRelativeLoad = std::numeric_limits<double>::max();
                    for (const auto& w2 : surplusComputationInfo.TechnicallyPossibleWorkers) {
                        if (surplusComputationInfo.Workers.contains(w2)) {
                            continue;
                        }
                        if (!canHostComputation(w2, cSurplus)) {
                            continue;
                        }
                        const auto& w2Info = GetOrCrash(context.Workers, w2);
                        double rel = w2Info.TotalLoad / std::max(w2Info.TotalCapacity, kEpsilon);
                        if (rel < altRelativeLoad) {
                            altRelativeLoad = rel;
                            altWorker = w2;
                        }
                    }

                    if (!altWorker) {
                        continue; // cSurplus has nowhere else to go.
                    }

                    double surplusLoadOnWorker = GetOrDefault(
                        GetOrCrash(context.Workers, workerAddress).ComputationLoad,
                        cSurplus,
                        0.);
                    if (surplusLoadOnWorker < bestSurplusLoad) {
                        bestSurplusLoad = surplusLoadOnWorker;
                        bestWorker = workerAddress;
                        bestSurplus = cSurplus;
                        bestAltWorker = altWorker;
                    }
                }
            }

            if (!bestWorker || !bestSurplus || !bestAltWorker) {
                YT_LOG_DEBUG("ResourceQueue: Step 4 no viable redistribution found for starving computation "
                    "(Computation: %v, Starvation: %v)",
                    cStarving,
                    starvation);
                continue; // No viable redistribution found.
            }

            YT_LOG_DEBUG("ResourceQueue: Step 4 stealing worker with replacement "
                "(StarvingComputation: %v, SurplusComputation: %v, "
                "Worker: %v, AltWorker: %v, SurplusLoadOnWorker: %v)",
                cStarving,
                *bestSurplus,
                *bestWorker,
                *bestAltWorker,
                bestSurplusLoad);

            // Move cSurplus from bestWorker to bestAltWorker.
            removeComputationFromWorker(*bestWorker, *bestSurplus);
            GetOrCrash(context.Computations, *bestSurplus).Workers.erase(*bestWorker);
            addComputationToWorker(*bestAltWorker, *bestSurplus);

            // Add bestWorker to cStarving.
            addComputationToWorker(*bestWorker, cStarving);

            ++movesDone;
        }

        YT_LOG_DEBUG("ResourceQueue: Step 4 finished (MovesDone: %v, MaxMoves: %v)",
            movesDone,
            maxMoves);
    }

    // =========================================================================
    // Step 4.5: Spread computations onto idle workers to balance queues.
    // =========================================================================
    //
    // Steps 2-4 add workers only to capacity-starving computations. But a worker
    // that keeps up with its input is never "starving", even when its queue is far
    // larger than an idle worker's. As a result a computation whose partitions are
    // concentrated on a few workers is never spread, and the primary goal of this
    // balancer — equal queue lengths across workers — is not met. Step 8 cannot fix
    // it either: it only moves partitions among workers a computation already occupies.
    //
    // Here we add idle technically-possible workers to such a computation's plan so
    // that Step 5 issues their preloads and Step 8 redistributes partitions onto them
    // once preload completes.
    //
    // Expand computation c onto worker w when:
    //   1. w is in c.TechnicallyPossibleWorkers and not already hosting c.
    //   2. w can host c (capability check).
    //   3. c has more partitions than the workers it currently occupies
    //      (there is something to redistribute).
    //   4. w's total queue is smaller than the largest total queue among c's current
    //      workers by at least RebalanceTargetDeviation (relative), i.e. there is a
    //      meaningful imbalance for w to absorb. With no load all queues are zero, so
    //      nothing is spread until traffic actually concentrates.
    {
        THashMap<TComputationId, int> computationPartitionCount;
        for (const auto& [partitionId, partitionInfo] : context.Partitions) {
            ++computationPartitionCount[partitionInfo.ComputationId];
        }

        for (auto& [computationId, computationInfo] : context.Computations) {
            int partitionCount = GetOrDefault(computationPartitionCount, computationId, 0);

            // Largest total queue among workers currently hosting c.
            double maxHostQueue = 0.;
            for (const auto& workerAddress : computationInfo.Workers) {
                maxHostQueue = std::max(
                    maxHostQueue,
                    GetOrCrash(context.Workers, workerAddress).TotalQueueSize);
            }

            // Candidate workers not yet hosting c, least-loaded first.
            std::vector<TWorkerId> candidates;
            for (const auto& workerAddress : computationInfo.TechnicallyPossibleWorkers) {
                if (!computationInfo.Workers.contains(workerAddress)) {
                    candidates.push_back(workerAddress);
                }
            }
            std::sort(candidates.begin(), candidates.end(), [&] (const TWorkerId& a, const TWorkerId& b) {
                return GetOrCrash(context.Workers, a).TotalLoad < GetOrCrash(context.Workers, b).TotalLoad;
            });

            for (const auto& workerAddress : candidates) {
                if (std::ssize(computationInfo.Workers) >= partitionCount) {
                    break; // No partitions left to give to a new worker.
                }
                if (!canHostComputation(workerAddress, computationId)) {
                    continue;
                }
                const auto& candidateInfo = GetOrCrash(context.Workers, workerAddress);
                if (maxHostQueue <= kEpsilon ||
                    (maxHostQueue - candidateInfo.TotalQueueSize) / maxHostQueue < balancerSpec->RebalanceTargetDeviation)
                {
                    continue; // Imbalance too small (relative to the busiest worker) to justify spreading.
                }

                YT_LOG_DEBUG("ResourceQueue: Step 4.5 spreading computation onto idle worker "
                    "(Computation: %v, Worker: %v, WorkerQueue: %v, MaxHostQueue: %v, "
                    "Partitions: %v, WorkersBefore: %v)",
                    computationId,
                    workerAddress,
                    candidateInfo.TotalQueueSize,
                    maxHostQueue,
                    partitionCount,
                    computationInfo.Workers.size());

                addComputationToWorker(workerAddress, computationId);
            }
        }
    }

    // =========================================================================
    // Step 5: Compute preload actions.
    // =========================================================================
    //
    // Based on the virtual computation-to-worker mapping (computationInfo.Workers),
    // determine which preloadable resources need to be started or stopped on
    // which workers.
    //
    // desiredWorkerResources[w] = union of all resources needed by all computations
    // whose Workers set includes w.
    // This is built from computationInfo.Workers (the virtual plan from Steps 2-4) and
    // computationInfo.ResourceConsumptionMultiplier (resources required by each computation per spec).
    //
    // For each worker w:
    //   - For each preloadable resource r in desiredWorkerResources[w] that is NOT
    //     in PreloadResourceIssued[w] and NOT in PreloadResourceCompleted[w]:
    //     emit TWorkerPreloadResultAction{Add, r, w}.
    //   - For each preloadable resource r in PreloadResourceIssued[w] that is NOT
    //     in desiredWorkerResources[w]:
    //     emit TWorkerPreloadResultAction{Del, r, w}.
    //
    // Also build workerPreloadReady[w]: set of preloadable resources that are
    // completed on w (used in Step 6 to filter eligible workers).

    // Build desiredWorkerResources[w] from the virtual computation-to-worker mapping.
    // computationInfo.Workers = workers virtually assigned to c (from Steps 2-4).
    // computationInfo.ResourceConsumptionMultiplier = resources required by c (from spec via CollectResourceContext).
    THashMap<TWorkerId, THashSet<TResourceId>> desiredWorkerResources;
    for (const auto& [computationId, computationInfo] : context.Computations) {
        for (const auto& workerAddress : computationInfo.Workers) {
            for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
                desiredWorkerResources[workerAddress].insert(resourceId);
            }
        }
    }

    // Emit preload Add/Del actions and build workerPreloadReady.
    // workerPreloadReady[w]: set of preloadable resources that are completed on w.
    THashMap<TWorkerId, THashSet<TResourceId>> workerPreloadReady;
    for (const auto& [workerAddress, workerInfo] : context.Workers) {
        // Populate ready set from completed preloads.
        for (const auto& resourceId : workerInfo.PreloadResourceCompleted) {
            workerPreloadReady[workerAddress].insert(resourceId);
        }

        const auto& desired = desiredWorkerResources[workerAddress];

        // Emit Add for preloadable resources that are desired but not yet issued/completed.
        for (const auto& resourceId : desired) {
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end() || !resourceIt->second.IsPreloadable) {
                continue; // Not preloadable.
            }
            if (workerInfo.PreloadResourceIssued.contains(resourceId) ||
                workerInfo.PreloadResourceCompleted.contains(resourceId))
            {
                continue; // Already issued or completed.
            }
            YT_LOG_DEBUG("ResourceQueue: Step 5 emitting preload Add "
                "(Worker: %v, Resource: %v)",
                workerAddress,
                resourceId);
            rebalanceResult.PreloadResourceActions.push_back(TWorkerPreloadResultAction{
                .Type = ERebalanceActionType::Add,
                .ResourceId = resourceId,
                .WorkerAddress = workerAddress,
            });
        }

        // Emit Del for preloadable resources that are issued but no longer desired.
        for (const auto& resourceId : workerInfo.PreloadResourceIssued) {
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end() || !resourceIt->second.IsPreloadable) {
                continue;
            }
            if (desired.contains(resourceId)) {
                continue; // Still desired.
            }
            YT_LOG_DEBUG("ResourceQueue: Step 5 emitting preload Del "
                "(Worker: %v, Resource: %v)",
                workerAddress,
                resourceId);
            rebalanceResult.PreloadResourceActions.push_back(TWorkerPreloadResultAction{
                .Type = ERebalanceActionType::Del,
                .ResourceId = resourceId,
                .WorkerAddress = workerAddress,
            });
        }
    }

    // =========================================================================
    // Step 6: Distribute stray partitions.
    // =========================================================================
    //
    // Assign partitions that have no worker (computationInfo.StrayPartitions) to workers,
    // respecting the virtual computation-to-worker mapping from Steps 2-4.
    //
    // Eligible workers for computation c:
    //   - Must be in computationInfo.Workers.
    //   - For each preloadable resource required by c, it must be in
    //     workerPreloadReady[w] (i.e., preload completed).
    //   - Non-preloadable resources are always ready.
    //
    // Assignment strategy:
    //   - Sort stray partitions of c by Rps descending (assign heavy ones first).
    //   - Use a min-heap of eligible workers keyed by current workerInfo.TotalLoad.
    //   - For each stray partition, pop the least-loaded eligible worker, emit Add,
    //     update workerInfo.TotalLoad, push worker back.
    //   - If no eligible worker exists (all preloads pending), leave partition stray.

    // Per-worker per-computation partition lists.
    // Seeded here from existing assigned partitions; also updated in Step 6 as stray
    // partitions are assigned, so that Step 8 can equalize all partitions (not just
    // the ones that were already assigned before this balancing round).
    THashMap<TWorkerId, THashMap<TComputationId, std::vector<TPartitionId>>> workerComputationPartitions;
    for (const auto& [partitionId, partitionInfo] : context.Partitions) {
        if (!partitionInfo.WorkerId) {
            continue;
        }
        workerComputationPartitions[*partitionInfo.WorkerId][partitionInfo.ComputationId].push_back(partitionId);
    }

    // Helper: check if worker w is preload-ready for computation c.
    // All preloadable resources required by c must be completed on w.
    auto isPreloadReady = [&] (const TWorkerId& workerAddress, const TComputationId& computationId) -> bool {
        const auto& computationInfo = GetOrCrash(context.Computations, computationId);
        const auto& ready = workerPreloadReady[workerAddress];
        for (const auto& [resourceId, consumptionMultiplier] : computationInfo.ResourceConsumptionMultiplier) {
            auto resourceIt = context.Resources.find(resourceId);
            if (resourceIt == context.Resources.end()) {
                continue;
            }
            if (!resourceIt->second.IsPreloadable) {
                continue; // Non-preloadable resources are always ready.
            }
            if (!ready.contains(resourceId)) {
                return false; // Preload not yet completed.
            }
        }
        return true;
    };

    // Process each computation's stray partitions.
    for (auto& [computationId, computationInfo] : context.Computations) {
        auto& partitions = computationInfo.StrayPartitions;
        if (partitions.empty()) {
            continue;
        }

        // Build list of eligible workers for this computation.
        // Eligible = in computationInfo.Workers AND preload-ready.
        const auto& assignedWorkers = computationInfo.Workers;

        // Min-heap: (TotalLoad, workerAddress).
        using TWorkerLoadEntry = std::pair<double, TWorkerId>;
        std::priority_queue<TWorkerLoadEntry, std::vector<TWorkerLoadEntry>, std::greater<>> workerHeap;

        for (const auto& workerAddress : assignedWorkers) {
            if (!isPreloadReady(workerAddress, computationId)) {
                YT_LOG_DEBUG("ResourceQueue: Step 6 worker not preload-ready, skipping "
                    "(Computation: %v, Worker: %v)",
                    computationId,
                    workerAddress);
                continue; // Preload not yet completed — skip.
            }
            workerHeap.emplace(GetOrCrash(context.Workers, workerAddress).TotalLoad, workerAddress);
        }

        if (workerHeap.empty()) {
            YT_LOG_DEBUG("ResourceQueue: Step 6 no eligible workers for stray partitions "
                "(Computation: %v, StrayPartitions: %v, AssignedWorkers: %v)",
                computationId,
                partitions.size(),
                assignedWorkers.size());
            continue; // No eligible workers — leave partitions stray for next round.
        }

        YT_LOG_DEBUG("ResourceQueue: Step 6 distributing stray partitions "
            "(Computation: %v, StrayPartitions: %v, EligibleWorkers: %v)",
            computationId,
            partitions.size(),
            workerHeap.size());

        // Sort stray partitions by Rps descending (assign heavy partitions first).
        std::sort(partitions.begin(), partitions.end(), [&] (const TPartitionId& a, const TPartitionId& b) {
            return context.Partitions.at(a).Rps > context.Partitions.at(b).Rps;
        });

        for (const auto& partitionId : partitions) {
            // Pop the least-loaded eligible worker.
            auto [load, workerAddress] = workerHeap.top();
            workerHeap.pop();

            // Emit Add action.
            rebalanceResult.Actions.push_back(TRebalanceResultAction{
                .Type = ERebalanceActionType::Add,
                .PartitionId = partitionId,
                .WorkerAddress = workerAddress,
            });

            // Update load tracking.
            const auto& partitionInfo = context.Partitions.at(partitionId);
            double addedLoad = partitionInfo.Rps * computationInfo.TotalConsumptionMultiplier;

            YT_LOG_DEBUG("ResourceQueue: Step 6 assigning stray partition "
                "(Computation: %v, Partition: %v, Worker: %v, Rps: %v, AddedLoad: %v)",
                computationId,
                partitionId,
                workerAddress,
                partitionInfo.Rps,
                addedLoad);

            GetOrCrash(context.Workers, workerAddress).TotalLoad += addedLoad;

            // Register the newly assigned partition in workerComputationPartitions so that
            // Step 8 (queue equalization) can consider moving it between workers.
            workerComputationPartitions[workerAddress][computationId].push_back(partitionId);

            // Push worker back with updated load.
            workerHeap.emplace(GetOrCrash(context.Workers, workerAddress).TotalLoad, workerAddress);
        }
    }

    // =========================================================================
    // Step 7: Compute projected queue metric (baseline).
    // =========================================================================
    //
    // For each worker w, compute the projected average queue over PlanningHorizon.

    // Helper: compute the projected average queue over PlanningHorizon for given worker.
    // Let q(t) be the queue size starting from time 0.
    // The projected average is integral q(t) over [0, PlanningHorizon) divided by PlanningHorizon.
    // Case 1. The queue grows or drains, but not becoming zero before the timespan ends.
    //   q(t) = queueSize + excessRate * t.
    //   The average is (q(0) / q(PlanningHorizon)) / 2.
    // Case 2. If the queue drains, it may become zero before the timespan ends.
    //   q(t) = max(0, queueSize + excessRate * t).
    //   The integral of q(t) is an area of right triangle with legs queueSize and drainTime.
    auto computeProjectedAvgQueue = [&] (const TWorkerId& workerAddress, double totalLoad) -> double {
        const auto& workerInfo = GetOrCrash(context.Workers, workerAddress);
        double queueSize = workerInfo.TotalQueueSize;
        double capacity = workerInfo.TotalCapacity;
        double excessRate = totalLoad - capacity; // >0 - queue grows, <0 - queue drains.
        if (excessRate > -kEpsilon) {
            // Queue grows.
            return queueSize + excessRate * planningHorizonSeconds / 2.;
        } else {
            // Queue drains.
            double drainRate = -excessRate; // >0 (moreover >=kEpsilon).
            double drainTime = queueSize / drainRate;
            if (drainTime <= planningHorizonSeconds) {
                // Queue fully drains within the horizon.
                return queueSize * drainTime / (2. * planningHorizonSeconds);
            } else {
                // Queue simply drains.
                return queueSize + excessRate * planningHorizonSeconds / 2.;
            }
        }
    };

    // Helper: compute coefficient of variation of projected queues across workers
    // that have at least one partition.
    auto computeQueueMetric = [&] (const THashMap<TWorkerId, double>& loads) -> double {
        std::vector<double> projectedQueues;
        projectedQueues.reserve(loads.size());
        for (const auto& [workerAddress, load] : loads) {
            // Only include workers that have partitions (existing or newly assigned).
            bool hasPartitions = false;
            const auto& wi = GetOrCrash(context.Workers, workerAddress);
            if (!wi.ComputationLoad.empty()) {
                hasPartitions = true;
            }
            if (!hasPartitions) {
                for (const auto& action : rebalanceResult.Actions) {
                    if (action.WorkerAddress == workerAddress && action.Type == ERebalanceActionType::Add) {
                        hasPartitions = true;
                        break;
                    }
                }
            }
            if (!hasPartitions) {
                continue;
            }
            projectedQueues.push_back(computeProjectedAvgQueue(workerAddress, load));
        }
        if (projectedQueues.size() < 2) {
            return 0.;
        }
        double mean = Accumulate(projectedQueues, 0.) / static_cast<double>(projectedQueues.size());
        if (mean <= kEpsilon) {
            return 0.;
        }
        double sumSq = 0.;
        for (double q : projectedQueues) {
            sumSq += (q - mean) * (q - mean);
        }
        double stddev = std::sqrt(sumSq / static_cast<double>(projectedQueues.size()));
        return stddev / mean;
    };

    THashMap<TWorkerId, double> workerTotalLoadMap;
    for (const auto& [workerAddress, workerInfo] : context.Workers) {
        workerTotalLoadMap[workerAddress] = workerInfo.TotalLoad;
    }
    double baselineMetric = computeQueueMetric(workerTotalLoadMap);

    YT_LOG_DEBUG("ResourceQueue: Step 7 baseline queue metric computed "
        "(WorkerGroup: %v, BaselineMetric: %v)",
        workerGroup,
        baselineMetric);

    // =========================================================================
    // Step 8: Greedy queue equalization by moving partitions.
    // =========================================================================
    //
    // For each computation c, try to move partitions between its workers to
    // reduce the spread of projected queues.
    //
    // For each computation c:
    //   - Find wHigh (highest projected queue) and wLow (lowest projected queue)
    //     among preload-ready workers of c.
    //   - Find the partition on wHigh whose load is closest to the ideal transfer:
    //     idealTransferLoad = (projectedQueue[wHigh] - projectedQueue[wLow]) / (2 * planningHorizon)
    //   - Tentatively move it: emit Del from wHigh, Add to wLow.
    //   - Repeat up to max(numWorkers, maxPartitionsOnAWorker) moves per computation, so the
    //     busiest worker can actually be drained (numWorkers alone is far too few).

    std::vector<TRebalanceResultAction> tentativeMoves;
    THashMap<TWorkerId, double> equalizationLoads;
    for (const auto& [workerAddress, workerInfo] : context.Workers) {
        equalizationLoads[workerAddress] = workerInfo.TotalLoad;
    }

    for (const auto& [computationId, computationInfo] : context.Computations) {
        const auto& workers = computationInfo.Workers;
        if (workers.size() < 2) {
            continue;
        }

        // numWorkers moves per round is far too few to drain a backlogged worker that holds many
        // partitions: convergence then takes dozens of rounds. Allow as many moves as the busiest
        // worker has partitions, so a single round can rebalance it.
        int maxPartitionsPerWorker = 0;
        for (const auto& workerAddress : workers) {
            auto wIt = workerComputationPartitions.find(workerAddress);
            if (wIt == workerComputationPartitions.end()) {
                continue;
            }
            auto cIt = wIt->second.find(computationId);
            if (cIt != wIt->second.end()) {
                maxPartitionsPerWorker =
                    std::max(maxPartitionsPerWorker, static_cast<int>(cIt->second.size()));
            }
        }
        int maxEqualizationMoves = std::max(static_cast<int>(context.Workers.size()), maxPartitionsPerWorker);

        int movesThisComputation = 0;
        bool improved = true;
        while (improved && movesThisComputation < maxEqualizationMoves) {
            improved = false;

            std::optional<TWorkerId> wHigh, wLow;
            double highQueue = -1., lowQueue = std::numeric_limits<double>::max();

            for (const auto& workerAddress : workers) {
                if (!isPreloadReady(workerAddress, computationId)) {
                    continue;
                }
                double q = computeProjectedAvgQueue(workerAddress, equalizationLoads[workerAddress]);
                if (q > highQueue) {
                    highQueue = q;
                    wHigh = workerAddress;
                }
                if (q < lowQueue) {
                    lowQueue = q;
                    wLow = workerAddress;
                }
            }

            if (!wHigh || !wLow || *wHigh == *wLow) {
                break;
            }

            double idealTransferLoad = (highQueue - lowQueue) / (2. * planningHorizonSeconds);
            if (idealTransferLoad <= 0.) {
                break;
            }

            auto partitionsIt = workerComputationPartitions.find(*wHigh);
            if (partitionsIt == workerComputationPartitions.end()) {
                break;
            }
            auto compPartitionsIt = partitionsIt->second.find(computationId);
            if (compPartitionsIt == partitionsIt->second.end() || compPartitionsIt->second.empty()) {
                break;
            }

            std::optional<TPartitionId> bestPartition;
            double bestDiff = std::numeric_limits<double>::max();
            for (const auto& partitionId : compPartitionsIt->second) {
                const auto& partitionInfo = context.Partitions.at(partitionId);
                // Skip partitions that are still warming up — their RPS is not yet
                // representative and moving them would cause oscillation.
                if (partitionInfo.TimeSinceStart < PartitionWarmupPeriod) {
                    continue;
                }
                double partLoad = partitionInfo.Rps * computationInfo.TotalConsumptionMultiplier;
                double diff = std::abs(partLoad - idealTransferLoad);
                if (diff < bestDiff) {
                    bestDiff = diff;
                    bestPartition = partitionId;
                }
            }

            if (!bestPartition) {
                break;
            }

            const auto& partitionInfo = context.Partitions.at(*bestPartition);
            double movedLoad = partitionInfo.Rps * computationInfo.TotalConsumptionMultiplier;

            // Tentatively apply the move and check whether the spread actually decreases.
            double newHighQueue = computeProjectedAvgQueue(*wHigh, equalizationLoads[*wHigh] - movedLoad);
            double newLowQueue = computeProjectedAvgQueue(*wLow, equalizationLoads[*wLow] + movedLoad);
            double newSpread = std::abs(newHighQueue - newLowQueue);
            double oldSpread = highQueue - lowQueue; // highQueue >= lowQueue by construction.

            if (newSpread >= oldSpread) {
                YT_LOG_DEBUG("ResourceQueue: Step 8 move does not reduce spread, stopping "
                    "(Computation: %v, wHigh: %v, wLow: %v, "
                    "OldSpread: %v, NewSpread: %v, Partition: %v, MovedLoad: %v)",
                    computationId,
                    *wHigh,
                    *wLow,
                    oldSpread,
                    newSpread,
                    *bestPartition,
                    movedLoad);
                break; // Move doesn't reduce the spread — stop for this computation.
            }

            YT_LOG_DEBUG("ResourceQueue: Step 8 equalization move "
                "(Computation: %v, Partition: %v, From: %v, To: %v, "
                "MovedLoad: %v, IdealTransferLoad: %v, OldSpread: %v, NewSpread: %v)",
                computationId,
                *bestPartition,
                *wHigh,
                *wLow,
                movedLoad,
                idealTransferLoad,
                oldSpread,
                newSpread);

            equalizationLoads[*wHigh] -= movedLoad;
            equalizationLoads[*wLow] += movedLoad;

            auto& highPartitions = workerComputationPartitions[*wHigh][computationId];
            highPartitions.erase(
                std::remove(highPartitions.begin(), highPartitions.end(), *bestPartition),
                highPartitions.end());
            workerComputationPartitions[*wLow][computationId].push_back(*bestPartition);

            tentativeMoves.push_back(TRebalanceResultAction{
                .Type = ERebalanceActionType::Del,
                .PartitionId = *bestPartition,
                .WorkerAddress = *wHigh,
            });
            tentativeMoves.push_back(TRebalanceResultAction{
                .Type = ERebalanceActionType::Add,
                .PartitionId = *bestPartition,
                .WorkerAddress = *wLow,
            });

            improved = true;
            ++movesThisComputation;
        }
    }

    // =========================================================================
    // Step 9: Evaluate and possibly revert moves.
    // =========================================================================
    //
    // If relative improvement >= RebalanceTargetDeviation, keep the tentative moves.
    // Otherwise discard them.

    double newMetric = computeQueueMetric(equalizationLoads);
    // Accept on the ABSOLUTE drop of the (dimensionless) queue CV, not the relative improvement.
    // A relative test (baseline-new)/baseline accepts micro-moves once baselineMetric is already near
    // zero (queues balanced) — any tiny absolute change is a large fraction of near-zero — causing
    // perpetual noise-chasing churn. Requiring an absolute drop >= RebalanceTargetDeviation leaves
    // already-balanced groups alone while still rebalancing genuinely imbalanced ones (large baseline).
    bool equalizationAccepted = !tentativeMoves.empty() &&
        (baselineMetric - newMetric) >= balancerSpec->RebalanceTargetDeviation;

    YT_LOG_DEBUG("ResourceQueue: Step 9 equalization evaluation "
        "(WorkerGroup: %v, TentativeMoves: %v, BaselineMetric: %v, NewMetric: %v, "
        "RelativeImprovement: %v, Threshold: %v, Accepted: %v)",
        workerGroup,
        tentativeMoves.size(),
        baselineMetric,
        newMetric,
        baselineMetric > kEpsilon ? (baselineMetric - newMetric) / baselineMetric : 0.,
        balancerSpec->RebalanceTargetDeviation,
        equalizationAccepted);

    if (equalizationAccepted) {
        for (auto& action : tentativeMoves) {
            rebalanceResult.Actions.push_back(std::move(action));
        }
    }

    // =========================================================================
    // Step 10: Remove redundant Add+Del pairs for the same partition on the same worker.
    // =========================================================================

    CompactRebalanceActions(rebalanceResult.Actions);

    // =========================================================================
    // Well done.
    // =========================================================================

    YT_LOG_INFO("ResourceQueue: balancing complete "
        "(WorkerGroup: %v, PartitionActions: %v, PreloadActions: %v)",
        workerGroup,
        rebalanceResult.Actions.size(),
        rebalanceResult.PreloadResourceActions.size());

    return rebalanceResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
