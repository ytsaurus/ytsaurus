#include "private.h"

#include "job_balancer_common.h"
#include "job_balancer_greedy.h"

#include <util/generic/set.h>

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

TRebalanceResult DoBalanceGreedy(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, IComputationControllerPtr>& controllers,
    const TWorkerGroupId& workerGroup)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    NBalancer::TRebalanceResult rebalanceResult;

    THashMap<TComputationId, double> totalWeightPerComputation;
    THashMap<TComputationId, THashSet<TPartitionId>> freePartitions;
    THashMap<TPartitionId, double> weights;
    for (const auto& [partitionId, partition] : layout->Partitions) {
        if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
            if (!ComputationBelongsToGroup(GetOrCrash(flowView->CurrentSpec->GetValue()->Computations, partition->ComputationId), workerGroup)) {
                continue;
            }
            freePartitions[partition->ComputationId].insert(partitionId);
            auto controller = GetOrCrash(controllers, partition->ComputationId);
            double weight = controller->ComputePartitionWeight(partitionId, flowView);
            THROW_ERROR_EXCEPTION_IF(std::isnan(weight), "Partition weight is none");
            weights[partitionId] = weight;
            totalWeightPerComputation[partition->ComputationId] += weight;
        }
    }

    struct TWorkerStats
    {
        double Weight{};
        TSet<std::pair<double, TJobId>> OldJobs;
    };

    THashMap<TComputationId, THashMap<std::string, TWorkerStats>> workerStats;

    for (const auto& [jobId, job] : layout->Jobs) {
        auto partition = GetOrCrash(layout->Partitions, job->PartitionId);
        auto& stats = workerStats[partition->ComputationId][job->WorkerAddress];
        stats.Weight += weights[job->PartitionId];
        stats.OldJobs.insert(std::pair(weights[job->PartitionId], jobId));
        freePartitions[partition->ComputationId].erase(job->PartitionId);
    }

    for (const auto& [computationId, totalWeight] : totalWeightPerComputation) {
        auto& stats = workerStats[computationId];

        TSet<std::tuple<double, ui64, NYT::NFlow::TWorkerPtr>> workers;
        for (const auto& [address, w] : flowView->State->Workers) {
            if (!WorkerBelongsToGroup(w, workerGroup)) {
                continue;
            }
            workers.insert(
                std::tuple(
                    stats[address].Weight,
                    MultiHash(w->RpcAddress, computationId),
                    w));
        }
        if (workers.empty()) {
            continue;
        }
        double averageWeight = totalWeight / workers.size();
        for (const auto& partitionId : freePartitions[computationId]) {
            double weight = weights[partitionId];
            auto [currWeight, hash, targetWorker] = *workers.begin();

            rebalanceResult.Actions.push_back(NBalancer::TRebalanceResultAction{
                .Type = NBalancer::ERebalanceActionType::Add,
                .PartitionId = partitionId,
                .WorkerAddress = targetWorker->RpcAddress});

            stats[targetWorker->RpcAddress].Weight += weight;
            workers.erase(workers.begin());
            workers.insert(
                std::tuple(
                    stats[targetWorker->RpcAddress].Weight,
                    hash,
                    targetWorker));
        }

        if (workers.size() >= 2) {
            while (true) {
                auto minWorkerIter = workers.begin();
                auto maxWorkerIter = std::prev(workers.end());
                auto [minWeight, minHash, minWorker] = *minWorkerIter;
                auto [maxWeight, maxHash, maxWorker] = *maxWorkerIter;
                if (maxWeight <= averageWeight) {
                    break;
                }
                if (minWeight >= averageWeight) {
                    break;
                }
                double freeWeight = averageWeight - minWeight;
                auto& oldJobs = stats[maxWorker->RpcAddress].OldJobs;
                if (oldJobs.size() == 0) {
                    break;
                }
                auto jobCandidate = oldJobs.lower_bound(std::pair(freeWeight, TJobId{}));
                if (jobCandidate != oldJobs.begin()) {
                    jobCandidate = std::prev(jobCandidate);
                } else {
                    break;
                }
                TJobId jobId = jobCandidate->second;
                double weight = jobCandidate->first;
                auto partitionId = GetOrCrash(layout->Jobs, jobId)->PartitionId;

                rebalanceResult.Actions.push_back(NBalancer::TRebalanceResultAction{
                    .Type = NBalancer::ERebalanceActionType::Del,
                    .PartitionId = partitionId,
                    .WorkerAddress = maxWorker->RpcAddress});
                rebalanceResult.Actions.push_back(NBalancer::TRebalanceResultAction{
                    .Type = NBalancer::ERebalanceActionType::Add,
                    .PartitionId = partitionId,
                    .WorkerAddress = minWorker->RpcAddress});

                stats[minWorker->RpcAddress].Weight += weight;
                stats[maxWorker->RpcAddress].Weight -= weight;
                oldJobs.erase(jobCandidate);

                workers.erase(minWorkerIter);
                workers.erase(maxWorkerIter);
                workers.insert(std::tuple(stats[minWorker->RpcAddress].Weight, minHash, minWorker));
                workers.insert(std::tuple(stats[maxWorker->RpcAddress].Weight, maxHash, maxWorker));
            }
        }
    }
    return rebalanceResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
