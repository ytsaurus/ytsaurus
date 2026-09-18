#include "lineage_aggregator.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <cmath>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto LineageAggregationPeriod = TDuration::Minutes(1);

void AddRatios(
    TLineageRatios* aggregate,
    const TLineageRatios& ratios,
    const THashMap<TStreamId, THashSet<TStreamId>>& allowedEdges,
    double factor)
{
    auto merge = [factor] (
        std::optional<TWeightedRatio>* aggregateValue,
        const std::optional<TWeightedRatio>& value) {
        if (!value || value->Weight <= 0) {
            return;
        }
        auto weight = value->Weight * factor;
        if (!*aggregateValue) {
            *aggregateValue = *value;
            (*aggregateValue)->Weight = weight;
            return;
        }
        auto& result = **aggregateValue;
        auto totalWeight = result.Weight + weight;
        result.Ratio += (value->Ratio - result.Ratio) * (weight / totalWeight);
        result.Weight = totalWeight;
    };
    for (const auto& [outputStreamId, parentRatios] : ratios) {
        const auto* allowedParents = allowedEdges.FindPtr(outputStreamId);
        if (!allowedParents) {
            continue;
        }
        for (const auto& [parentStreamId, ratio] : parentRatios) {
            if (!allowedParents->contains(parentStreamId)) {
                continue;
            }
            auto& aggregateRatio = (*aggregate)[outputStreamId][parentStreamId];
            merge(&aggregateRatio.Count, ratio.Count);
            merge(&aggregateRatio.ByteSize, ratio.ByteSize);
        }
    }
}

} // namespace

void TLineageAggregator::AddWorkerRatios(
    TIncarnationId workerIncarnationId,
    TLineageRatios ratios)
{
    auto guard = Guard(PendingWorkerRatiosLock_);
    PendingWorkerRatios_[workerIncarnationId] = std::move(ratios);
}

void TLineageAggregator::Update(
    const TFlowViewPtr& flowView,
    TInstant now)
{
    THashSet<TIncarnationId> activeWorkerIncarnations;
    for (const auto& worker : GetValues(flowView->State->Workers)) {
        activeWorkerIncarnations.insert(worker->IncarnationId);
    }

    THashMap<TIncarnationId, TLineageRatios> pendingWorkerRatios;
    {
        auto guard = Guard(PendingWorkerRatiosLock_);
        std::swap(pendingWorkerRatios, PendingWorkerRatios_);
    }
    for (auto& [workerIncarnationId, ratios] : pendingWorkerRatios) {
        if (!activeWorkerIncarnations.contains(workerIncarnationId) &&
            !WorkerSnapshots_.contains(workerIncarnationId))
        {
            continue;
        }
        auto& snapshot = WorkerSnapshots_[workerIncarnationId];
        snapshot.Ratios = std::move(ratios);
    }

    const auto pipelineSpecVersion = flowView->CurrentSpec->GetVersion();
    if (now < NextAggregationTime_ && pipelineSpecVersion == LastPipelineSpecVersion_) {
        return;
    }
    NextAggregationTime_ = now + LineageAggregationPeriod;
    LastPipelineSpecVersion_ = pipelineSpecVersion;

    THashMap<TStreamId, THashSet<TStreamId>> allowedEdges;
    for (const auto& [computationId, computationSpec] : flowView->CurrentSpec->GetValue()->Computations) {
        for (const auto& [localOutputStreamId, localParentStreamIds] : computationSpec->StreamsDependency) {
            const auto outputStreamId = MakeGlobalStreamId(computationId, localOutputStreamId, computationSpec);
            auto& allowedParents = allowedEdges[outputStreamId];
            for (const auto& localParentStreamId : localParentStreamIds) {
                allowedParents.insert(MakeGlobalStreamId(computationId, localParentStreamId, computationSpec));
            }
        }
    }

    TLineageRatios aggregate;
    for (auto it = WorkerSnapshots_.begin(); it != WorkerSnapshots_.end();) {
        const bool active = activeWorkerIncarnations.contains(it->first);
        if (active) {
            it->second.InactiveSince.reset();
            AddRatios(&aggregate, it->second.Ratios, allowedEdges, 1.0);
            ++it;
            continue;
        }

        if (!it->second.InactiveSince) {
            it->second.InactiveSince = now;
        }
        const auto age = now - *it->second.InactiveSince;
        if (age >= LineageRetentionTime) {
            auto staleIt = it++;
            WorkerSnapshots_.erase(staleIt);
            continue;
        }

        const double factor = std::exp(-age.SecondsFloat() / LineageDecayTime.SecondsFloat());
        AddRatios(&aggregate, it->second.Ratios, allowedEdges, factor);
        ++it;
    }

    flowView->EphemeralState->LineageRatios = std::move(aggregate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
