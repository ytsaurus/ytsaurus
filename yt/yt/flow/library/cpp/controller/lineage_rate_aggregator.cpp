#include "lineage_rate_aggregator.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <cmath>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto LineageRateAggregationPeriod = TDuration::Minutes(1);

void AddRates(
    TLineageRates* aggregate,
    const TLineageRates& rates,
    const THashMap<TStreamId, THashSet<TStreamId>>& allowedEdges,
    double factor)
{
    auto addRate = [factor] (std::optional<double>* aggregateValue, const std::optional<double>& value) {
        if (value) {
            *aggregateValue = aggregateValue->value_or(0) + *value * factor;
        }
    };
    for (const auto& [outputStreamId, parentRates] : rates) {
        const auto* allowedParents = allowedEdges.FindPtr(outputStreamId);
        if (!allowedParents) {
            continue;
        }
        for (const auto& [parentStreamId, rate] : parentRates) {
            if (!allowedParents->contains(parentStreamId)) {
                continue;
            }
            auto& aggregateRate = (*aggregate)[outputStreamId][parentStreamId];
            // A legacy or incomplete observation must not contribute only one side of a ratio.
            if (rate.CountPerSecond && rate.InputCountPerSecond) {
                addRate(&aggregateRate.CountPerSecond, rate.CountPerSecond);
                addRate(&aggregateRate.InputCountPerSecond, rate.InputCountPerSecond);
            }
            if (rate.BytesPerSecond && rate.InputBytesPerSecond) {
                addRate(&aggregateRate.BytesPerSecond, rate.BytesPerSecond);
                addRate(&aggregateRate.InputBytesPerSecond, rate.InputBytesPerSecond);
            }
        }
    }
}

} // namespace

void TLineageRateAggregator::AddWorkerRates(
    TIncarnationId workerIncarnationId,
    TLineageRates rates)
{
    auto guard = Guard(PendingWorkerRatesLock_);
    PendingWorkerRates_[workerIncarnationId] = std::move(rates);
}

void TLineageRateAggregator::Update(
    const TFlowViewPtr& flowView,
    TInstant now)
{
    THashSet<TIncarnationId> activeWorkerIncarnations;
    for (const auto& worker : GetValues(flowView->State->Workers)) {
        activeWorkerIncarnations.insert(worker->IncarnationId);
    }

    THashMap<TIncarnationId, TLineageRates> pendingWorkerRates;
    {
        auto guard = Guard(PendingWorkerRatesLock_);
        std::swap(pendingWorkerRates, PendingWorkerRates_);
    }
    for (auto& [workerIncarnationId, rates] : pendingWorkerRates) {
        if (!activeWorkerIncarnations.contains(workerIncarnationId) &&
            !WorkerSnapshots_.contains(workerIncarnationId))
        {
            continue;
        }
        auto& snapshot = WorkerSnapshots_[workerIncarnationId];
        snapshot.Rates = std::move(rates);
    }

    const auto pipelineSpecVersion = flowView->CurrentSpec->GetVersion();
    if (now < NextAggregationTime_ && pipelineSpecVersion == LastPipelineSpecVersion_) {
        return;
    }
    NextAggregationTime_ = now + LineageRateAggregationPeriod;
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

    TLineageRates aggregate;
    for (auto it = WorkerSnapshots_.begin(); it != WorkerSnapshots_.end();) {
        const bool active = activeWorkerIncarnations.contains(it->first);
        if (active) {
            it->second.InactiveSince.reset();
            AddRates(&aggregate, it->second.Rates, allowedEdges, 1.0);
            ++it;
            continue;
        }

        if (!it->second.InactiveSince) {
            it->second.InactiveSince = now;
        }
        const auto age = now - *it->second.InactiveSince;
        if (age >= LineageRateRetentionTime) {
            auto staleIt = it++;
            WorkerSnapshots_.erase(staleIt);
            continue;
        }

        const double factor = std::exp(-age.SecondsFloat() / (LineageRateDecayTime.SecondsFloat() / 2.0));
        AddRates(&aggregate, it->second.Rates, allowedEdges, factor);
        ++it;
    }

    flowView->EphemeralState->LineageRates = std::move(aggregate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
