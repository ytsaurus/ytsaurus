#include "lineage_tracker.h"

#include <yt/yt/flow/library/cpp/common/job_lineage_tracker.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TJobLineageTracker
    : public IJobLineageTracker
{
public:
    TJobLineageTracker(
        TLineageTrackerPtr lineageTracker,
        TComputationId computationId,
        TComputationSpecPtr computationSpec)
        : LineageTracker_(std::move(lineageTracker))
        , ComputationId_(std::move(computationId))
        , ComputationSpec_(std::move(computationSpec))
    { }

    void Add(TLineageDelta delta) override
    {
        LineageTracker_->Add(ComputationId_, ComputationSpec_, delta);
    }

private:
    const TLineageTrackerPtr LineageTracker_;
    const TComputationId ComputationId_;
    const TComputationSpecPtr ComputationSpec_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TLineageTracker::Add(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta)
{
    auto guard = Guard(Lock_);
    DoAdd(computationId, computationSpec, delta, TInstant::Now());
}

void TLineageTracker::Add(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta,
    TInstant now)
{
    auto guard = Guard(Lock_);
    DoAdd(computationId, computationSpec, delta, now);
}

void TLineageTracker::DoAdd(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta,
    TInstant now)
{
    LastObservationTime_ = std::max(now, LastObservationTime_);

    auto updateEdge = [&] (
        const TStreamId& localOutputStreamId,
        const TStreamId& localParentStreamId,
        const TLineageDeltaValue& value) {
        const auto outputStreamId = MakeGlobalStreamId(computationId, localOutputStreamId, computationSpec);
        const auto parentStreamId = MakeGlobalStreamId(computationId, localParentStreamId, computationSpec);
        auto& counters = Counters_[outputStreamId][parentStreamId];
        counters.CountCounter.Add(value.Count, now);
        counters.ByteCounter.Add(value.ByteSize, now);
        counters.InputCountCounter.Add(value.InputCount, now);
        counters.InputByteCounter.Add(value.InputByteSize, now);
        counters.LastUpdateTime = std::max(counters.LastUpdateTime, now);
    };

    for (const auto& [localOutputStreamId, parentDeltas] : delta) {
        const auto* parentStreamIds = computationSpec->StreamsDependency.FindPtr(localOutputStreamId);
        YT_VERIFY(parentStreamIds);
        for (const auto& [localParentStreamId, _] : parentDeltas) {
            YT_VERIFY(parentStreamIds->contains(localParentStreamId));
        }
    }

    for (const auto& [localOutputStreamId, localParentStreamIds] : computationSpec->StreamsDependency) {
        for (const auto& localParentStreamId : localParentStreamIds) {
            TLineageDeltaValue value;
            if (auto outputIt = delta.find(localOutputStreamId); outputIt != delta.end()) {
                if (auto parentIt = outputIt->second.find(localParentStreamId); parentIt != outputIt->second.end()) {
                    value = parentIt->second;
                }
            }
            updateEdge(localOutputStreamId, localParentStreamId, value);
        }
    }
}

TLineageRatios TLineageTracker::GetRatios(TInstant now)
{
    auto guard = Guard(Lock_);
    return DoGetRatios(std::max(now, LastObservationTime_));
}

TLineageRatios TLineageTracker::DoGetRatios(TInstant now)
{
    TLineageRatios result;
    for (auto outputIt = Counters_.begin(); outputIt != Counters_.end();) {
        auto& parentCounters = outputIt->second;
        for (auto parentIt = parentCounters.begin(); parentIt != parentCounters.end();) {
            if (parentIt->second.LastUpdateTime + LineageRetentionTime <= now) {
                auto staleIt = parentIt++;
                parentCounters.erase(staleIt);
                continue;
            }
            auto getRatio = [now] (const TDecayedSum& output, const TDecayedSum& input) -> std::optional<TWeightedRatio> {
                if (input.GetLastValue() == 0) {
                    return std::nullopt;
                }
                TWeightedRatio result;
                result.Ratio = output.GetLastValue() / input.GetLastValue();
                result.Weight = input.GetDecayedValue(now);
                return result;
            };
            const auto& counters = parentIt->second;
            auto& ratio = result[outputIt->first][parentIt->first];
            ratio.Count = getRatio(counters.CountCounter, counters.InputCountCounter);
            ratio.ByteSize = getRatio(counters.ByteCounter, counters.InputByteCounter);
            ++parentIt;
        }
        if (parentCounters.empty()) {
            auto emptyIt = outputIt++;
            Counters_.erase(emptyIt);
        } else {
            ++outputIt;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

IJobLineageTrackerPtr CreateJobLineageTracker(
    TLineageTrackerPtr lineageTracker,
    TComputationId computationId,
    TComputationSpecPtr computationSpec)
{
    return New<TJobLineageTracker>(
        std::move(lineageTracker),
        std::move(computationId),
        std::move(computationSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
