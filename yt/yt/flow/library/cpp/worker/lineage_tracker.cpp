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
        if (!PendingDelta_) {
            PendingDelta_ = std::move(delta);
            return;
        }

        for (const auto& [outputStreamId, parentDeltas] : delta) {
            auto& pendingParentDeltas = (*PendingDelta_)[outputStreamId];
            for (const auto& [parentStreamId, value] : parentDeltas) {
                auto& pendingValue = pendingParentDeltas[parentStreamId];
                pendingValue.Count += value.Count;
                pendingValue.ByteSize += value.ByteSize;
                pendingValue.InputCount += value.InputCount;
                pendingValue.InputByteSize += value.InputByteSize;
            }
        }
    }

    void Commit() override
    {
        if (!PendingDelta_) {
            return;
        }
        auto delta = std::exchange(PendingDelta_, std::nullopt);
        LineageTracker_->Commit(ComputationId_, ComputationSpec_, *delta);
    }

private:
    const TLineageTrackerPtr LineageTracker_;
    const TComputationId ComputationId_;
    const TComputationSpecPtr ComputationSpec_;

    std::optional<TLineageDelta> PendingDelta_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TLineageTracker::Commit(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta)
{
    auto guard = Guard(Lock_);
    DoCommit(computationId, computationSpec, delta, TInstant::Now());
}

void TLineageTracker::Commit(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta,
    TInstant now)
{
    auto guard = Guard(Lock_);
    DoCommit(computationId, computationSpec, delta, now);
}

void TLineageTracker::DoCommit(
    const TComputationId& computationId,
    const TComputationSpecPtr& computationSpec,
    const TLineageDelta& delta,
    TInstant now)
{
    if (now <= LastCommitTime_) {
        now = LastCommitTime_ + TDuration::MicroSeconds(1);
    }
    LastCommitTime_ = now;

    auto updateEdge = [&] (
        const TStreamId& localOutputStreamId,
        const TStreamId& localParentStreamId,
        const TLineageDeltaValue& value) {
        const auto outputStreamId = MakeGlobalStreamId(computationId, localOutputStreamId, computationSpec);
        const auto parentStreamId = MakeGlobalStreamId(computationId, localParentStreamId, computationSpec);
        auto [it, inserted] = Counters_[outputStreamId].try_emplace(parentStreamId);
        if (inserted) {
            it->second.CountCounter.Update(value.Count, now);
            it->second.ByteCounter.Update(value.ByteSize, now);
            it->second.InputCountCounter.Update(value.InputCount, now);
            it->second.InputByteCounter.Update(value.InputByteSize, now);
        } else {
            it->second.CountCounter.Inc(value.Count, now);
            it->second.ByteCounter.Inc(value.ByteSize, now);
            it->second.InputCountCounter.Inc(value.InputCount, now);
            it->second.InputByteCounter.Inc(value.InputByteSize, now);
        }
        it->second.LastUpdateTime = now;
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

TLineageRates TLineageTracker::GetRates(TInstant now)
{
    auto guard = Guard(Lock_);
    return DoGetRates(std::max(now, LastCommitTime_));
}

TLineageRates TLineageTracker::DoGetRates(TInstant now)
{
    TLineageRates result;
    for (auto outputIt = Counters_.begin(); outputIt != Counters_.end();) {
        auto& parentCounters = outputIt->second;
        for (auto parentIt = parentCounters.begin(); parentIt != parentCounters.end();) {
            if (parentIt->second.LastUpdateTime + LineageRateRetentionTime <= now) {
                auto staleIt = parentIt++;
                parentCounters.erase(staleIt);
                continue;
            }
            auto& rate = result[outputIt->first][parentIt->first];
            rate.CountPerSecond = parentIt->second.CountCounter.GetDecayedRate(now);
            rate.BytesPerSecond = parentIt->second.ByteCounter.GetDecayedRate(now);
            rate.InputCountPerSecond = parentIt->second.InputCountCounter.GetDecayedRate(now);
            rate.InputBytesPerSecond = parentIt->second.InputByteCounter.GetDecayedRate(now);
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
