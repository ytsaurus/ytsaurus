#include "key_count_partitioning.h"

#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>
#include <limits>

namespace NYT::NFlow::NPartitioning {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::span<const TKey> GetKeysInRange(std::span<const TKey> sortedKeys, const TKeyRange& range)
{
    const auto lowerIt = std::lower_bound(sortedKeys.begin(), sortedKeys.end(), range.Lower);
    const auto upperIt = std::lower_bound(lowerIt, sortedKeys.end(), range.Upper);
    return {lowerIt, upperIt};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TKeyCountPartitioningState::Register(TRegistrar registrar)
{
    registrar.Parameter("observed_keys", &TThis::ObservedKeys).Default();
    registrar.Parameter("expired_before", &TThis::ExpiredBefore).Default();
}

i64 CountExecutableKeyCountOwners(const TFlowViewPtr& flowView, const TComputationId& computationId)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    i64 count = 0;
    for (const auto& [jobId, job] : layout->Jobs) {
        const auto& partition = GetOrCrash(layout->Partitions, job->PartitionId);
        if (partition->ComputationId != computationId) {
            continue;
        }
        const auto* feedback = flowView->Feedback->PartitionJobStatuses.FindPtr(job->PartitionId);
        const auto status = feedback ? (*feedback)->CurrentJobStatus : nullptr;
        if (!status || status->JobId != jobId || !status->IsFinished) {
            ++count;
        }
    }
    return count;
}

void IngestObservedKeys(
    const TKeyCountPartitioningStatePtr& state,
    const THashMap<TKey, TInstant>& keys)
{
    for (const auto& [key, at] : keys) {
        // Lifetime reports may repeat observations expired before a pause or leader change.
        if (at <= state->ExpiredBefore) {
            continue;
        }
        auto [it, inserted] = state->ObservedKeys.try_emplace(key, at);
        it->second = std::max(it->second, at);
    }
}

void ExpireKeyCatalogue(const TKeyCountPartitioningStatePtr& state, TInstant cutoff)
{
    state->ExpiredBefore = std::max(state->ExpiredBefore, cutoff);
    EraseNodesIf(state->ObservedKeys, [&] (const auto& item) {
        return item.second <= state->ExpiredBefore;
    });
}

std::vector<TKeyRange> BuildKeyCountRanges(
    std::span<const TKey> sortedKeys,
    const TKeyRange& range,
    i64 maxKeys)
{
    YT_VERIFY(maxKeys > 0);
    const auto keys = GetKeysInRange(sortedKeys, range);
    const i64 count = keys.empty() ? 1 : 1 + (std::ssize(keys) - 1) / maxKeys;
    const i64 quotient = std::ssize(keys) / count;
    const i64 remainder = std::ssize(keys) % count;
    std::vector<TKeyRange> ranges;
    auto lower = range.Lower;
    i64 offset = 0;
    for (i64 index = 0; index < count; ++index) {
        offset += quotient + (index < remainder ? 1 : 0);
        auto upper = index + 1 == count ? range.Upper : keys[offset];
        ranges.push_back({lower, upper});
        lower = upper;
    }
    return ranges;
}

TKeyCountPartitioning::TKeyCountPartitioning(TKeyCountPartitioningStatePtr state, IStatusErrorStatePtr errorState)
    : State_(std::move(state))
    , ErrorState_(std::move(errorState))
{ }

void TKeyCountPartitioning::BeginIteration(const TFlowViewPtr& committedFlowView)
{
    PendingRebalanceAt_ = LastRebalanceAt_;
    if (committedFlowView->State->ExecutionSpec->PipelineState->GetValue() != EPipelineState::Working) {
        ActiveSince_.reset();
    }
}

void TKeyCountPartitioning::Commit()
{
    LastRebalanceAt_ = PendingRebalanceAt_;
}

TErrorOr<TKeyCountPartitioning::TResult> TKeyCountPartitioning::Partition(
    const TComputationId& computationId,
    const TPartitioningSpecPtr& spec,
    const TDynamicPartitioningSpecPtr& dynamicSpec,
    const THashMap<TPartitionId, TKeyRange>& ranges,
    const TFlowViewPtr& flowView,
    bool firstKeyIsUint)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto now = flowView->Feedback->UpdateTime;
    const auto& config = dynamicSpec->KeyCountPartitioning;
    const i64 maxKeys = spec->KeyCountPartitioning->MaxKeysPerPartition;
    const auto [minimum, maximum] = dynamicSpec->GetPartitionCountBounds();
    THROW_ERROR_EXCEPTION_IF(minimum <= 0 || maximum <= 0,
        "Key-count partitioning requires positive partition count bounds");
    if (flowView->State->ExecutionSpec->PipelineState->GetValue() == EPipelineState::Working && !ActiveSince_) {
        ActiveSince_ = now;
    }
    // A fresh leader or a resumed pipeline must observe a full retention window before expiry.
    if (ActiveSince_ && now >= *ActiveSince_ + config->KeyRetentionPeriod) {
        ExpireKeyCatalogue(State_, now - config->KeyRetentionPeriod);
    }
    auto publish = [&] (const TError& error) {
        if (ErrorState_) {
            ErrorState_->SetError(error);
        }
        auto diagnostic = NYTree::ConvertToNode(State_)->AsMap();
        diagnostic->AddChild("error", NYTree::ConvertToNode(error));
        diagnostic->AddChild("capacity_blocked", NYTree::ConvertToNode(!error.IsOK()));
        flowView->EphemeralState->KeyCountPartitioning[computationId] = std::move(diagnostic);
    };
    auto block = [&] {
        auto error = TError("Key-count partitioning cannot fit within max_partition_count")
            .With("max_keys_per_partition", maxKeys)
            .With("max_partition_count", maximum);
        std::vector<TJobId> jobs;
        for (const auto& [id, job] : layout->Jobs) {
            if (GetOrCrash(layout->Partitions, job->PartitionId)->ComputationId != computationId) {
                continue;
            }
            const auto& status = flowView->Feedback->GetFreshCurrentJobStatus(job->PartitionId, TInstant::Zero());
            // Keep terminal overflow feedback until its partition can be split.
            if (status && status->JobId == id && status->IsFinished &&
                status->Error.FindMatching(EErrorCode::PartitionKeyLimitExceeded))
            {
                continue;
            }
            jobs.push_back(id);
        }
        for (const auto& id : jobs) {
            layout->RemoveJob(id, EJobFinishReason::Stopped);
        }
        flowView->EphemeralState->KeyCountPartitioningBlockedComputations.insert(computationId);
        publish(error);
        return error;
    };
    const i64 neededPartitions = State_->ObservedKeys.empty()
        ? 1
        : 1 + (std::ssize(State_->ObservedKeys) - 1) / maxKeys;
    if (neededPartitions > maximum ||
        CountExecutableKeyCountOwners(flowView, computationId) > maximum)
    {
        return block();
    }

    std::optional<std::vector<TKey>> sortedKeys;
    auto getSortedKeys = [&] () -> const std::vector<TKey>& {
        if (!sortedKeys) {
            sortedKeys = GetKeys(State_->ObservedKeys);
            std::sort(sortedKeys->begin(), sortedKeys->end());
        }
        return *sortedKeys;
    };

    TResult result;
    if (ranges.empty()) {
        result.Ranges = State_->ObservedKeys.empty() && firstKeyIsUint
            ? SplitPartitionRangeIntoBuckets(UniversalKeyRange(), minimum)
            : BuildKeyCountRanges(getSortedKeys(), UniversalKeyRange(), maxKeys);
        result.Reason = "initial_layout";
    } else {
        for (const auto& [id, range] : ranges) {
            const auto& partition = GetOrCrash(layout->Partitions, id);
            const auto& status = flowView->Feedback->GetFreshCurrentJobStatus(id, TInstant::Zero());
            if (partition->CurrentJobId && status && status->JobId == *partition->CurrentJobId &&
                status->IsFinished && status->Error.FindMatching(EErrorCode::PartitionKeyLimitExceeded))
            {
                auto replacement = BuildKeyCountRanges(getSortedKeys(), range, maxKeys);
                result.Ranges.insert(result.Ranges.end(), replacement.begin(), replacement.end());
                result.Reason = "partition_key_limit";
            } else {
                result.Ranges.push_back(range);
                result.RetainedPartitions.insert(id);
            }
        }
    }

    if (!PendingRebalanceAt_) {
        PendingRebalanceAt_ = now;
    }
    if (now >= PendingRebalanceAt_ + config->RebalancePeriod) {
        PendingRebalanceAt_ = now;
        i64 smallest = std::numeric_limits<i64>::max();
        i64 largest = 0;
        for (const auto& [_, range] : ranges) {
            const i64 count = std::ssize(GetKeysInRange(getSortedKeys(), range));
            smallest = std::min(smallest, count);
            largest = std::max(largest, count);
        }
        if (largest - smallest > 1 || std::ssize(ranges) > maximum) {
            result.Ranges = BuildKeyCountRanges(getSortedKeys(), UniversalKeyRange(), maxKeys);
            result.RetainedPartitions.clear();
            result.Reason = "periodic_rebalance";
        }
    }
    if (std::ssize(result.Ranges) > maximum) {
        return block();
    }
    if (!result.Reason.empty()) {
        for (const auto& [id, _] : ranges) {
            if (result.RetainedPartitions.contains(id)) {
                continue;
            }
            const auto& partition = GetOrCrash(layout->Partitions, id);
            if (partition->CurrentJobId) {
                layout->RemoveJob(*partition->CurrentJobId, EJobFinishReason::Rebalanced);
            }
        }
    }
    publish(TError());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
