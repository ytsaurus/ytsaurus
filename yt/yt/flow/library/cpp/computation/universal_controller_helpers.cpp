#include "universal_controller_helpers.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow {

using namespace NYT::NLogging;

////////////////////////////////////////////////////////////////////////////////

TBlockedStreamComputer::TBlockedStreamComputer(
    const TFlowViewPtr& flowView,
    const THashSet<TPartitionId>& interruptingPartitions,
    const TLogger& logger)
    : Logger(logger.WithMinLevel(ELogLevel::Trace))
    , FlowView_(flowView)
{
    BlockingRanges_[MinKey()] = {MaxKey(), {}};
    for (const auto& partitionId : interruptingPartitions) {
        AddInterruptingPartition(partitionId);
    }
}

void TBlockedStreamComputer::AddInterruptingPartition(const TPartitionId& partitionId)
{
    const auto& executionSpec = FlowView_->State->ExecutionSpec;
    auto partition = GetOrCrash(executionSpec->Layout->Partitions, partitionId);
    auto computationSpec = GetOrCrash(executionSpec->PipelineSpec->GetValue()->Computations, partition->ComputationId);
    if (computationSpec->DistributionOrdering == EDistributionOrdering::Relaxed || computationSpec->OutputStreamIds.empty()) {
        return;
    }

    THashSet<TStreamId> blockingStreams;
    const auto& currentPartitionJobStatuses = FlowView_->Feedback->PartitionJobStatuses;
    if (auto it = currentPartitionJobStatuses.find(partitionId); it != currentPartitionJobStatuses.end() && it->second->LastTraverseData) {
        const auto& traverseStreams = it->second->LastTraverseData->Node->Streams;
        for (const auto& streamId : computationSpec->OutputStreamIds) {
            if (auto streamIt = traverseStreams.find(streamId); streamIt == traverseStreams.end() || streamIt->second->State != EStreamState::Completed) {
                blockingStreams.insert(streamId);
            }
        }
    } else {
        blockingStreams = computationSpec->OutputStreamIds;
    }
    if (blockingStreams.empty()) {
        return;
    }

    YT_LOG_TRACE("TBlockedStreamComputer: adding not trivial interrupting partition "
        "(ComputationId: %v, PartitionId: %v, BlockingStreams: %v)",
        partition->ComputationId,
        partitionId,
        ConvertToYsonString(blockingStreams, NYson::EYsonFormat::Text));

    if (partition->SourceKey.has_value()) {
        BlockingKeys_[*partition->SourceKey].insert(blockingStreams.begin(), blockingStreams.end());
        return;
    }
    const auto& lower = partition->LowerKey.value();
    const auto& upper = partition->UpperKey.value();

    auto it = BlockingRanges_.lower_bound(lower);
    if (it == BlockingRanges_.end() || it->first > lower) {
        --it;
    }

    // Now it - first range overlapping with new partition.

    if (it->first < lower) {
        // Split it into two ranges by partition->LowerKey. And skip not overlapping range.
        BlockingRanges_[lower] = it->second;
        it->second.Upper = lower;
        ++it;
    }

    YT_VERIFY(it->first == lower);

    // Process "internal" ranges.
    while (it != BlockingRanges_.end() && it->second.Upper <= upper) {
        it->second.BlockingStreams.insert(blockingStreams.begin(), blockingStreams.end());
        ++it;
    }

    if (it != BlockingRanges_.end() && it->first < upper) {
        // Split and process last part of range.
        BlockingRanges_[upper] = it->second;
        it->second.Upper = upper;
        it->second.BlockingStreams.insert(blockingStreams.begin(), blockingStreams.end());
    }
}

THashSet<TStreamId> TBlockedStreamComputer::GetBlockedStreams(const TKey& lower, const TKey& upper) const
{
    THashSet<TStreamId> blockedStreams;

    auto it = BlockingRanges_.lower_bound(lower);
    if (it == BlockingRanges_.end() || it->first > lower) {
        --it;
    }

    while (it != BlockingRanges_.end() && it->first < upper) {
        blockedStreams.insert(it->second.BlockingStreams.begin(), it->second.BlockingStreams.end());
        ++it;
    }

    YT_LOG_TRACE("TBlockedStreamComputer: blocked streams computed (BlockedStreams: %v)", ConvertToYsonString(blockedStreams, NYson::EYsonFormat::Text));

    return blockedStreams;
}

THashSet<TStreamId> TBlockedStreamComputer::GetBlockedStreams(const TKey& sourceKey) const
{
    return GetOrDefault(BlockingKeys_, sourceKey);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
