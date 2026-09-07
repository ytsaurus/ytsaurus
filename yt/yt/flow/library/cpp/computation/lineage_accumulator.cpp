#include "lineage_accumulator.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void AddLineageInput(
    TLineageDelta* delta,
    const TComputationSpecPtr& spec,
    const TStreamId& inputStreamId,
    i64 count,
    i64 byteSize)
{
    for (const auto& [outputStreamId, parents] : spec->StreamsDependency) {
        if (parents.contains(inputStreamId)) {
            auto& value = (*delta)[outputStreamId][inputStreamId];
            value.InputCount += count;
            value.InputByteSize += byteSize;
        }
    }
}

void AddLineageInputs(
    TLineageDelta* delta,
    const TComputationSpecPtr& spec,
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    THashMap<TStreamId, std::pair<i64, i64>> totals;
    auto add = [&] (const auto& input) {
        auto& [count, byteSize] = totals[input->StreamId];
        ++count;
        byteSize += input->ByteSize;
    };
    for (const auto& message : messages) {
        add(message);
    }
    for (const auto& timer : timers) {
        add(timer);
    }
    for (const auto& visit : visits) {
        add(visit);
    }
    for (const auto& [outputStreamId, parents] : spec->StreamsDependency) {
        for (const auto& inputStreamId : parents) {
            if (const auto* total = totals.FindPtr(inputStreamId)) {
                auto& value = (*delta)[outputStreamId][inputStreamId];
                value.InputCount += total->first;
                value.InputByteSize += total->second;
            }
        }
    }
}

void TLineageAccumulator::Add(
    const TMessage& output,
    const TMessageParentsConstPtr& parents)
{
    DoAdd(output.StreamId, GetMessageByteSize(output), parents);
}

void TLineageAccumulator::Add(
    const TTimer& output,
    const TMessageParentsConstPtr& parents)
{
    DoAdd(output.StreamId, GetTimerByteSize(output), parents);
}

void TLineageAccumulator::DoAdd(
    const TStreamId& outputStreamId,
    i64 outputByteSize,
    const TMessageParentsConstPtr& parents)
{
    YT_VERIFY(parents);
    const auto parentCount =
        parents->ParentMessages.size() +
        parents->ParentTimers.size() +
        parents->ParentVisits.size();
    YT_VERIFY(parentCount > 0);

    // DoProcessMessage() creates one parent set per input. Account the common single-parent
    // path directly, so the number of allocations is bounded by topology edges, not messages.
    if (parentCount == 1) {
        auto addParent = [&] (const auto& parent) {
            auto& delta = SingleParentDelta_[outputStreamId][parent->StreamId];
            delta.Count += 1;
            delta.ByteSize += outputByteSize;
        };
        if (!parents->ParentMessages.empty()) {
            addParent(parents->ParentMessages[0]);
        } else if (!parents->ParentTimers.empty()) {
            addParent(parents->ParentTimers[0]);
        } else {
            addParent(parents->ParentVisits[0]);
        }
        return;
    }

    // Outputs emitted from one child collector share their parent set. Group multi-parent
    // outputs by that set to avoid walking all parents for every emitted entity.
    auto [it, inserted] = MultiParentGroups_.try_emplace(parents.Get());
    if (inserted) {
        it->second.Parents = parents;
    }
    auto& outputDelta = it->second.OutputDeltas[outputStreamId];
    outputDelta.Count += 1;
    outputDelta.ByteSize += outputByteSize;
}

TLineageDelta TLineageAccumulator::Finish()
{
    auto result = std::exchange(SingleParentDelta_, {});
    for (const auto& [_, group] : MultiParentGroups_) {
        THashMap<TStreamId, i64> parentCounts;
        i64 totalParentCount = 0;
        auto addParent = [&] (const auto& parent) {
            ++parentCounts[parent->StreamId];
            ++totalParentCount;
        };
        for (const auto& parent : group.Parents->ParentMessages) {
            addParent(parent);
        }
        for (const auto& parent : group.Parents->ParentTimers) {
            addParent(parent);
        }
        for (const auto& parent : group.Parents->ParentVisits) {
            addParent(parent);
        }
        YT_VERIFY(totalParentCount > 1);

        for (const auto& [outputStreamId, outputDelta] : group.OutputDeltas) {
            auto& parentDeltas = result[outputStreamId];
            for (const auto& [parentStreamId, parentCount] : parentCounts) {
                auto& parentDelta = parentDeltas[parentStreamId];
                const double fraction = static_cast<double>(parentCount) / totalParentCount;
                parentDelta.Count += outputDelta.Count * fraction;
                parentDelta.ByteSize += outputDelta.ByteSize * fraction;
            }
        }
    }
    MultiParentGroups_.clear();
    return result;
}

} // namespace NYT::NFlow
