#include "memory_tag_queue.h"
#include "private.h"
#include "config.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

static const auto& Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagQueue::TMemoryTagQueue(TControllerAgentConfigPtr config)
    : Config_(config)
    , MaxUsedMemoryTag_(MaxMemoryTag)
{
    TagToLastOperationId_.resize(MaxUsedMemoryTag_);
    for (TMemoryTag tag = 1; tag < MaxUsedMemoryTag_; ++tag) {
        AvailableTags_.push(tag);
    }
}

TMemoryTag TMemoryTagQueue::AssignTagToOperation(const TOperationId& operationId)
{
    YCHECK(!AvailableTags_.empty());
    auto tag = AvailableTags_.front();
    AvailableTags_.pop();
    OperationIdToTag_[operationId] = tag;
    TagToLastOperationId_[tag] = operationId;
    LOG_DEBUG("Assigning memory tag to operation (OperationId: %v, MemoryTag: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        AvailableTags_.size());
    return tag;
}

void TMemoryTagQueue::ReclaimOperationTag(const TOperationId& operationId)
{
    auto it = OperationIdToTag_.find(operationId);
    YCHECK(it != OperationIdToTag_.end());
    auto tag = it->second;
    OperationIdToTag_.erase(it);
    AvailableTags_.push(tag);
    LOG_DEBUG("Reclaiming memory tag of operation (OperationId: %v, MemoryTag: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        AvailableTags_.size());
}

void TMemoryTagQueue::BuildTaggedMemoryStatistics(TFluentList fluent)
{
    auto now = NProfiling::GetInstant();

    if (CachedTaggedMemoryStatisticsLastUpdateTime_ + Config_->TaggedMemoryStatisticsUpdatePeriod < now) {
        UpdateStatistics();
    }

    fluent.GetConsumer()->OnRaw(CachedTaggedMemoryStatistics_);
}

void TMemoryTagQueue::UpdateStatistics()
{
    auto fluent = BuildYsonStringFluently<NYson::EYsonType::ListFragment>();
    std::vector<TMemoryTag> tags(MaxUsedMemoryTag_ - 1);
    std::iota(tags.begin(), tags.end(), 1);
    std::vector<ssize_t> usages(MaxUsedMemoryTag_ - 1);

    LOG_INFO("Started building tagged memory statistics (EntryCount: %v)", tags.size());
    GetMemoryUsageForTagList(tags.data(), tags.size(), usages.data());
    LOG_INFO("Finished building tagged memory statistics (EntryCount: %v)", tags.size());

    for (int index = 0; index < tags.size(); ++index) {
        auto tag = tags[index];
        auto usage = usages[index];
        auto operationId = TagToLastOperationId_[tag] ? MakeNullable(TagToLastOperationId_[tag]) : Null;
        auto alive = operationId && OperationIdToTag_.has(*operationId);
        fluent
            .Item().BeginMap()
                .Item("usage").Value(usage)
                .Item("operation_id").Value(operationId)
                .Item("alive").Value(alive)
            .EndMap();
    }
    CachedTaggedMemoryStatistics_ = fluent.Finish();
    CachedTaggedMemoryStatisticsLastUpdateTime_ = NProfiling::GetInstant();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
