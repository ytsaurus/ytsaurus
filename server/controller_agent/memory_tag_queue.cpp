#include "memory_tag_queue.h"
#include "private.h"
#include "config.h"

#include <yt/server/controller_agent/controller_agent.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

static const auto& Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagQueue::TMemoryTagQueue(TControllerAgentConfigPtr config)
    : Config_(std::move(config))
    , TagToLastOperationId_(AllocatedTagCount_)
{
    for (TMemoryTag tag = 1; tag < AllocatedTagCount_; ++tag) {
        AvailableTags_.push(tag);
    }
}

TMemoryTag TMemoryTagQueue::AssignTagToOperation(const TOperationId& operationId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (UsedTags_.size() > MemoryTagQueueLoadFactor * AllocatedTagCount_) {
        AllocateNewTags();
    }

    YCHECK(!AvailableTags_.empty());
    auto tag = AvailableTags_.front();
    AvailableTags_.pop();
    UsedTags_.insert(tag);
    TagToLastOperationId_[tag] = operationId;
    LOG_INFO("Assigning memory tag to operation (OperationId: %v, MemoryTag: %v, UsedMemoryTagCount: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        UsedTags_.size(),
        AvailableTags_.size());

    return tag;
}

void TMemoryTagQueue::ReclaimTag(TMemoryTag tag)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(UsedTags_.erase(tag));

    auto operationId = TagToLastOperationId_[tag];

    AvailableTags_.push(tag);
    LOG_INFO("Reclaiming memory tag of operation (OperationId: %v, MemoryTag: %v, UsedMemoryTagCount: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        UsedTags_.size(),
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

void TMemoryTagQueue::UpdateConfig(TControllerAgentConfigPtr config)
{
    Config_ = std::move(config);
}

void TMemoryTagQueue::AllocateNewTags()
{
    LOG_INFO("Allocating new memory tags (AllocatedTagCount: %v, NewAllocatedTagCount: %v)", AllocatedTagCount_, 2 * AllocatedTagCount_);
    TagToLastOperationId_.resize(2 * AllocatedTagCount_);
    for (TMemoryTag tag = AllocatedTagCount_; tag < 2 * AllocatedTagCount_; ++tag) {
        AvailableTags_.push(tag);
    }

    AllocatedTagCount_ *= 2;
}

void TMemoryTagQueue::UpdateStatistics()
{
    auto fluent = BuildYsonStringFluently<NYson::EYsonType::ListFragment>();

    std::vector<TMemoryTag> tags;
    std::vector<ssize_t> usages;
    {
        TGuard<TSpinLock> guard(Lock_);
        tags.resize(AllocatedTagCount_ - 1);
        std::iota(tags.begin(), tags.end(), 1);
        usages.resize(AllocatedTagCount_ - 1);
    }

    LOG_INFO("Started building tagged memory statistics (EntryCount: %v)", tags.size());
    GetMemoryUsageForTagList(tags.data(), tags.size(), usages.data());
    LOG_INFO("Finished building tagged memory statistics (EntryCount: %v)", tags.size());

    {
        TGuard<TSpinLock> guard(Lock_);
        for (int index = 0; index < tags.size(); ++index) {
            auto tag = tags[index];
            auto usage = usages[index];
            auto operationId = TagToLastOperationId_[tag] ? MakeNullable(TagToLastOperationId_[tag]) : Null;
            auto alive = operationId && UsedTags_.has(tag);
            fluent
                .Item().BeginMap()
                    .Item("usage").Value(usage)
                    .Item("operation_id").Value(operationId)
                    .Item("alive").Value(alive)
                .EndMap();
        }
    }
    CachedTaggedMemoryStatistics_ = fluent.Finish();
    CachedTaggedMemoryStatisticsLastUpdateTime_ = NProfiling::GetInstant();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
