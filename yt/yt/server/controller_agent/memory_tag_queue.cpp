#include "memory_tag_queue.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/controller_agent/controller_agent.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NYTAlloc;

static const auto& Logger = ControllerLogger;
static const TProfiler MemoryTagQueueProfiler("/memory_tag_queue");

DEFINE_REFCOUNTED_TYPE(TMemoryTagQueue)

////////////////////////////////////////////////////////////////////////////////

TMemoryTagQueue::TMemoryTagQueue(
    TControllerAgentConfigPtr config,
    IInvokerPtr invoker)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , TagToLastOperationInfo_(AllocatedTagCount_)
{
    MemoryTagQueueProfiler.WithSparse().AddProducer("", MakeStrong(this));

    for (int tag = 1; tag < AllocatedTagCount_; ++tag) {
        AvailableTags_.push(static_cast<TMemoryTag>(tag));
    }
}

TMemoryTag TMemoryTagQueue::AssignTagToOperation(TOperationId operationId, i64 testingMemoryFootprint)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    if (UsedTags_.size() > MemoryTagQueueLoadFactor * AllocatedTagCount_) {
        AllocateNewTags();
    }

    YT_VERIFY(!AvailableTags_.empty());
    auto tag = AvailableTags_.front();
    AvailableTags_.pop();
    UsedTags_.insert(tag);
    TagToLastOperationInfo_[tag] = TOperationInfo{
        .Id = operationId,
        .TestingMemoryFootprint = testingMemoryFootprint,
    };
    YT_LOG_INFO("Assigning memory tag to operation (OperationId: %v, MemoryTag: %v, TestingMemoryFootprint: %v, UsedMemoryTagCount: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        testingMemoryFootprint,
        UsedTags_.size(),
        AvailableTags_.size());

    return tag;
}

void TMemoryTagQueue::ReclaimTag(TMemoryTag tag)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    YT_VERIFY(UsedTags_.erase(tag));

    auto operationId = TagToLastOperationInfo_[tag].Id;

    AvailableTags_.push(tag);
    YT_LOG_INFO("Reclaiming memory tag of operation (OperationId: %v, MemoryTag: %v, UsedMemoryTagCount: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        UsedTags_.size(),
        AvailableTags_.size());
}

void TMemoryTagQueue::BuildTaggedMemoryStatistics(TFluentList fluent)
{
    UpdateStatisticsIfNeeded();

    fluent.GetConsumer()->OnRaw(CachedTaggedMemoryStatistics_);
}

void TMemoryTagQueue::UpdateConfig(TControllerAgentConfigPtr config)
{
    Config_ = std::move(config);
}

void TMemoryTagQueue::AllocateNewTags()
{
    YT_LOG_INFO("Allocating new memory tags (AllocatedTagCount: %v, NewAllocatedTagCount: %v)", AllocatedTagCount_, 2 * AllocatedTagCount_);
    TagToLastOperationInfo_.resize(2 * AllocatedTagCount_);
    for (int tag = AllocatedTagCount_; tag < 2 * AllocatedTagCount_; ++tag) {
        AvailableTags_.push(static_cast<TMemoryTag>(tag));
    }
    AllocatedTagCount_ *= 2;
}

void TMemoryTagQueue::UpdateStatisticsIfNeeded()
{
    auto guard = ReaderGuard(Lock_);

    auto now = NProfiling::GetInstant();
    if (CachedTaggedMemoryStatisticsLastUpdateTime_ + Config_->TaggedMemoryStatisticsUpdatePeriod < now) {
        guard.Release();
        UpdateStatistics();
    }
}

void TMemoryTagQueue::UpdateStatistics()
{
    auto fluent = BuildYsonStringFluently<NYson::EYsonType::ListFragment>();

    std::vector<TMemoryTag> tags;
    std::vector<size_t> usages;
    {
        auto guard = ReaderGuard(Lock_);
        tags.resize(AllocatedTagCount_ - 1);
        std::iota(tags.begin(), tags.end(), 1);
        usages.resize(AllocatedTagCount_ - 1);
    }

    YT_LOG_INFO("Started building tagged memory statistics (EntryCount: %v)", tags.size());
    auto heapUsage = NYTProf::GetEstimatedMemoryUsage();
    NYTProf::UpdateMemoryUsageSnapshot(heapUsage);
    GetMemoryUsageForTags(tags.data(), tags.size(), usages.data());
    YT_LOG_INFO("Finished building tagged memory statistics (EntryCount: %v)", tags.size());

    {
        auto guard = WriterGuard(Lock_);
        CachedTotalUsage_ = 0;
        for (int index = 0; index < std::ssize(tags); ++index) {
            auto tag = tags[index];
            auto operationInfo = TagToLastOperationInfo_[tag];

            auto operationId = operationInfo.Id ? std::make_optional(operationInfo.Id) : std::nullopt;
            auto alive = operationId && UsedTags_.contains(tag);

            auto usage = usages[index] + heapUsage[tag];
            if (alive) {
                usage += operationInfo.TestingMemoryFootprint;
            }

            YT_LOG_INFO("Memory usage (Tag: %v, OperationId: %v, Usage: %v, Alive: %v)", tag, operationId, usage, alive);

            fluent
                .Item().BeginMap()
                    .Item("usage").Value(usage)
                    .Item("operation_id").Value(operationId)
                    .Item("alive").Value(alive)
                .EndMap();

            CachedTotalUsage_ += std::max<i64>(0, usage);
            CachedMemoryUsage_[tag] = std::max<i64>(0, usage);
        }

        CachedTaggedMemoryStatistics_ = fluent.Finish();
        CachedTaggedMemoryStatisticsLastUpdateTime_ = NProfiling::GetInstant();
    }
}

i64 TMemoryTagQueue::GetTotalUsage()
{
    UpdateStatisticsIfNeeded();

    auto guard = ReaderGuard(Lock_);

    return CachedTotalUsage_;
}

void TMemoryTagQueue::CollectSensors(ISensorWriter* writer)
{
    UpdateStatisticsIfNeeded();

    THashMap<TMemoryTag, i64> cachedMemoryUsage;
    int cachedTagCount;
    {
        auto guard = ReaderGuard(Lock_);
        cachedMemoryUsage = CachedMemoryUsage_;
        cachedTagCount = AllocatedTagCount_;
    }

    for (int tag = 1; tag < cachedTagCount; ++tag) {
        TWithTagGuard tagGuard(writer, "tag", ToString(tag));
        writer->AddGauge("/memory_usage", cachedMemoryUsage[tag]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
