#include "memory_tag_queue.h"
#include "private.h"
#include "config.h"

#include <yt/server/controller_agent/controller_agent.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NYTAlloc;

static const auto& Logger = ControllerLogger;
static const TProfiler MemoryTagQueueProfiler("/memory_tag_queue");

////////////////////////////////////////////////////////////////////////////////

TMemoryTagQueue::TMemoryTagQueue(
    TControllerAgentConfigPtr config,
    IInvokerPtr invoker)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , TagToLastOperationId_(AllocatedTagCount_)
    , ProfilingExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TMemoryTagQueue::OnProfiling, this),
        Config_->MemoryUsageProfilingPeriod))
{
    for (TMemoryTag tag = 1; tag < AllocatedTagCount_; ++tag) {
        AvailableTags_.push(tag);
        YT_VERIFY(ProfilingTags_.emplace(tag, TProfileManager::Get()->RegisterTag("tag", tag)).second);
    }
    ProfilingExecutor_->Start();
}

TMemoryTag TMemoryTagQueue::AssignTagToOperation(TOperationId operationId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(Lock_);

    if (UsedTags_.size() > MemoryTagQueueLoadFactor * AllocatedTagCount_) {
        AllocateNewTags();
    }

    YT_VERIFY(!AvailableTags_.empty());
    auto tag = AvailableTags_.front();
    AvailableTags_.pop();
    UsedTags_.insert(tag);
    TagToLastOperationId_[tag] = operationId;
    YT_LOG_INFO("Assigning memory tag to operation (OperationId: %v, MemoryTag: %v, UsedMemoryTagCount: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        UsedTags_.size(),
        AvailableTags_.size());

    return tag;
}

void TMemoryTagQueue::ReclaimTag(TMemoryTag tag)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(Lock_);

    YT_VERIFY(UsedTags_.erase(tag));

    auto operationId = TagToLastOperationId_[tag];

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
    TagToLastOperationId_.resize(2 * AllocatedTagCount_);
    for (TMemoryTag tag = AllocatedTagCount_; tag < 2 * AllocatedTagCount_; ++tag) {
        AvailableTags_.push(tag);
        YT_VERIFY(ProfilingTags_.emplace(tag, TProfileManager::Get()->RegisterTag("tag", tag)).second);
    }

    AllocatedTagCount_ *= 2;
}

void TMemoryTagQueue::UpdateStatisticsIfNeeded()
{
    TReaderGuard guard(Lock_);

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
        TReaderGuard guard(Lock_);
        tags.resize(AllocatedTagCount_ - 1);
        std::iota(tags.begin(), tags.end(), 1);
        usages.resize(AllocatedTagCount_ - 1);
    }

    YT_LOG_INFO("Started building tagged memory statistics (EntryCount: %v)", tags.size());
    GetMemoryUsageForTags(tags.data(), tags.size(), usages.data());
    YT_LOG_INFO("Finished building tagged memory statistics (EntryCount: %v)", tags.size());

    {
        TWriterGuard guard(Lock_);
        CachedTotalUsage_ = 0;
        for (int index = 0; index < tags.size(); ++index) {
            auto tag = tags[index];
            auto usage = usages[index];
            auto operationId = TagToLastOperationId_[tag] ? std::make_optional(TagToLastOperationId_[tag]) : std::nullopt;
            auto alive = operationId && UsedTags_.contains(tag);
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

    TReaderGuard guard(Lock_);

    return CachedTotalUsage_;
}

void TMemoryTagQueue::OnProfiling()
{
    UpdateStatisticsIfNeeded();

    THashMap<TMemoryTag, i64> cachedMemoryUsage;
    {
        TReaderGuard guard(Lock_);

        cachedMemoryUsage = CachedMemoryUsage_;
    }

    for (int tag = 1; tag < AllocatedTagCount_; ++tag) {
        MemoryTagQueueProfiler.Enqueue(
            "/memory_usage",
            cachedMemoryUsage[tag],
            EMetricType::Gauge,
            {GetOrCrash(ProfilingTags_, tag)});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
