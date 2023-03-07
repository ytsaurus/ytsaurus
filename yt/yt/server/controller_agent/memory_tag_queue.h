#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/ytree/fluent.h>

#include <library/cpp/ytalloc/core/concurrency/rw_spinlock.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultMemoryTagCount = 4096;

//! When used tag count exceeds allocated tag count multiplied by this factor, we
//! allocate twice as many memory tags as it was previously to ensure that the
//! same tag is not re-used too often.
constexpr double MemoryTagQueueLoadFactor = 0.5;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagQueue
{
public:
    TMemoryTagQueue(
        TControllerAgentConfigPtr config,
        IInvokerPtr invoker);

    NYTAlloc::TMemoryTag AssignTagToOperation(TOperationId operationId);
    void ReclaimTag(NYTAlloc::TMemoryTag tag);

    void BuildTaggedMemoryStatistics(NYTree::TFluentList fluent);

    void UpdateConfig(TControllerAgentConfigPtr config);

    i64 GetTotalUsage();

private:
    TControllerAgentConfigPtr Config_;
    const IInvokerPtr Invoker_;

    int AllocatedTagCount_ = DefaultMemoryTagCount;

    NConcurrency::TReaderWriterSpinLock Lock_;

    //! A queue of spare tags.
    std::queue<NYTAlloc::TMemoryTag> AvailableTags_;

    //! A hashset of used tags.
    THashSet<NYTAlloc::TMemoryTag> UsedTags_;

    //! Last operation id that was assigned to each of the tags.
    std::vector<TOperationId> TagToLastOperationId_;

    //! Cached YSON representation of operations, their memory tags and memory usages.
    NYson::TYsonString CachedTaggedMemoryStatistics_ = NYson::TYsonString("", NYson::EYsonType::ListFragment);
    TInstant CachedTaggedMemoryStatisticsLastUpdateTime_;

    //! Cached total memory usage.
    i64 CachedTotalUsage_;

    //! Cached per-tag memory usage.
    THashMap<NYTAlloc::TMemoryTag, i64> CachedMemoryUsage_;

    THashMap<NYTAlloc::TMemoryTag, NProfiling::TTagId> ProfilingTags_;
    const NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    void AllocateNewTags();

    void UpdateStatistics();
    void UpdateStatisticsIfNeeded();

    void OnProfiling();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

