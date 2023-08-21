#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultMemoryTagCount = 4096;

//! When used tag count exceeds allocated tag count multiplied by this factor, we
//! allocate twice as many memory tags as it was previously to ensure that the
//! same tag is not re-used too often.
constexpr double MemoryTagQueueLoadFactor = 0.5;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagQueue
    : public NProfiling::ISensorProducer
{
public:
    TMemoryTagQueue(
        TControllerAgentConfigPtr config,
        IInvokerPtr invoker);

    TMemoryTag AssignTagToOperation(TOperationId operationId, i64 testingMemoryFootprint);
    void ReclaimTag(TMemoryTag tag);

    void BuildTaggedMemoryStatistics(NYTree::TFluentList fluent);

    void UpdateConfig(TControllerAgentConfigPtr config);

    i64 GetTotalUsage();

private:
    TControllerAgentConfigPtr Config_;
    const IInvokerPtr Invoker_;

    int AllocatedTagCount_ = DefaultMemoryTagCount;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    //! A queue of spare tags.
    std::queue<TMemoryTag> AvailableTags_;

    //! A hashset of used tags.
    THashSet<TMemoryTag> UsedTags_;

    struct TOperationInfo
    {
        TOperationId Id;
        i64 TestingMemoryFootprint = 0;
    };

    //! Last operation id that was assigned to each of the tags.
    std::vector<TOperationInfo> TagToLastOperationInfo_;

    //! Cached YSON representation of operations, their memory tags and memory usages.
    NYson::TYsonString CachedTaggedMemoryStatistics_ = NYson::TYsonString(TStringBuf(), NYson::EYsonType::ListFragment);
    TInstant CachedTaggedMemoryStatisticsLastUpdateTime_;

    //! Cached total memory usage.
    i64 CachedTotalUsage_;

    //! Cached per-tag memory usage.
    THashMap<TMemoryTag, i64> CachedMemoryUsage_;

    void AllocateNewTags();

    void UpdateStatistics();
    void UpdateStatisticsIfNeeded();

    void CollectSensors(NProfiling::ISensorWriter* writer) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

