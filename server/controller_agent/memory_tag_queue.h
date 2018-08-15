#pragma once

#include "public.h"

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent {

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
    TMemoryTagQueue(TControllerAgentConfigPtr config);

    TMemoryTag AssignTagToOperation(const TOperationId& operationId);
    void ReclaimTag(TMemoryTag tag);

    void BuildTaggedMemoryStatistics(NYTree::TFluentList fluent);

    void UpdateConfig(TControllerAgentConfigPtr config);

private:
    TControllerAgentConfigPtr Config_;
    int AllocatedTagCount_ = DefaultMemoryTagCount;

    TSpinLock Lock_;

    //! A queue of spare tags.
    std::queue<TMemoryTag> AvailableTags_;

    //! A hashset of used tags.
    THashSet<TMemoryTag> UsedTags_;

    //! Last operation id that was assigned to each of the tags.
    std::vector<TOperationId> TagToLastOperationId_;

    //! Cached YSON representation of operations, their memory tags and memory usages.
    NYson::TYsonString CachedTaggedMemoryStatistics_ = NYson::TYsonString("", NYson::EYsonType::ListFragment);
    TInstant CachedTaggedMemoryStatisticsLastUpdateTime_;

    void AllocateNewTags();

    void UpdateStatistics();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

