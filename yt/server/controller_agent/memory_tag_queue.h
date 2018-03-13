#pragma once

#include "public.h"

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagQueue
{
public:
    TMemoryTagQueue(TControllerAgentConfigPtr config);

    TMemoryTag AssignTagToOperation(const TOperationId& operationId);
    void ReclaimOperationTag(const TOperationId& operationId);

    void BuildTaggedMemoryStatistics(NYTree::TFluentList fluent);

private:
    const TControllerAgentConfigPtr Config_;

    std::queue<TMemoryTag> AvailableTags_;
    THashMap<TOperationId, TMemoryTag> OperationIdToTag_;

    NYson::TYsonString CachedTaggedMemoryStatistics_ = NYson::TYsonString("", NYson::EYsonType::ListFragment);
    TInstant CachedTaggedMemoryStatisticsLastUpdateTime_;


    std::vector<TOperationId> TagToLastOperationId_;
    const TMemoryTag MaxUsedMemoryTag_;

    void UpdateStatistics();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

