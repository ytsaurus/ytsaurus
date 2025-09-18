#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/public.h>

#include <yt/yt/server/lib/scheduler/structs.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TScheduleAllocationsContext)

DECLARE_REFCOUNTED_STRUCT(TDynamicAttributesListSnapshot)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationSchedulingPriority,
    (High)
    (Medium)
);

////////////////////////////////////////////////////////////////////////////////

using TOperationElementsBySchedulingPriority = TEnumIndexedArray<EOperationSchedulingPriority, TNonOwningOperationElementList>;

using TOperationCountByPreemptionPriority = TEnumIndexedArray<EOperationPreemptionPriority, int>;
using TOperationPreemptionPriorityParameters = std::pair<EOperationPreemptionPriorityScope, /*ssdPriorityPreemptionEnabled*/ bool>;
using TOperationCountsByPreemptionPriorityParameters = THashMap<TOperationPreemptionPriorityParameters, TOperationCountByPreemptionPriority>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
