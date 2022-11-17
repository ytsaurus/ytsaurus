#pragma once

#include "public.h"
#include "persistent_scheduler_state.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPersistentFairShareTreeJobSchedulerState
    : public NYTree::TYsonStruct
{
public:
    TPersistentSchedulingSegmentsStatePtr SchedulingSegmentsState;

    REGISTER_YSON_STRUCT(TPersistentFairShareTreeJobSchedulerState);

    static void Register(TRegistrar registrar);
};

using TPersistentFairShareTreeJobSchedulerStatePtr = TIntrusivePtr<TPersistentFairShareTreeJobSchedulerState>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
