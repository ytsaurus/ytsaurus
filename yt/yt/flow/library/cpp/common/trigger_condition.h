#pragma once

#include "public.h"

#include "watermark_history.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TTimerTriggerCondition
{
    ETimeType TimeType = ETimeType::EventTime;
    THashMap<TStreamId, TDuration> StreamsWithDelays;
};

using TTimerTriggerConditions = THashMap<TStreamId, TTimerTriggerCondition>;

TTimerTriggerConditions BuildTimerTriggerConditions(
    const THashMap<TStreamId, TTimerSpecPtr>& timerSpecs,
    const THashMap<TStreamId, THashSet<TStreamId>>& streamsDependency);

//! Returns a conservative estimate of the first instant at which the trigger watermark of |condition| reached
//! |triggerTimestamp|, as published up to |now|: never later than that instant, and for event and system time
//! earlier by at most 1/K of its age, K being the history thinning factor. For current time it is the trigger
//! timestamp itself.
//! For event and system time, an empty condition and a zero trigger timestamp are reached at the zero instant.
//! #InfinitySystemTimestamp means not reached yet; null means the instant precedes the history.
std::optional<TSystemTimestamp> ResolveTriggerReach(
    const TTimerTriggerCondition& condition,
    TSystemTimestamp triggerTimestamp,
    const TPipelineWatermarkHistory& history,
    TSystemTimestamp now);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
