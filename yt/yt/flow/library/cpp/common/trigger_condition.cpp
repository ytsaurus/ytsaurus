#include "trigger_condition.h"

#include "spec.h"

#include <limits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TTimerTriggerConditions BuildTimerTriggerConditions(
    const THashMap<TStreamId, TTimerSpecPtr>& timerSpecs,
    const THashMap<TStreamId, THashSet<TStreamId>>& streamsDependency)
{
    TTimerTriggerConditions conditions;
    for (const auto& [timerId, spec] : timerSpecs) {
        auto& condition = conditions[timerId];
        condition.TimeType = spec->TimeType;
        if (spec->StreamsWithDelays) {
            condition.StreamsWithDelays = *spec->StreamsWithDelays;
        } else {
            const auto& streams = spec->Streams
                ? *spec->Streams
                : GetOrCrash(streamsDependency, timerId);
            for (const auto& streamId : streams) {
                condition.StreamsWithDelays.emplace(streamId, TDuration::Zero());
            }
        }
    }

    return conditions;
}

std::optional<TSystemTimestamp> ResolveTriggerReach(
    const TTimerTriggerCondition& condition,
    TSystemTimestamp triggerTimestamp,
    const TPipelineWatermarkHistory& history,
    TSystemTimestamp now)
{
    // Current-time timers trigger at their trigger timestamp; watched streams and delays do not apply.
    if (condition.TimeType == ETimeType::CurrentTime) {
        return triggerTimestamp <= now ? triggerTimestamp : InfinitySystemTimestamp;
    }

    // The trigger watermark of an empty condition is infinite, and the delayed watermark saturates at zero.
    if (condition.StreamsWithDelays.empty() || triggerTimestamp == ZeroSystemTimestamp) {
        return ZeroSystemTimestamp;
    }

    // The trigger watermark reaches a positive key once every stream watermark reaches the key plus its delay.
    const auto& histories = history.Get(condition.TimeType);
    std::optional<TSystemTimestamp> reachedMax;
    // Streams whose reach instant precedes their history reached the key no later than their history start.
    std::optional<TSystemTimestamp> maxUnresolvedStartTime;
    bool unresolvable = false;
    for (const auto& [streamId, delay] : condition.StreamsWithDelays) {
        auto key = triggerTimestamp.Underlying() > std::numeric_limits<ui64>::max() - delay.Seconds()
            ? InfinitySystemTimestamp
            : TSystemTimestamp(triggerTimestamp.Underlying() + delay.Seconds());
        const auto* streamHistory = histories.FindPtr(streamId);
        if (!streamHistory || streamHistory->Elements.empty()) {
            unresolvable = true;
            continue;
        }

        auto reach = streamHistory->FindReachInstant(key);
        if (!reach) {
            maxUnresolvedStartTime = std::max(maxUnresolvedStartTime.value_or(ZeroSystemTimestamp), streamHistory->GetStartTime());
        } else if (*reach == InfinitySystemTimestamp) {
            return reach;
        } else {
            reachedMax = std::max(reachedMax.value_or(ZeroSystemTimestamp), *reach);
        }
    }

    if (unresolvable || !reachedMax || (maxUnresolvedStartTime && *reachedMax < *maxUnresolvedStartTime)) {
        return std::nullopt;
    }

    return reachedMax;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
