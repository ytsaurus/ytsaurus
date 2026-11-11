#include "watermark_history.h"

#include <yt/yt/flow/library/cpp/misc/exponential_history.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TWatermarkHistoryElement::Register(TRegistrar registrar)
{
    registrar.Parameter("time", &TThis::Time)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("watermark", &TThis::Watermark)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("first_sample_time", &TThis::FirstSampleTime)
        .Default(ZeroSystemTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkHistory::Register(TRegistrar registrar)
{
    registrar.Parameter("elements", &TThis::Elements)
        .Default();
}

void TWatermarkHistory::Append(TSystemTimestamp instant, TSystemTimestamp watermark, int thinningFactor)
{
    if (!Elements.empty() && (instant <= Elements.back().Time || watermark < Elements.back().Watermark)) {
        return;
    }

    TWatermarkHistoryElement sample;
    sample.Time = instant;
    sample.Watermark = watermark;
    sample.FirstSampleTime = instant;
    AppendToExponentialHistory(
        &Elements,
        std::move(sample),
        [] (const TWatermarkHistoryElement& element) {
            return element.Time.Underlying();
        },
        [] (const TWatermarkHistoryElement& older, const TWatermarkHistoryElement& newer) {
            auto merged = newer;
            merged.FirstSampleTime = older.FirstSampleTime;
            return merged;
        },
        thinningFactor);
}

std::optional<TSystemTimestamp> TWatermarkHistory::FindReachInstant(TSystemTimestamp watermark) const
{
    if (Elements.empty() || watermark <= Elements.front().Watermark) {
        return std::nullopt;
    }

    auto it = std::partition_point(Elements.begin(), Elements.end(), [&] (const TWatermarkHistoryElement& element) {
        return element.Watermark < watermark;
    });
    if (it == Elements.end()) {
        return InfinitySystemTimestamp;
    }

    // The watermark was first published by some sample folded into the element. Its oldest sample never
    // under-reports the lag and over-reports it by at most the element's span.
    return it->FirstSampleTime;
}

TSystemTimestamp TWatermarkHistory::GetStartTime() const
{
    YT_VERIFY(!Elements.empty());

    return Elements.front().Time;
}

////////////////////////////////////////////////////////////////////////////////

void TPipelineWatermarkHistory::Register(TRegistrar registrar)
{
    registrar.Parameter("event", &TThis::Event)
        .Default();
    registrar.Parameter("system", &TThis::System)
        .Default();
}

THashMap<TStreamId, TWatermarkHistory>& TPipelineWatermarkHistory::Get(ETimeType timeType)
{
    YT_VERIFY(timeType != ETimeType::CurrentTime);

    return timeType == ETimeType::EventTime ? Event : System;
}

const THashMap<TStreamId, TWatermarkHistory>& TPipelineWatermarkHistory::Get(ETimeType timeType) const
{
    YT_VERIFY(timeType != ETimeType::CurrentTime);

    return timeType == ETimeType::EventTime ? Event : System;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
