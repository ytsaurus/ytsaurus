#include "stream_metrics.h"

#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NController {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TLagGaugePtr TLagGauge::Create(
    const TProfiler& profiler,
    const std::string& name)
{
    auto gauge = New<TLagGauge>();
    profiler.AddFuncGauge(name, gauge, [gauge = gauge.Get()] () -> double {
        auto watermark = TSystemTimestamp(gauge->Watermark_.load(std::memory_order::relaxed));
        if (watermark == ZeroSystemTimestamp) {
            return -1.0;
        }
        auto now = static_cast<i64>(TInstant::Now().Seconds());
        return static_cast<double>(std::max<i64>(0, now - static_cast<i64>(watermark.Underlying())));
    });
    return gauge;
}

void TLagGauge::Update(TSystemTimestamp watermark)
{
    Watermark_.store(watermark.Underlying(), std::memory_order::relaxed);
}

////////////////////////////////////////////////////////////////////////////////

TStreamMetrics::TStreamMetrics(const TStreamId& streamId, const TProfiler& profiler)
    : Profiler_(profiler
            .WithPrefix("/streams")
            .WithRequiredTag("stream_id", streamId.Underlying()))
    , TimeLagGauge_(TLagGauge::Create(Profiler_, "/time_lag"))
    , EventTimeLagGauge_(TLagGauge::Create(Profiler_, "/event_time_lag"))
    , LagGauge_(Profiler_.Gauge("/count_lag"))
    , ByteSizeLagGauge_()
{ }

void TStreamMetrics::Apply(const TSystemTimestamp /*now*/, const TStreamTraverseDataPtr& stream)
{
    TimeLagGauge_->Update(stream->SystemWatermark);
    EventTimeLagGauge_->Update(stream->EventWatermark);

    const auto& metrics = stream->InflightMetrics;
    LagGauge_.Update(metrics->Count);

    auto update = [&] (auto& gauge, const std::string& name, auto value) {
        if (value) {
            if (!gauge) {
                gauge = Profiler_.Gauge(name);
            }
            gauge->Update(*value);
        } else {
            gauge = {};
        }
    };

    update(ByteSizeLagGauge_, "/byte_size_lag", metrics->ByteSize);
}

////////////////////////////////////////////////////////////////////////////////

void UpdateStreamMetrics(
    THashMap<TStreamId, TStreamMetrics>& streamMetrics,
    TSystemTimestamp now,
    const TPipelineTraverseDataPtr& traverseData,
    bool enableUnitedStreamMetrics,
    const TProfiler& profiler)
{
    auto registerStream = [&] (const auto& streamId, const auto& stream) {
        auto& metrics = streamMetrics
            .emplace(streamId, TStreamMetrics(streamId, profiler))
            .first->second;
        metrics.Apply(now, stream);
    };

    for (const auto& [streamId, stream] : traverseData->Streams) {
        registerStream(streamId, stream);
    }
    auto streamIds = GetKeys(traverseData->Streams);
    if (enableUnitedStreamMetrics) {
        static const TStreamId unitedStreamId = TStreamId("__united__");
        registerStream(unitedStreamId, traverseData->UnitedStream);
        static const TStreamId unitedSourceStreamId = TStreamId("__united_source__");
        registerStream(unitedSourceStreamId, traverseData->UnitedSourceStream);
        static const TStreamId unitedTimerStreamId = TStreamId("__united_timer__");
        registerStream(unitedTimerStreamId, traverseData->UnitedTimerStream);
        static const TStreamId unitedKeyVisitorStreamId = TStreamId("__united_key_visitor__");
        registerStream(unitedKeyVisitorStreamId, traverseData->UnitedKeyVisitorStream);
        static const TStreamId unitedOutputStreamId = TStreamId("__united_output__");
        registerStream(unitedOutputStreamId, traverseData->UnitedOutputStream);

        streamIds.insert(streamIds.end(), {unitedStreamId, unitedSourceStreamId, unitedTimerStreamId, unitedKeyVisitorStreamId, unitedOutputStreamId});
    }
    DropMissingKeys(streamMetrics, THashSet<TStreamId>(streamIds.begin(), streamIds.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
