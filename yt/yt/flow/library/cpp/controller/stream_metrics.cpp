#include "stream_metrics.h"

#include <yt/yt/flow/library/cpp/common/spec.h>
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

namespace {

//! Timer streams count only triggered timers, other streams count all their messages.
TReadyVolume ComputeReadyVolume(const TStreamTraverseDataPtr& stream, bool timerStream)
{
    const auto& metrics = stream->InflightMetrics;
    if (!timerStream) {
        return {.Count = metrics->Count, .ByteSize = metrics->ByteSize};
    }

    // Without a reported ready volume the whole volume is the conservative fallback.
    TReadyVolume volume{.Count = std::min(metrics->ReadyCount.value_or(metrics->Count), metrics->Count)};
    if (metrics->ByteSize) {
        volume.ByteSize = std::min(metrics->ReadyByteSize.value_or(*metrics->ByteSize), *metrics->ByteSize);
    }

    return volume;
}

} // namespace

THashSet<TStreamId> CollectTimerStreamIds(const TPipelineSpecPtr& spec)
{
    THashSet<TStreamId> timerStreamIds;
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        for (const auto& timerStreamId : GetKeys(computationSpec->TimerStreams)) {
            timerStreamIds.insert(MakeGlobalStreamId(computationId, timerStreamId, computationSpec));
        }
    }

    return timerStreamIds;
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
    , ReadyCountLagGauge_(Profiler_.Gauge("/ready_count_lag"))
{ }

void TStreamMetrics::Apply(const TSystemTimestamp /*now*/, const TStreamTraverseDataPtr& stream, const TReadyVolume& readyVolume)
{
    TimeLagGauge_->Update(stream->SystemWatermark);
    EventTimeLagGauge_->Update(stream->EventWatermark);

    // Dropping the gauge removes the sensor, so an unresolvable ready watermark publishes no sample.
    if (stream->ReadyWatermarkUnknown) {
        ReadyTimeLagGauge_.Reset();
    } else {
        if (!ReadyTimeLagGauge_) {
            ReadyTimeLagGauge_ = TLagGauge::Create(Profiler_, "/ready_time_lag");
        }

        ReadyTimeLagGauge_->Update(stream->ReadyWatermark.value_or(stream->SystemWatermark));
    }

    const auto& metrics = stream->InflightMetrics;
    LagGauge_.Update(metrics->Count);
    ReadyCountLagGauge_.Update(std::max<i64>(0, readyVolume.Count));

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
    update(ReadyByteSizeLagGauge_, "/ready_byte_size_lag", metrics->ByteSize ? readyVolume.ByteSize : std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

void UpdateStreamMetrics(
    THashMap<TStreamId, TStreamMetrics>& streamMetrics,
    TSystemTimestamp now,
    const TPipelineTraverseDataPtr& traverseData,
    const THashSet<TStreamId>& timerStreamIds,
    bool enableUnitedStreamMetrics,
    const TProfiler& profiler)
{
    auto registerStream = [&] (const auto& streamId, const auto& stream, const TReadyVolume& readyVolume) {
        auto& metrics = streamMetrics
            .emplace(streamId, TStreamMetrics(streamId, profiler))
            .first->second;
        metrics.Apply(now, stream, readyVolume);
    };

    // The united stream mixes timer and other streams, so its ready volume is summed over its members.
    TReadyVolume unitedReadyVolume;
    for (const auto& [streamId, stream] : traverseData->Streams) {
        auto readyVolume = ComputeReadyVolume(stream, timerStreamIds.contains(streamId));
        registerStream(streamId, stream, readyVolume);
        unitedReadyVolume.Count += readyVolume.Count;

        if (readyVolume.ByteSize) {
            unitedReadyVolume.ByteSize = unitedReadyVolume.ByteSize.value_or(0) + *readyVolume.ByteSize;
        }
    }

    auto streamIds = GetKeys(traverseData->Streams);
    if (enableUnitedStreamMetrics) {
        auto registerUnitedStream = [&] (const auto& streamId, const auto& stream, bool timerStream) {
            registerStream(streamId, stream, ComputeReadyVolume(stream, timerStream));
        };
        static const TStreamId unitedStreamId = TStreamId("__united__");
        registerStream(unitedStreamId, traverseData->UnitedStream, unitedReadyVolume);
        static const TStreamId unitedSourceStreamId = TStreamId("__united_source__");
        registerUnitedStream(unitedSourceStreamId, traverseData->UnitedSourceStream, /*timerStream*/ false);
        static const TStreamId unitedTimerStreamId = TStreamId("__united_timer__");
        registerUnitedStream(unitedTimerStreamId, traverseData->UnitedTimerStream, /*timerStream*/ true);
        static const TStreamId unitedKeyVisitorStreamId = TStreamId("__united_key_visitor__");
        registerUnitedStream(unitedKeyVisitorStreamId, traverseData->UnitedKeyVisitorStream, /*timerStream*/ false);
        static const TStreamId unitedOutputStreamId = TStreamId("__united_output__");
        registerUnitedStream(unitedOutputStreamId, traverseData->UnitedOutputStream, /*timerStream*/ false);

        streamIds.insert(streamIds.end(), {unitedStreamId, unitedSourceStreamId, unitedTimerStreamId, unitedKeyVisitorStreamId, unitedOutputStreamId});
    }
    DropMissingKeys(streamMetrics, THashSet<TStreamId>(streamIds.begin(), streamIds.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
