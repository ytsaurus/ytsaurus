#include <yt/yt/flow/library/cpp/controller/stream_metrics.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TStreamMetricsTest, UnitedStreamMetrics)
{
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto profiler = NProfiling::TProfiler(registry, "/controller");

    auto makeStream = [] (i64 count) {
        auto stream = New<TStreamTraverseData>();
        stream->InflightMetrics->Count = count;
        stream->InflightMetrics->ByteSize = count * 10;
        return stream;
    };
    auto traverseData = New<TPipelineTraverseData>();
    traverseData->Streams[TStreamId("data_with_united")] = makeStream(1);
    traverseData->UnitedStream = makeStream(2);
    traverseData->UnitedSourceStream = makeStream(3);
    traverseData->UnitedTimerStream = makeStream(4);
    traverseData->UnitedKeyVisitorStream = makeStream(5);
    traverseData->UnitedOutputStream = makeStream(6);
    const std::vector<std::pair<std::string, i64>> unitedCounts = {
        {"__united__", 2},
        {"__united_source__", 3},
        {"__united_timer__", 4},
        {"__united_key_visitor__", 5},
        {"__united_output__", 6},
    };

    auto readSensor = [&] (TStringBuf name, const std::string& streamId) {
        return ConvertTo<double>(BuildYsonStringFluently().Do([&] (TFluentAny fluent) {
            registry->ReadRecentSensorValues(Format("yt/controller/streams%v", name), {{"stream_id", streamId}}, {}, fluent);
        }));
    };

    THashMap<TStreamId, TStreamMetrics> streamMetrics;
    auto updateAndCheck = [&] (bool enableUnitedStreamMetrics) {
        UpdateStreamMetrics(streamMetrics, ZeroSystemTimestamp, traverseData, /*timerStreamIds*/ {}, enableUnitedStreamMetrics, profiler);
        registry->Collect();

        EXPECT_EQ(readSensor("/count_lag", "data_with_united"), 1);
        for (const auto& [streamId, count] : unitedCounts) {
            if (enableUnitedStreamMetrics) {
                EXPECT_EQ(readSensor("/count_lag", streamId), count);
                EXPECT_EQ(readSensor("/byte_size_lag", streamId), count * 10);
                EXPECT_EQ(readSensor("/time_lag", streamId), -1);
                EXPECT_EQ(readSensor("/event_time_lag", streamId), -1);
            } else {
                for (auto name : {"/count_lag", "/byte_size_lag", "/time_lag", "/event_time_lag", "/ready_time_lag", "/ready_count_lag", "/ready_byte_size_lag"}) {
                    EXPECT_THROW_WITH_ERROR_CODE(readSensor(name, streamId), NYTree::EErrorCode::ResolveError);
                }
            }
        }
    };

    updateAndCheck(false);
    EXPECT_TRUE(New<TDynamicPipelineSpec>()->EnableUnitedStreamMetrics);
    updateAndCheck(New<TDynamicPipelineSpec>()->EnableUnitedStreamMetrics);
    updateAndCheck(false);
    updateAndCheck(true);
}

TEST(TStreamMetricsTest, ReadyMetrics)
{
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto profiler = NProfiling::TProfiler(registry, "/controller");

    auto makeStream = [] (i64 count, std::optional<i64> byteSize, i64 readyCount, std::optional<i64> readyByteSize) {
        auto stream = New<TStreamTraverseData>();
        stream->SystemWatermark = TSystemTimestamp(1);
        stream->InflightMetrics->Count = count;
        stream->InflightMetrics->ByteSize = byteSize;
        stream->InflightMetrics->ReadyCount = readyCount;
        stream->InflightMetrics->ReadyByteSize = readyByteSize;
        return stream;
    };
    const std::string timerStreamName = "computation/timer";
    const TStreamId timerStreamId(timerStreamName);
    auto timerStream = makeStream(/*count*/ 10, /*byteSize*/ 100, /*readyCount*/ 3, /*readyByteSize*/ 30);
    timerStream->ReadyWatermark = TSystemTimestamp(1);
    auto traverseData = New<TPipelineTraverseData>();
    traverseData->Streams[timerStreamId] = timerStream;
    // Other streams count all their messages, whatever their lifecycle ready volume is.
    traverseData->Streams[TStreamId("events")] = makeStream(/*count*/ 5, /*byteSize*/ 50, /*readyCount*/ 1, /*readyByteSize*/ 10);
    traverseData->Streams[TStreamId("no_byte_size")] = makeStream(/*count*/ 2, /*byteSize*/ std::nullopt, /*readyCount*/ 0, /*readyByteSize*/ std::nullopt);
    traverseData->UnitedStream = makeStream(/*count*/ 17, /*byteSize*/ 150, /*readyCount*/ 4, /*readyByteSize*/ 40);
    traverseData->UnitedTimerStream = makeStream(/*count*/ 10, /*byteSize*/ 100, /*readyCount*/ 3, /*readyByteSize*/ 30);

    auto readSensor = [&] (TStringBuf name, const std::string& streamId) {
        return ConvertTo<double>(BuildYsonStringFluently().Do([&] (TFluentAny fluent) {
            registry->ReadRecentSensorValues(Format("yt/controller/streams%v", name), {{"stream_id", streamId}}, {}, fluent);
        }));
    };

    THashMap<TStreamId, TStreamMetrics> streamMetrics;
    auto update = [&] {
        UpdateStreamMetrics(streamMetrics, ZeroSystemTimestamp, traverseData, {timerStreamId}, /*enableUnitedStreamMetrics*/ true, profiler);
        registry->Collect();
    };
    update();

    EXPECT_EQ(readSensor("/ready_count_lag", timerStreamName), 3);
    EXPECT_EQ(readSensor("/ready_byte_size_lag", timerStreamName), 30);
    EXPECT_EQ(readSensor("/ready_count_lag", "events"), 5);
    EXPECT_EQ(readSensor("/ready_byte_size_lag", "events"), 50);
    EXPECT_EQ(readSensor("/ready_count_lag", "no_byte_size"), 2);
    EXPECT_THROW_WITH_ERROR_CODE(readSensor("/ready_byte_size_lag", "no_byte_size"), NYTree::EErrorCode::ResolveError);
    // The united stream sums its members: the timer stream's ready part and the other streams' whole volume.
    EXPECT_EQ(readSensor("/ready_count_lag", "__united__"), 10);
    EXPECT_EQ(readSensor("/ready_byte_size_lag", "__united__"), 80);
    EXPECT_EQ(readSensor("/ready_count_lag", "__united_timer__"), 3);
    EXPECT_EQ(readSensor("/ready_byte_size_lag", "__united_timer__"), 30);
    EXPECT_GT(readSensor("/ready_time_lag", timerStreamName), 0);

    // An unresolvable ready watermark publishes no ready time lag, while the volume lags stay.
    timerStream->ReadyWatermark.reset();
    timerStream->ReadyWatermarkUnknown = true;
    update();
    EXPECT_THROW_WITH_ERROR_CODE(readSensor("/ready_time_lag", timerStreamName), NYTree::EErrorCode::ResolveError);
    EXPECT_EQ(readSensor("/ready_count_lag", timerStreamName), 3);

    timerStream->ReadyWatermark = TSystemTimestamp(1);
    timerStream->ReadyWatermarkUnknown = false;
    update();
    EXPECT_GT(readSensor("/ready_time_lag", timerStreamName), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
