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
        UpdateStreamMetrics(streamMetrics, ZeroSystemTimestamp, traverseData, enableUnitedStreamMetrics, profiler);
        registry->Collect();

        EXPECT_EQ(readSensor("/count_lag", "data_with_united"), 1);
        for (const auto& [streamId, count] : unitedCounts) {
            if (enableUnitedStreamMetrics) {
                EXPECT_EQ(readSensor("/count_lag", streamId), count);
                EXPECT_EQ(readSensor("/byte_size_lag", streamId), count * 10);
                EXPECT_EQ(readSensor("/time_lag", streamId), -1);
                EXPECT_EQ(readSensor("/event_time_lag", streamId), -1);
            } else {
                for (auto name : {"/count_lag", "/byte_size_lag", "/time_lag", "/event_time_lag"}) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
