#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/controller_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TApplyEventWatermarkComputeRuleTest, Simple)
{
    TNodeTraverseDataPtr node = ConvertTo<TNodeTraverseDataPtr>(TYsonString(TStringBuf(R"""(
        {
            "report_time" = 1729497623u;
            "streams" = {
                "bigb/profile-hit-log" = {
                    "epoch" = 29238380;
                    "inflight_metrics" = {
                        "count" = 0;
                        "idle_duration" = 756996;
                        "last_idle_timestamp" = 1729497623u;
                    };
                    "state" = "active";
                    "system_watermark" = 1729497623u;
                    "event_watermark" = 1729497623u;
                };
                "bigb_profile_hit" = {
                    "epoch" = 29238380;
                    "inflight_metrics" = {
                        "byte_size" = 0;
                        "count" = 0;
                    };
                    "state" = "active";
                    "system_watermark" = 1729497623u;
                    "event_watermark" = 1729243989u;
                };
            };
        }
    )""")));
    TComputationSpecPtr spec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
        {
            "computation_class_name" = "NColibri::TBigbProfileHitReader";
            "group_by_schema" = [];
            "input_stream_ids" = [];
            "output_stream_ids" = ["bigb_profile_hit";];
            "parameters" = {};
            "source_streams" = {
                "bigb/profile-hit-log" = {
                    "parameters" = {
                        "consumer" = "colibri/prestable-consumer";
                        "logbroker" = "lbk";
                        "topic" = "bigb/profile-hit-log";
                    };
                    "source_class_name" = "NYT::NFlow::TLogbrokerSource";
                };
            };
            "streams_dependency" = {
                "bigb_profile_hit" = ["bigb/profile-hit-log";];
            };
            "watermark_strategy" = {
                "watermark_generator" = {
                    "idle_partitions" = {
                        "ignore" = %true;
                        "duration" = 120000;
                        "max_ratio" = 1.0;
                    };
                    "out_of_orderness_bound" = 120000;
                };
            };
        }
    )""")));
    const TStreamId sourceStreamId("bigb/profile-hit-log");
    auto otherNode = CloneYsonStruct(node);
    otherNode->Streams["bigb/profile-hit-log"]->InflightMetrics->IdleDuration = TDuration::Seconds(10);
    ASSERT_TRUE(GetPartitionLastIdleTimestamp(node, spec, sourceStreamId));
    ASSERT_FALSE(GetPartitionLastIdleTimestamp(otherNode, spec, sourceStreamId));
    auto preparedNodes = ApplyEventWatermarkComputeRule(
        {{sourceStreamId, {{"default", {node, otherNode}}}}},
        spec,
        {},
        NLogging::TLogger("Test"),
        CreateSyncStatusProfiler()->ErrorState("/idle_partitions_watermark_stall"));
    ASSERT_EQ(preparedNodes[0]->Streams["bigb_profile_hit"]->EventWatermark.Underlying(), 1729497503ull);
    ASSERT_EQ(preparedNodes[1]->Streams["bigb_profile_hit"]->EventWatermark.Underlying(), 1729243989ull);
}

////////////////////////////////////////////////////////////////////////////////

// When the idle fraction exceeds |MaxRatio| but stays below 100%, the excess idle partitions keep
// gating the watermark; the stall error state must be raised, then cleared once it no longer holds.
TEST(TApplyEventWatermarkComputeRuleTest, PartialIdleStall)
{
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
        {
            "computation_class_name" = "TestComputation";
            "group_by_schema" = [];
            "input_stream_ids" = [];
            "output_stream_ids" = ["output_stream";];
            "parameters" = {};
            "source_streams" = {
                "source_stream" = {};
            };
            "streams_dependency" = {
                "output_stream" = ["source_stream";];
            };
            "watermark_strategy" = {
                "watermark_generator" = {
                    "out_of_orderness_bound" = 120000;
                    "idle_partitions" = {
                        "duration" = 120000;
                        "max_ratio" = 0.4;
                    };
                };
            };
        }
    )""")));
    const TStreamId sourceStreamId("source_stream");

    auto idleNode = ConvertTo<TNodeTraverseDataPtr>(TYsonString(TStringBuf(R"""(
        {
            "report_time" = 1000u;
            "streams" = {
                "source_stream" = {
                    "inflight_metrics" = {
                        "count" = 0;
                        "idle_duration" = 600000;
                        "last_idle_timestamp" = 1000u;
                    };
                    "state" = "active";
                    "event_watermark" = 1000u;
                };
                "output_stream" = {
                    "inflight_metrics" = {"count" = 0;};
                    "state" = "active";
                    "event_watermark" = 500u;
                };
            };
        }
    )""")));
    // A non-idle partition: the same node, but its source stream has been empty for less than |duration|.
    auto activeNode = CloneYsonStruct(idleNode);
    activeNode->Streams["source_stream"]->InflightMetrics->IdleDuration = TDuration::Seconds(10);
    ASSERT_TRUE(GetPartitionLastIdleTimestamp(idleNode, spec, sourceStreamId));
    ASSERT_FALSE(GetPartitionLastIdleTimestamp(activeNode, spec, sourceStreamId));

    auto errorState = CreateSyncStatusProfiler(NLogging::TLogger("TestPublic"))
        ->ErrorState("/idle_partitions_watermark_stall");

    // 1 of 2 partitions idle: IgnoreLimit = floor(0.4 * 2) = 0 < 1 idle < 2 total, so the stall is raised.
    ApplyEventWatermarkComputeRule(
        {{sourceStreamId, {{"default", {idleNode, activeNode}}}}},
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);
    ASSERT_TRUE(errorState->GetStatus().IsOK == false);

    // Raising MaxRatio to 1.0 lets the idle partition be ignored, so the stall clears.
    spec->WatermarkStrategy->WatermarkGenerator->IdlePartitions->MaxRatio = 1.0;
    ApplyEventWatermarkComputeRule(
        {{sourceStreamId, {{"default", {idleNode, activeNode}}}}},
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);
    ASSERT_TRUE(errorState->GetStatus().IsOK == true);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TApplyEventWatermarkComputeRuleTest, MultipleSourcesAreProcessedIndependently)
{
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
        {
            "computation_class_name" = "TestComputation";
            "group_by_schema" = [];
            "input_stream_ids" = [];
            "output_stream_ids" = ["output_stream";];
            "parameters" = {};
            "source_streams" = {
                "first" = {};
                "second" = {};
            };
            "streams_dependency" = {
                "output_stream" = ["first"; "second";];
            };
            "watermark_strategy" = {
                "watermark_generator" = {
                    "idle_partitions" = {
                        "duration" = 1000;
                        "max_ratio" = 0.25;
                    };
                };
            };
        }
    )""")));

    const auto now = TSystemTimestamp(1000);
    const auto outdated = TSystemTimestamp(500);
    auto makeNode = [&] (const TStreamId& activeSource, bool idle) {
        auto node = New<TNodeTraverseData>();
        node->ReportTime = now;
        for (const auto& sourceId : {TStreamId("first"), TStreamId("second")}) {
            auto stream = New<TStreamTraverseData>();
            stream->State = EStreamState::Active;
            stream->EventWatermark = now;
            stream->InflightMetrics = New<TInflightMetrics>();
            if (sourceId == activeSource) {
                stream->InflightMetrics->IdleDuration = idle
                    ? TDuration::Seconds(2)
                    : TDuration::MilliSeconds(100);
                stream->InflightMetrics->LastIdleTimestamp = now;
            } else {
                stream->InflightMetrics->Count = 1;
            }
            node->Streams[sourceId] = std::move(stream);
        }
        auto outputStream = New<TStreamTraverseData>();
        outputStream->State = EStreamState::Active;
        outputStream->EventWatermark = idle ? outdated : now;
        outputStream->InflightMetrics = New<TInflightMetrics>();
        node->Streams["output_stream"] = std::move(outputStream);
        return node;
    };

    auto idleFirst = makeNode("first", true);
    auto activeFirst = makeNode("first", false);
    auto activeSecond1 = makeNode("second", false);
    auto activeSecond2 = makeNode("second", false);
    auto errorState = CreateSyncStatusProfiler(NLogging::TLogger("TestPublic"))
        ->ErrorState("/idle_partitions_watermark_stall");

    auto preparedNodes = ApplyEventWatermarkComputeRule(
        {
            {TStreamId("first"), {{"default", {idleFirst, activeFirst}}}},
            {TStreamId("second"), {{"default", {activeSecond1, activeSecond2}}}},
        },
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);

    int outdatedNodes = 0;
    for (const auto& node : preparedNodes) {
        outdatedNodes += node->Streams["output_stream"]->EventWatermark == outdated;
    }
    EXPECT_EQ(outdatedNodes, 1);
    EXPECT_TRUE(errorState->GetStatus().IsOK == false);

    auto anotherIdleFirst = makeNode("first", true);
    preparedNodes = ApplyEventWatermarkComputeRule(
        {
            {TStreamId("first"), {{"default", {idleFirst, anotherIdleFirst}}}},
            {TStreamId("second"), {{"default", {activeSecond1, activeSecond2}}}},
        },
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);

    // A fully idle source still gates the merged watermark, but is not a partial-idle stall.
    auto mergedTraverseData = MergeNodeTraverseData(preparedNodes);
    EXPECT_EQ(mergedTraverseData->Streams["output_stream"]->EventWatermark, outdated);
    EXPECT_TRUE(errorState->GetStatus().IsOK);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
