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
    auto otherNode = CloneYsonStruct(node);
    otherNode->Streams["bigb/profile-hit-log"]->InflightMetrics->IdleDuration = TDuration::Seconds(10);
    ASSERT_TRUE(GetPartitionLastIdleTimestamp(node, spec));
    ASSERT_FALSE(GetPartitionLastIdleTimestamp(otherNode, spec));
    auto preparedNodes = ApplyEventWatermarkComputeRule(
        {
            {"default", {node, otherNode}},
        },
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
    ASSERT_TRUE(GetPartitionLastIdleTimestamp(idleNode, spec));
    ASSERT_FALSE(GetPartitionLastIdleTimestamp(activeNode, spec));

    auto errorState = CreateSyncStatusProfiler(NLogging::TLogger("TestPublic"))
        ->ErrorState("/idle_partitions_watermark_stall");

    // 1 of 2 partitions idle: IgnoreLimit = floor(0.4 * 2) = 0 < 1 idle < 2 total, so the stall is raised.
    ApplyEventWatermarkComputeRule(
        {{"default", {idleNode, activeNode}}},
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);
    ASSERT_TRUE(errorState->GetStatus().IsOK == false);

    // Raising MaxRatio to 1.0 lets the idle partition be ignored, so the stall clears.
    spec->WatermarkStrategy->WatermarkGenerator->IdlePartitions->MaxRatio = 1.0;
    ApplyEventWatermarkComputeRule(
        {{"default", {idleNode, activeNode}}},
        spec,
        {},
        NLogging::TLogger("Test"),
        errorState);
    ASSERT_TRUE(errorState->GetStatus().IsOK == true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
