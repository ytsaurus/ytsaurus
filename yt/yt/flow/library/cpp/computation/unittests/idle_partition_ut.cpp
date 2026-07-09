#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/controller_base.h>

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
        NLogging::TLogger("Test"));
    ASSERT_EQ(preparedNodes[0]->Streams["bigb_profile_hit"]->EventWatermark.Underlying(), 1729497503ull);
    ASSERT_EQ(preparedNodes[1]->Streams["bigb_profile_hit"]->EventWatermark.Underlying(), 1729243989ull);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
