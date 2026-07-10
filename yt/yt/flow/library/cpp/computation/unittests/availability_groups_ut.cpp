#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/controller_base.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogging;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TApplyAvailabilityGroupsEventWatermarkComputeRuleTest, Simple)
{
    const TComputationSpecPtr spec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
        {
            "computation_class_name" = "NColibri::TBigbProfileHitReader";
            "group_by_schema" = [];
            "input_stream_ids" = [];
            "output_stream_ids" = ["bigb_profile_hit";];
            "parameters" = {};
            "source_streams" = {
                "bigb/profile-hit-log" = {
                };
            };
            "streams_dependency" = {
                "bigb_profile_hit" = ["bigb/profile-hit-log";];
            };
            "watermark_strategy" = {
                "watermark_generator" = {
                    "out_of_orderness_bound" = 1000;
                    "unavailable_partition_groups" = {
                        "max_groups" = 1;
                    };
                }
            };
        }
    )""")));

    const auto now = TSystemTimestamp(TInstant::ParseIso8601("2024-01-01T15:00:00Z").Seconds());
    const auto outdatedTimestamp = TSystemTimestamp(now.Underlying() - 15);
    const auto hiddenWatermark = TSystemTimestamp(now.Underlying() - 1);
    const TNodeTraverseDataPtr defaultNode = ConvertTo<TNodeTraverseDataPtr>(TYsonString(TStringBuf(R"""(
        {
            "streams" = {
                "bigb/profile-hit-log" = {
                    "inflight_metrics" = {
                        "zero_count_duration" = 100000;
                        "count" = 0;
                    };
                    "state" = "active";
                };
                "bigb_profile_hit" = {
                    "inflight_metrics" = {
                        "byte_size" = 0;
                        "count" = 0;
                    };
                    "state" = "active";
                    "event_watermark" = 0u;
                };
            };
        }
    )""")));
    defaultNode->ReportTime = now;
    defaultNode->Streams["bigb/profile-hit-log"]->InflightMetrics->UnavailableTimestamp = std::nullopt;
    defaultNode->Streams["bigb_profile_hit"]->EventWatermark = now;

    ASSERT_FALSE(GetPartitionLastUnavailableTimestamp(defaultNode, spec, TLogger("Test")));

    const auto unavailableNode = CloneYsonStruct(defaultNode);
    unavailableNode->Streams["bigb/profile-hit-log"]->InflightMetrics->UnavailableTimestamp = now;
    unavailableNode->Streams["bigb_profile_hit"]->EventWatermark = outdatedTimestamp;
    ASSERT_TRUE(GetPartitionLastUnavailableTimestamp(unavailableNode, spec, TLogger("Test")));

    // One availability group, partially unavailable. Do nothing.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"default", {defaultNode, unavailableNode}},
            },
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        ASSERT_EQ(availablePartitionNodes[0], defaultNode);
        ASSERT_EQ(availablePartitionNodes[1], unavailableNode);
    }

    // One fully unavailable availability group. MaxUnavailableGroups = 1. Watermark should be hidden.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"default", {unavailableNode, unavailableNode}},
            },
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(node->Streams["bigb_profile_hit"]->EventWatermark, hiddenWatermark);
        }
    }

    // Two availability groups. Everything is OK. Do nothing.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"sas", {defaultNode, defaultNode}},
                {"vla", {defaultNode, unavailableNode}},
            },
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 4u);
    }

    // Two availability groups. One of them is fully unavailable. Watermark should be hidden.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"sas", {defaultNode, defaultNode}},
                {"vla", {unavailableNode, unavailableNode}},
            },
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 4u);
        for (const auto& node : availablePartitionNodes) {
            if (node != defaultNode) {
                ASSERT_EQ(node->Streams["bigb_profile_hit"]->EventWatermark, hiddenWatermark);
            }
        }
    }

    // Two availability groups. All of them are fully unavailable. Do nothing.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"sas", {unavailableNode, unavailableNode}},
                {"vla", {unavailableNode, unavailableNode}},
            },
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 4u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(ConvertToYsonString(node), ConvertToYsonString(unavailableNode));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
