#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/controller_base.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

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
                        "max_unavailable_groups" = 1;
                    };
                }
            };
        }
    )""")));
    const TStreamId sourceStreamId("bigb/profile-hit-log");

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

    const auto unavailableNode = CloneYsonStruct(defaultNode);
    unavailableNode->Streams["bigb/profile-hit-log"]->InflightMetrics->UnavailableTimestamp = now;
    unavailableNode->Streams["bigb_profile_hit"]->EventWatermark = outdatedTimestamp;

    // One availability group, partially unavailable. Do nothing.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {defaultNode, unavailableNode}}},
            sourceStreamId,
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        ASSERT_EQ(availablePartitionNodes[0], defaultNode);
        ASSERT_EQ(availablePartitionNodes[1], unavailableNode);
    }

    // One fully unavailable availability group. Default min_available_groups = 1 requires at least
    // one available group, so the watermark must NOT be hidden.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {unavailableNode, unavailableNode}}},
            sourceStreamId,
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(ConvertToYsonString(node), ConvertToYsonString(unavailableNode));
        }
    }

    // One fully unavailable availability group with min_available_groups = 0. Watermark should be hidden.
    {
        auto zeroMinSpec = CloneYsonStruct(spec);
        zeroMinSpec->WatermarkStrategy->WatermarkGenerator->UnavailablePartitionGroups->MinAvailableGroups = 0;
        THashSet<std::string> suppressedGroups;
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {unavailableNode, unavailableNode}}},
            sourceStreamId,
            zeroMinSpec,
            TSensorsOwner(),
            TLogger("Test"),
            &suppressedGroups);
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(node->Streams["bigb_profile_hit"]->EventWatermark, hiddenWatermark);
        }
        // The group was skipped, so its errors are redundant and may be silenced.
        ASSERT_EQ(suppressedGroups, THashSet<std::string>{"default"});
    }

    // The same group, but min_available_groups = 1 forbids skipping it. The pipeline now stalls on it, so
    // its errors are the explanation of the stall and must not be silenced.
    {
        THashSet<std::string> suppressedGroups;
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {unavailableNode, unavailableNode}}},
            sourceStreamId,
            spec,
            TSensorsOwner(),
            TLogger("Test"),
            &suppressedGroups);
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        ASSERT_TRUE(suppressedGroups.empty());
    }

    // A partially unavailable group is not skipped either, so it is not reported.
    {
        THashSet<std::string> suppressedGroups;
        ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {defaultNode, unavailableNode}}},
            sourceStreamId,
            spec,
            TSensorsOwner(),
            TLogger("Test"),
            &suppressedGroups);
        ASSERT_TRUE(suppressedGroups.empty());
    }

    // Two availability groups. Everything is OK. Do nothing.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"sas", {defaultNode, defaultNode}},
                {"vla", {defaultNode, unavailableNode}},
            },
            sourceStreamId,
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
            sourceStreamId,
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
            sourceStreamId,
            spec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 4u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(ConvertToYsonString(node), ConvertToYsonString(unavailableNode));
        }
    }

    // watermark_generator without explicit options must use zero out-of-orderness and default to
    // max_unavailable_groups = 1, min_available_groups = 1.
    const TComputationSpecPtr explicitDefaultSpec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
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
                "watermark_generator" = {}
            };
        }
    )""")));
    ASSERT_EQ(explicitDefaultSpec->WatermarkStrategy->WatermarkGenerator->OutOfOrdernessBound, TDuration::Zero());

    // Three availability groups, one fully unavailable. The default cap hides it, advancing its
    // watermark to now because out-of-orderness is zero.
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {
                {"sas", {defaultNode, defaultNode}},
                {"vla", {defaultNode, defaultNode}},
                {"klg", {unavailableNode, unavailableNode}},
            },
            sourceStreamId,
            explicitDefaultSpec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 6u);
        for (const auto& node : availablePartitionNodes) {
            if (node != defaultNode) {
                ASSERT_EQ(node->Streams["bigb_profile_hit"]->EventWatermark, now);
            }
        }
    }

    // Single fully unavailable availability group. Watermark must NOT be hidden (default min_available_groups = 1).
    {
        auto availablePartitionNodes = ApplyAvailabilityGroupsEventWatermarkComputeRule(
            {{"default", {unavailableNode, unavailableNode}}},
            sourceStreamId,
            explicitDefaultSpec,
            TSensorsOwner(),
            TLogger("Test"));
        ASSERT_EQ(availablePartitionNodes.size(), 2u);
        for (const auto& node : availablePartitionNodes) {
            ASSERT_EQ(ConvertToYsonString(node), ConvertToYsonString(unavailableNode));
        }
    }
}

TEST(TApplyAvailabilityGroupsEventWatermarkComputeRuleTest, MultipleSources)
{
    const auto spec = ConvertTo<TComputationSpecPtr>(TYsonString(TStringBuf(R"""(
        {
            "computation_class_name" = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
            "output_stream_ids" = ["output";];
            "source_streams" = {
                "first" = {};
                "second" = {};
            };
            "streams_dependency" = {
                "output" = ["first"; "second";];
            };
        }
    )""")));
    ASSERT_TRUE(spec->WatermarkStrategy->WatermarkGenerator);
    ASSERT_EQ(spec->WatermarkStrategy->WatermarkGenerator->OutOfOrdernessBound, TDuration::Zero());

    const auto now = TSystemTimestamp(TInstant::ParseIso8601("2024-01-01T15:00:00Z").Seconds());
    const auto outdatedTimestamp = TSystemTimestamp(now.Underlying() - 15);

    auto makeNode = [&] (const TStreamId& activeSource, bool unavailable) {
        auto node = New<TNodeTraverseData>();
        node->ReportTime = now;
        for (const auto& sourceStream : {TStreamId("first"), TStreamId("second")}) {
            auto stream = New<TStreamTraverseData>();
            stream->State = EStreamState::Active;
            stream->InflightMetrics = New<TInflightMetrics>();
            if (sourceStream != activeSource || unavailable) {
                stream->InflightMetrics->UnavailableTimestamp = now;
            }
            if (sourceStream == activeSource) {
                stream->InflightMetrics->IdleDuration = TDuration::MilliSeconds(100);
                stream->InflightMetrics->LastIdleTimestamp = now;
            }
            node->Streams[sourceStream] = std::move(stream);
        }
        auto outputStream = New<TStreamTraverseData>();
        outputStream->State = EStreamState::Active;
        outputStream->EventWatermark = unavailable ? outdatedTimestamp : now;
        outputStream->InflightMetrics = New<TInflightMetrics>();
        node->Streams["output"] = std::move(outputStream);
        return node;
    };

    const auto firstDown = makeNode("first", true);
    const auto secondDown = makeNode("second", true);
    const auto firstSas = makeNode("first", false);
    const auto secondVla = makeNode("second", false);
    TNodesByAvailabilityGroupBySource nodesByAvailabilityGroupBySource = {
        {
            TStreamId("first"),
            {
                {"down", {firstDown}},
                {"sas", {firstSas}},
            },
        },
        {
            TStreamId("second"),
            {
                {"down", {secondDown}},
                {"vla", {secondVla}},
            },
        },
    };
    TSuppressedAvailabilityGroupsBySource suppressedGroupsBySource;
    auto preparedNodes = ApplyEventWatermarkComputeRule(
        nodesByAvailabilityGroupBySource,
        spec,
        TSensorsOwner(),
        TLogger("Test"),
        CreateSyncStatusProfiler()->ErrorState("/idle_partitions_watermark_stall"),
        &suppressedGroupsBySource);
    ASSERT_EQ(suppressedGroupsBySource.at("first"), THashSet<std::string>{"down"});
    ASSERT_EQ(suppressedGroupsBySource.at("second"), THashSet<std::string>{"down"});
    ASSERT_EQ(preparedNodes.size(), 4u);
    for (const auto& node : preparedNodes) {
        EXPECT_EQ(node->Streams["output"]->EventWatermark, now);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
