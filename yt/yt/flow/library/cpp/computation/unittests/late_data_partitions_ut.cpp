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

class TLateDataPartitionsTestBase
    : public ::testing::Test
{
protected:
    static constexpr auto BaseTime = "2024-01-01T15:00:00Z";
    static constexpr TDuration OutOfOrdernessBound = TDuration::Seconds(60);

    TSystemTimestamp Now() const
    {
        return TSystemTimestamp(TInstant::ParseIso8601(BaseTime).Seconds());
    }

    TSystemTimestamp TimestampWithOffset(TDuration offset) const
    {
        return TSystemTimestamp(Now().Underlying() - offset.Seconds());
    }

    TComputationSpecPtr CreateSpec(
        TWatermarkPercentile value,
        TDuration delay,
        TDuration outOfOrdernessBound = OutOfOrdernessBound) const
    {
        auto specYson = Format(R"""(
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
                        "out_of_orderness_bound" = "%v";
                        "late_data_partitions" = {
                            "value" = %v;
                            "delay" = "%v";
                        };
                    };
                };
            }
        )""",
            outOfOrdernessBound,
            value.Underlying(),
            delay);

        return ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    }

    TNodeTraverseDataPtr CreateNode(TSystemTimestamp watermark) const
    {
        auto nodeYson = Format(R"""(
            {
                "report_time" = %vu;
                "streams" = {
                    "source_stream" = {
                        "inflight_metrics" = {
                            "count" = 0;
                        };
                        "state" = "active";
                        "event_watermark" = %vu;
                    };
                    "output_stream" = {
                        "inflight_metrics" = {
                            "count" = 0;
                        };
                        "state" = "active";
                        "event_watermark" = %vu;
                    };
                };
            }
        )""",
            Now().Underlying(),
            watermark.Underlying(),
            watermark.Underlying());

        return ConvertTo<TNodeTraverseDataPtr>(TYsonStringBuf(nodeYson));
    }

    std::vector<TNodeTraverseDataPtr> CreateNodes(const std::vector<TSystemTimestamp>& watermarks) const
    {
        std::vector<TNodeTraverseDataPtr> nodes;
        nodes.reserve(watermarks.size());
        for (const auto& watermark : watermarks) {
            nodes.push_back(CreateNode(watermark));
        }
        return nodes;
    }

    std::vector<TNodeTraverseDataPtr> ApplyRule(
        const std::vector<TNodeTraverseDataPtr>& nodes,
        const TComputationSpecPtr& spec) const
    {
        return ApplyEventWatermarkComputeRule(
            {{"default", nodes}},
            spec,
            TSensorsOwner(),
            TLogger("Test"));
    }

    i32 CountNodesWithWatermark(
        const std::vector<TNodeTraverseDataPtr>& nodes,
        TSystemTimestamp watermark) const
    {
        i32 count = 0;
        for (const auto& node : nodes) {
            if (node->Streams["output_stream"]->EventWatermark == watermark) {
                count++;
            }
        }
        return count;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLateDataPartitionsTestParams
{
    std::string Name;

    double Value;
    TDuration Delay;

    std::vector<TDuration> WatermarkOffsets;

    struct TExpectedResult
    {
        TDuration WatermarkOffset; // Offset from Now
        int Count;
    };

    std::vector<TExpectedResult> ExpectedWatermarks;
};

class TLateDataPartitionsParameterizedTest
    : public TLateDataPartitionsTestBase
    , public ::testing::WithParamInterface<TLateDataPartitionsTestParams>
{
};

TEST_P(TLateDataPartitionsParameterizedTest, Scenario)
{
    const auto& params = GetParam();

    auto spec = CreateSpec(
        TWatermarkPercentile(params.Value),
        params.Delay);

    std::vector<TSystemTimestamp> watermarks;
    for (const auto& offset : params.WatermarkOffsets) {
        watermarks.push_back(TimestampWithOffset(offset));
    }
    auto nodes = CreateNodes(watermarks);
    auto result = ApplyRule(nodes, spec);

    ASSERT_EQ(result.size(), nodes.size());
    for (const auto& expected : params.ExpectedWatermarks) {
        auto expectedWatermark = TimestampWithOffset(expected.WatermarkOffset);
        int actualCount = CountNodesWithWatermark(result, expectedWatermark);
        EXPECT_EQ(actualCount, expected.Count)
            << "Expected " << expected.Count << " nodes with watermark offset "
            << expected.WatermarkOffset.ToString() << ", got " << actualCount;
    }
}

INSTANTIATE_TEST_SUITE_P(
    LateDataPartitions,
    TLateDataPartitionsParameterizedTest,
    ::testing::Values(
        TLateDataPartitionsTestParams{
            .Name = "WithinDelayTolerance",
            .Value = 50.0,
            .Delay = TDuration::Minutes(12),
            .WatermarkOffsets = {
                TDuration::Zero(),      // Normal
                TDuration::Minutes(10), // 10 minutes behind
            },
            // Percentile=50, 2 partitions, index=1
            // PercentileWatermark = now, thresholdWatermark = now - 12m
            // LateTimestamp = now - 10m >= threshold, NOT ignored
            .ExpectedWatermarks = {
                {TDuration::Zero(), 1},
                {TDuration::Minutes(10), 1},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "BeyondDelayIsIgnored",
            .Value = 50.0,
            .Delay = TDuration::Minutes(5),
            .WatermarkOffsets = {
                TDuration::Zero(),      // Normal
                TDuration::Minutes(10), // 10 minutes behind
            },
            // Percentile=50, index=1, percentileWatermark = now
            // ThresholdWatermark = now - 5m
            // LateTimestamp = now - 10m < threshold -> hidden
            // EffectiveWatermark = max(now - 10m, now - 5m) = now - 5m
            // HiddenWatermark = now - 5m - 1m(OutOfOrdernessBound) = now - 6m
            .ExpectedWatermarks = {
                {TDuration::Zero(), 1},
                {TDuration::Minutes(6), 1},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "AllSameWatermark",
            .Value = 95.0,
            .Delay = TDuration::Minutes(1),
            .WatermarkOffsets = {
                TDuration::Zero(),
                TDuration::Zero(),
            },
            // All nodes have the same watermark, no hidden watermarks
            .ExpectedWatermarks = {
                {TDuration::Zero(), 2},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "ManyPartitionsOneLate",
            .Value = 90.0,
            .Delay = TDuration::Minutes(1),
            .WatermarkOffsets = {
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                // 1 very late
                TDuration::Minutes(10),
            },
            // Percentile=90, index = 10 * (100-90)/100 = 1
            // PercentileWatermark = now, thresholdWatermark = now - 1m
            // LateTimestamp = now - 10m < threshold -> hidden
            // HiddenWatermark = now - 1m - 1m(OutOfOrdernessBound) = now - 2m
            .ExpectedWatermarks = {
                {TDuration::Zero(), 9},
                {TDuration::Minutes(2), 1},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "ManyLatePartitionsPercentileLimit",
            .Value = 90.0,
            .Delay = TDuration::Minutes(1),
            .WatermarkOffsets = {
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Minutes(10),
                TDuration::Minutes(10),
                TDuration::Minutes(10),
                TDuration::Minutes(10),
                TDuration::Minutes(10),
            },
            // Percentile=90, index = 1, percentileWatermark = partitions[1] = now - 10m
            // ThresholdWatermark = now - 10m - 1m = now - 11m
            // All watermarks >= threshold, hide nothing
            .ExpectedWatermarks = {
                {TDuration::Zero(), 5},
                {TDuration::Minutes(10), 5},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "LowPercentileIgnoresMore",
            .Value = 50.0,
            .Delay = TDuration::Seconds(30),
            .WatermarkOffsets = {
                TDuration::Zero(),
                TDuration::Zero(),
                TDuration::Minutes(5),
                TDuration::Minutes(5),
            },
            // Percentile=50, index = 4 * (100-50)/100 = 2
            // PercentileWatermark = now, thresholdWatermark = now - 30s
            // Both now-5m < threshold -> hidden to now - 30s - 1m = now - 1m30s
            .ExpectedWatermarks = {
                {TDuration::Zero(), 2},
                {TDuration::Seconds(90), 2},
            },
        },

        TLateDataPartitionsTestParams{
            .Name = "Percentile100DisablesIgnoring",
            .Value = 100.0,
            .Delay = TDuration::Seconds(0),
            .WatermarkOffsets = {
                TDuration::Zero(),
                TDuration::Minutes(30),
            },
            // Percentile=100, index = 2 * (100-100)/100 = 0
            // PercentileWatermark = oldest = now - 30m
            // ThresholdWatermark = now - 30m <= percentileWatermark, hide nothing
            .ExpectedWatermarks = {
                {TDuration::Zero(), 1},
                {TDuration::Minutes(30), 1},
            },
        }),
    [] (const ::testing::TestParamInfo<TLateDataPartitionsTestParams>& info) {
        return info.param.Name;
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
