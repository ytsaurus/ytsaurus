#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/core/test_framework/framework.h>

#include <numeric>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(THistogramTest, Compute)
{
    TFixedBinsHistogramBase hist({4, 8, 16});

    hist.RecordValue(4);
    hist.RecordValue(0);

    hist.RecordValue(8);
    hist.RecordValue(5);
    hist.RecordValue(6);

    hist.RecordValue(9);
    hist.RecordValue(15);
    hist.RecordValue(16);
    hist.RecordValue(20);
    hist.RecordValue(25);

    auto counters = hist.GetCounters();

    EXPECT_EQ(counters[0], 2);
    EXPECT_EQ(counters[1], 3);
    EXPECT_EQ(counters[2], 5);
}

TEST(THistogramTest, Quantiles)
{
    std::vector<i64> binValues(1000);
    std::iota(binValues.begin(), binValues.end(), 0);
    TFixedBinsHistogramBase hist(std::move(binValues));

    const i64 LargeValue = 2'700'000'001;

    for (int i = 0; i < std::ssize(hist.GetBins()); ++i) {
        hist.RecordValue(i, LargeValue);
    }

    auto summary = ComputeHistogramSummary(hist);

    EXPECT_EQ(summary.TotalCount, std::ssize(hist.GetBins()) * LargeValue);
    EXPECT_EQ(summary.P90, 900);
    EXPECT_EQ(summary.P99, 990);
    EXPECT_EQ(summary.P99_9, 999);
}

TEST(THistogramTest, QuantileBoundaryConditions)
{
    TFixedBinsHistogramBase hist({4, 8, 16});

    {
        auto summary = ComputeHistogramSummary(hist);
        EXPECT_EQ(summary.TotalCount, 0);
        EXPECT_EQ(summary.P90, 4);
        EXPECT_EQ(summary.P99, 4);
        EXPECT_EQ(summary.P99_9, 4);
    }

    hist.RecordValue(17);
    {
        auto summary = ComputeHistogramSummary(hist);
        EXPECT_EQ(summary.TotalCount, 1);
        EXPECT_EQ(summary.P90, 16);
        EXPECT_EQ(summary.P99, 16);
        EXPECT_EQ(summary.P99_9, 16);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
