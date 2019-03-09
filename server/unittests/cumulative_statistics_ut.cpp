#include <yt/core/test_framework/framework.h>

#include <yt/server/master/chunk_server/cumulative_statistics.h>

namespace NYT::NChunkServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(CumulativeStatistics, SimpleAppendable)
{
    TCumulativeStatistics stats;
    stats.DeclareAppendable();

    EXPECT_EQ(stats.Size(), 0);

    stats.PushBack({1, 10, 100});
    stats.PushBack({2, 20, 200});

    EXPECT_EQ(3, stats.Back().RowCount);
    EXPECT_EQ(0, stats.LowerBound(-1, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.UpperBound(-1, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.LowerBound(0, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.LowerBound(10, &TCumulativeStatisticsEntry::ChunkCount));
    EXPECT_EQ(1, stats.UpperBound(10, &TCumulativeStatisticsEntry::ChunkCount));
    EXPECT_EQ(1, stats.LowerBound(300, &TCumulativeStatisticsEntry::DataSize));
    EXPECT_EQ(2, stats.UpperBound(300, &TCumulativeStatisticsEntry::DataSize));

    stats.EraseFromFront(1);
    EXPECT_EQ(3, stats.Back().RowCount);
}

TEST(CumulativeStatistics, SimpleModifiable)
{
    TCumulativeStatistics stats;
    stats.DeclareModifiable();

    EXPECT_EQ(stats.Size(), 0);

    stats.PushBack({1, 10, 100});
    stats.PushBack({2, 20, 200});

    EXPECT_EQ(3, stats.Back().RowCount);
    EXPECT_EQ(0, stats.LowerBound(-1, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.UpperBound(-1, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.LowerBound(0, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(1, stats.LowerBound(2, &TCumulativeStatisticsEntry::RowCount));
    EXPECT_EQ(0, stats.LowerBound(10, &TCumulativeStatisticsEntry::ChunkCount));
    EXPECT_EQ(1, stats.UpperBound(10, &TCumulativeStatisticsEntry::ChunkCount));
    EXPECT_EQ(1, stats.LowerBound(300, &TCumulativeStatisticsEntry::DataSize));
    EXPECT_EQ(2, stats.UpperBound(300, &TCumulativeStatisticsEntry::DataSize));

    stats.Update(0, {1, 0, 0});
    EXPECT_EQ(2, stats[0].RowCount);
    EXPECT_EQ(4, stats[1].RowCount);
    EXPECT_EQ(0, stats.LowerBound(2, &TCumulativeStatisticsEntry::RowCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
