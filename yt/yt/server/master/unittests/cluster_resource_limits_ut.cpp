#include <yt/yt/server/master/security_server/cluster_resource_limits.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NSecurityServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TClusterResourceLimitsTest, ExplicitZeroWithInfiniteDiskSpaceDefault)
{
    auto limits = TClusterResourceLimits::Infinity();
    limits.SetMediumDiskSpace(0, TLimit64(0));
    EXPECT_EQ(TLimit64(0), limits.DiskSpace().GetOrDefault(0));
    EXPECT_TRUE(limits.DiskSpace().GetOrDefault(1).IsInfinite());

    limits.AddToMediumDiskSpace(0, 10);
    limits.AddToMediumDiskSpace(0, -10);
    EXPECT_EQ(TLimit64(0), limits.DiskSpace().GetOrDefault(0));

    limits.SetMediumDiskSpace(0, TLimit64::Infinity());
    EXPECT_TRUE(limits.DiskSpace().empty());
}

TEST(TClusterResourceLimitsTest, SumPreservesInfiniteDefaultsAndExplicitZeros)
{
    auto left = TClusterResourceLimits::Infinity();
    left.SetMediumDiskSpace(0, TLimit64(0));
    left.SetMediumDiskSpace(1, TLimit64(10));
    auto right = TClusterResourceLimits::Infinity();
    right.SetMediumDiskSpace(0, TLimit64(0));
    right.SetMediumDiskSpace(1, TLimit64(20));

    auto sum = left;
    sum.IncreaseWithInfinityAllowed(right);
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(30), sum.DiskSpace().GetOrDefault(1));
    EXPECT_TRUE(sum.DiskSpace().GetOrDefault(2).IsInfinite());
    EXPECT_TRUE(sum.GetNodeCount().IsInfinite());
    EXPECT_TRUE(sum.MasterMemory().PerCell.GetOrDefault(NObjectClient::TCellTag(10)).IsInfinite());

    left.IncreaseWithInfinityAllowed(left);
    EXPECT_EQ(TLimit64(0), left.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(20), left.DiskSpace().GetOrDefault(1));
    EXPECT_TRUE(left.DiskSpace().DefaultValue().IsInfinite());
}

TEST(TClusterResourceLimitsTest, SumHandlesOneSidedDiskSpaceEntriesInEitherOrder)
{
    auto left = TClusterResourceLimits::Infinity();
    // Exceed the compact map's inline capacity.
    for (int mediumIndex = 0; mediumIndex < 8; ++mediumIndex) {
        left.SetMediumDiskSpace(mediumIndex, TLimit64(10));
    }
    left.SetMediumDiskSpace(1, TLimit64(0));

    auto right = TClusterResourceLimits::Infinity();
    right.SetMediumDiskSpace(0, TLimit64(20));
    right.SetMediumDiskSpace(1, TLimit64(0));
    right.SetMediumDiskSpace(8, TLimit64(50));

    auto sum = left;
    sum.IncreaseWithInfinityAllowed(right);
    auto reverseSum = right;
    reverseSum.IncreaseWithInfinityAllowed(left);
    EXPECT_EQ(sum, reverseSum);
    EXPECT_EQ(2u, sum.DiskSpace().size());
    EXPECT_EQ(TLimit64(30), sum.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(1));
    for (int mediumIndex = 2; mediumIndex <= 8; ++mediumIndex) {
        EXPECT_TRUE(sum.DiskSpace().GetOrDefault(mediumIndex).IsInfinite());
    }
}

TEST(TClusterResourceLimitsTest, SumCombinesDiskSpaceDefaults)
{
    for (bool leftInfinite : {false, true}) {
        for (bool rightInfinite : {false, true}) {
            auto sum = leftInfinite ? TClusterResourceLimits::Infinity() : TClusterResourceLimits::Zero();
            auto right = rightInfinite ? TClusterResourceLimits::Infinity() : TClusterResourceLimits::Zero();
            sum.IncreaseWithInfinityAllowed(right);

            EXPECT_TRUE(sum.DiskSpace().empty());
            EXPECT_EQ(leftInfinite || rightInfinite, sum.DiskSpace().DefaultValue().IsInfinite());
            if (!leftInfinite && !rightInfinite) {
                EXPECT_EQ(TLimit64(0), sum.DiskSpace().DefaultValue());
            }
        }
    }
}

TEST(TClusterResourceLimitsTest, SumWithZeroPreservesExplicitZeroWithInfiniteDiskSpaceDefault)
{
    auto limits = TClusterResourceLimits::Infinity();
    limits.SetMediumDiskSpace(0, TLimit64(0));

    auto sum = TClusterResourceLimits::Zero();
    sum.IncreaseWithInfinityAllowed(limits);
    auto reverseSum = limits;
    reverseSum.IncreaseWithInfinityAllowed(TClusterResourceLimits::Zero());

    EXPECT_EQ(sum.DiskSpace(), reverseSum.DiskSpace());
    EXPECT_TRUE(sum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_TRUE(reverseSum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_EQ(1u, sum.DiskSpace().size());
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(0));
    EXPECT_TRUE(sum.DiskSpace().GetOrDefault(1).IsInfinite());
}

TEST(TClusterResourceLimitsTest, SumHandlesMixedDiskSpaceDefaultsInEitherOrder)
{
    auto left = TClusterResourceLimits::Zero();
    left.SetMediumDiskSpace(0, TLimit64(10));
    left.SetMediumDiskSpace(1, TLimit64(40));
    auto right = TClusterResourceLimits::Infinity();
    right.SetMediumDiskSpace(0, TLimit64(20));
    right.SetMediumDiskSpace(2, TLimit64(50));
    right.SetMediumDiskSpace(3, TLimit64(0));
    auto originalLeft = left;
    auto originalRight = right;

    auto sum = left;
    sum.IncreaseWithInfinityAllowed(right);
    auto reverseSum = right;
    reverseSum.IncreaseWithInfinityAllowed(left);

    EXPECT_EQ(sum.DiskSpace(), reverseSum.DiskSpace());
    EXPECT_TRUE(sum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_TRUE(reverseSum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_EQ(3u, sum.DiskSpace().size());
    EXPECT_EQ(TLimit64(30), sum.DiskSpace().GetOrDefault(0));
    EXPECT_FALSE(sum.DiskSpace().contains(1));
    EXPECT_TRUE(sum.DiskSpace().GetOrDefault(1).IsInfinite());
    EXPECT_EQ(TLimit64(50), sum.DiskSpace().GetOrDefault(2));
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(3));
    EXPECT_EQ(originalLeft, left);
    EXPECT_EQ(originalRight, right);
    EXPECT_EQ(TLimit64(0), left.DiskSpace().DefaultValue());
    EXPECT_TRUE(right.DiskSpace().DefaultValue().IsInfinite());
}

TEST(TClusterResourceLimitsTest, MasterMemorySumUsesOnlyExplicitPerCellLimits)
{
    for (auto defaultValue : {TLimit64(0), TLimit64::Infinity()}) {
        TMasterMemoryLimits left{
            TLimit64(10),
            TLimit64(20),
            TMasterMemoryLimits::TPerCellLimits(defaultValue),
        };
        left.PerCell[NObjectClient::TCellTag(10)] = TLimit64(10);
        left.PerCell[NObjectClient::TCellTag(11)] = TLimit64(100);
        left.PerCell[NObjectClient::TCellTag(13)] = TLimit64(0);
        left.PerCell[NObjectClient::TCellTag(15)] = TLimit64::Infinity();

        TMasterMemoryLimits right{TLimit64(30), TLimit64::Infinity()};
        right.PerCell[NObjectClient::TCellTag(10)] = TLimit64(20);
        right.PerCell[NObjectClient::TCellTag(12)] = TLimit64(200);
        right.PerCell[NObjectClient::TCellTag(13)] = TLimit64(0);
        right.PerCell[NObjectClient::TCellTag(15)] = TLimit64(5);

        auto sum = TMasterMemoryLimits::Zero();
        sum.IncreaseWithInfinityAllowed(left);
        sum.IncreaseWithInfinityAllowed(right);
        auto reverseSum = TMasterMemoryLimits::Zero();
        reverseSum.IncreaseWithInfinityAllowed(right);
        reverseSum.IncreaseWithInfinityAllowed(left);

        EXPECT_EQ(sum, reverseSum);
        EXPECT_EQ(TLimit64(40), sum.Total);
        EXPECT_TRUE(sum.ChunkHost.IsInfinite());
        EXPECT_EQ(TLimit64(0), sum.PerCell.DefaultValue());
        EXPECT_EQ(TLimit64(0), reverseSum.PerCell.DefaultValue());
        EXPECT_EQ(4u, sum.PerCell.size());
        EXPECT_EQ(TLimit64(30), sum.PerCell.GetOrDefault(NObjectClient::TCellTag(10)));
        EXPECT_EQ(TLimit64(100), sum.PerCell.GetOrDefault(NObjectClient::TCellTag(11)));
        EXPECT_EQ(TLimit64(200), sum.PerCell.GetOrDefault(NObjectClient::TCellTag(12)));
        EXPECT_FALSE(sum.PerCell.contains(NObjectClient::TCellTag(13)));
        EXPECT_EQ(TLimit64(0), sum.PerCell.GetOrDefault(NObjectClient::TCellTag(14)));
        EXPECT_TRUE(sum.PerCell.GetOrDefault(NObjectClient::TCellTag(15)).IsInfinite());
    }
}

TEST(TClusterResourceLimitsTest, SumPreservesZeroDiskSpaceDefault)
{
    auto left = TClusterResourceLimits::Zero().SetNodeCount(TLimit64(10));
    left.SetMediumDiskSpace(0, TLimit64(10));
    left.SetMediumDiskSpace(1, TLimit64(5));
    auto right = TClusterResourceLimits::Zero().SetNodeCount(TLimit64(20));
    right.SetMediumDiskSpace(0, TLimit64(20));
    right.SetMediumDiskSpace(2, TLimit64(7));

    auto sum = left;
    sum.IncreaseWithInfinityAllowed(right);
    EXPECT_EQ(TLimit64(30), sum.GetNodeCount());
    EXPECT_EQ(TLimit64(30), sum.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(5), sum.DiskSpace().GetOrDefault(1));
    EXPECT_EQ(TLimit64(7), sum.DiskSpace().GetOrDefault(2));
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(3));
}

TEST(TClusterResourceLimitsTest, TransferPreservesUnspecifiedResourceLimits)
{
    auto limits = TClusterResourceLimits::Infinity().SetNodeCount(TLimit64(10));
    limits.SetMediumDiskSpace(0, TLimit64(10));
    limits.SetMediumDiskSpace(1, TLimit64(20));
    limits.MasterMemory().PerCell[NObjectClient::TCellTag(10)] = TLimit64(10);
    limits.MasterMemory().PerCell[NObjectClient::TCellTag(11)] = TLimit64(20);
    auto originalLimits = limits;
    auto delta = TClusterResourceLimits::Zero().SetNodeCount(TLimit64(5));
    delta.SetMediumDiskSpace(0, TLimit64(5));
    delta.MasterMemory().PerCell[NObjectClient::TCellTag(10)] = TLimit64(5);
    delta.MasterMemory().PerCell[NObjectClient::TCellTag(12)] = TLimit64(0);
    ASSERT_FALSE(limits.CheckIncrease(delta));
    limits.Increase(delta);
    EXPECT_EQ(TLimit64(15), limits.GetNodeCount());
    EXPECT_TRUE(limits.GetChunkCount().IsInfinite());
    EXPECT_TRUE(limits.GetTabletCount().IsInfinite());
    EXPECT_TRUE(limits.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_EQ(TLimit64(15), limits.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(20), limits.DiskSpace().GetOrDefault(1));
    EXPECT_TRUE(limits.MasterMemory().Total.IsInfinite());
    EXPECT_TRUE(limits.MasterMemory().ChunkHost.IsInfinite());
    EXPECT_EQ(TLimit64(15), limits.MasterMemory().PerCell.GetOrDefault(NObjectClient::TCellTag(10)));
    EXPECT_EQ(TLimit64(20), limits.MasterMemory().PerCell.GetOrDefault(NObjectClient::TCellTag(11)));
    EXPECT_FALSE(limits.MasterMemory().PerCell.contains(NObjectClient::TCellTag(12)));

    ASSERT_FALSE(limits.CheckDecrease(delta));
    limits.Decrease(delta);
    EXPECT_EQ(originalLimits, limits);
    EXPECT_TRUE(limits.DiskSpace().DefaultValue().IsInfinite());
}

TEST(TClusterResourceLimitsTest, TransferToZeroPreservesUnspecifiedMedia)
{
    auto limits = TClusterResourceLimits::Infinity();
    limits.SetMediumDiskSpace(0, TLimit64(10));
    auto delta = TClusterResourceLimits::Zero();
    delta.SetMediumDiskSpace(0, TLimit64(10));

    ASSERT_FALSE(limits.CheckDecrease(delta));
    limits.Decrease(delta);
    EXPECT_EQ(TLimit64(0), limits.DiskSpace().GetOrDefault(0));
    EXPECT_TRUE(limits.DiskSpace().GetOrDefault(1).IsInfinite());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSecurityServer
