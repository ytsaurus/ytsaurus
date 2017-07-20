#include <yt/server/hydra/snapshot_quota_helpers.h>

#include <yt/core/test_framework/framework.h>

#include <limits>
#include <vector>

namespace NYT {
namespace NHydra {

TEST(TThresholdMixed, EmptyVector)
{
    ASSERT_FALSE(GetSnapshotThresholdId({}, 1, 1));
}

TEST(TThresholdCount, ShortVector)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11}
    };
    ASSERT_FALSE(GetSnapshotThresholdId(test, 3, Null));
}

TEST(TThresholdCount, LongVector)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11},
        {9, 15}
    };
    ASSERT_EQ(1, GetSnapshotThresholdId(test, 2, Null));
}

TEST(TThresholdSize, SmallSize)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 3},
        {3, 7}
    };
    ASSERT_FALSE(GetSnapshotThresholdId(test, Null, 30));
}

TEST(TThresholdSize, BigSize)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 4},
        {3, 15},
        {4, 7}
    };
    ASSERT_EQ(2, GetSnapshotThresholdId(test, Null, 25));
}

TEST(TThresholdSize, AccurateSize)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(1, GetSnapshotThresholdId(test, Null, 38));
}

TEST(TThresholdMixed, CountStronger)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(3, GetSnapshotThresholdId(test, 1, 30));
}

TEST(TThresholdMixed, SizeStronger)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(3, GetSnapshotThresholdId(test, 3, 15));
}

TEST(TThresholdMixed, Zeros)
{
    std::vector<TSnapshotInfo> test{
        {1, 10},
        {2, 11},
        {3, 13}
    };
    ASSERT_EQ(3, GetSnapshotThresholdId(test, 0, 0));
}
} // namespace NHydra
} // namespace NYT
