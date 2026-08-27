#include <yt/yt/server/lib/hydra/mutation_committer.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NHydra {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TComputeQuorumSequenceNumberTest, EmptyInput)
{
    ASSERT_EQ(ComputeQuorumSequenceNumber({}, 1), -1);
}

TEST(TComputeQuorumSequenceNumberTest, AllWeightsOneMatchesCountBasedMajority)
{
    std::vector<std::pair<i64, int>> loggedNumbersAndWeights{
        {10, 1}, {9, 1}, {8, 1}, {3, 1}, {1, 1},
    };
    ASSERT_EQ(ComputeQuorumSequenceNumber(loggedNumbersAndWeights, 3), 8);
}

TEST(TComputeQuorumSequenceNumberTest, WeightedExampleBelowQuorum)
{
    std::vector<std::pair<i64, int>> loggedNumbersAndWeights{
        {-1, 1}, {-1, 2}, {100, 3}, {100, 4}, {-1, 5},
    };
    ASSERT_EQ(ComputeQuorumSequenceNumber(loggedNumbersAndWeights, 8), -1);
}

TEST(TComputeQuorumSequenceNumberTest, WeightedExampleReachesQuorum)
{
    std::vector<std::pair<i64, int>> loggedNumbersAndWeights{
        {100, 1}, {-1, 2}, {100, 3}, {100, 4}, {-1, 5},
    };
    ASSERT_EQ(ComputeQuorumSequenceNumber(loggedNumbersAndWeights, 8), 100);
}

TEST(TComputeQuorumSequenceNumberTest, LaggingPeerStillContributesWeightToLowerBucket)
{
    std::vector<std::pair<i64, int>> loggedNumbersAndWeights{
        {-1, 1}, {-1, 2}, {-1, 3}, {10, 4}, {20, 5},
    };
    ASSERT_EQ(ComputeQuorumSequenceNumber(loggedNumbersAndWeights, 8), 10);
}

// Doesn't happen in real life but why not.
TEST(TComputeQuorumSequenceNumberTest, QuorumWeightExceedsTotalWeightReturnsMinusOne)
{
    std::vector<std::pair<i64, int>> loggedNumbersAndWeights{
        {5, 1}, {5, 2},
    };
    ASSERT_EQ(ComputeQuorumSequenceNumber(loggedNumbersAndWeights, 100), -1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
