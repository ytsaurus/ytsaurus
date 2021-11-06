#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

//! Describes a path from root to final partition leaf.
using TPartition = std::vector<int>;
using TPartitions = std::vector<TPartition>;

void ListPartitions(TPartitionTreeSkeleton* tree, TPartitions* partitions, std::vector<int>* currentPath)
{
    if (tree->Children.empty()) {
        partitions->push_back(*currentPath);
    } else {
        for (int childIndex = 0; childIndex < std::ssize(tree->Children); ++childIndex) {
            auto* child = tree->Children[childIndex].get();
            currentPath->push_back(childIndex);
            ListPartitions(child, partitions, currentPath);
            currentPath->pop_back();
        }
    }
}

TPartitions BuildTreeAndListPartitions(int partitionCount, int maxPartitionFactor)
{
    auto partitionTreeSkeleton = BuildPartitionTreeSkeleton(partitionCount, maxPartitionFactor);
    TPartitions partitions;
    std::vector<int> currentPath;
    ListPartitions(partitionTreeSkeleton.get(), &partitions, &currentPath);

    return partitions;
}

TEST(TPartitionsTest, TestPartitionTreeSkeleton)
{
    EXPECT_EQ(BuildTreeAndListPartitions(1, 1), TPartitions({{0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(1, 123), TPartitions({{0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(2, 1), TPartitions({{0}, {1}}));
    EXPECT_EQ(BuildTreeAndListPartitions(2, 2), TPartitions({{0}, {1}}));
    EXPECT_EQ(BuildTreeAndListPartitions(2, 123), TPartitions({{0}, {1}}));
    EXPECT_EQ(BuildTreeAndListPartitions(3, 1), TPartitions({{0, 0}, {0, 1}, {1, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(3, 2), TPartitions({{0, 0}, {0, 1}, {1, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(3, 3), TPartitions({{0}, {1}, {2}}));
    EXPECT_EQ(BuildTreeAndListPartitions(3, 123), TPartitions({{0}, {1}, {2}}));
    EXPECT_EQ(BuildTreeAndListPartitions(4, 1), TPartitions({{0, 0}, {0, 1}, {1, 0}, {1, 1}}));
    EXPECT_EQ(BuildTreeAndListPartitions(4, 2), TPartitions({{0, 0}, {0, 1}, {1, 0}, {1, 1}}));
    EXPECT_EQ(BuildTreeAndListPartitions(4, 3), TPartitions({{0, 0}, {0, 1}, {1, 0}, {2, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(4, 4), TPartitions({{0}, {1}, {2}, {3}}));
    EXPECT_EQ(BuildTreeAndListPartitions(4, 123), TPartitions({{0}, {1}, {2}, {3}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 1), TPartitions({{0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0}, {1, 1, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 2), TPartitions({{0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0}, {1, 1, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 3), TPartitions({{0, 0}, {0, 1}, {1, 0}, {1, 1}, {2, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 4), TPartitions({{0, 0}, {0, 1}, {1, 0}, {2, 0}, {3, 0}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 5), TPartitions({{0}, {1}, {2}, {3}, {4}}));
    EXPECT_EQ(BuildTreeAndListPartitions(5, 123), TPartitions({{0}, {1}, {2}, {3}, {4}}));

    {
        int partitionCount = (1 << 16);
        auto partitions = BuildTreeAndListPartitions(partitionCount, 2);
        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const auto& partition = partitions[partitionIndex];
            EXPECT_EQ(partition.size(), 16u);
            for (int layerIndex = 0; layerIndex < 16; ++layerIndex) {
                EXPECT_EQ(partition[layerIndex], std::min(1, partitionIndex & (1 << (15 - layerIndex))));
            }
        }
    }

    {
        int partitionCount = (1 << 16);
        auto partitions = BuildTreeAndListPartitions(partitionCount, 1 << 8);
        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const auto& partition = partitions[partitionIndex];
            EXPECT_EQ(partition, std::vector<int>({partitionIndex / 256, partitionIndex % 256}));
        }
    }

    {
        int partitionCount = 1234;
        auto partitions = BuildTreeAndListPartitions(partitionCount, 45);
        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const auto& partition = partitions[partitionIndex];
            EXPECT_EQ(partition.size(), 2u);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
