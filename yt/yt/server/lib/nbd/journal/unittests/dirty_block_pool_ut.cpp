#include <yt/yt/server/lib/nbd/journal/dirty_block_pool.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/memory/range.h>
#include <library/cpp/yt/memory/ref.h>

#include <algorithm>

namespace NYT::NNbd::NJournal {
namespace {

////////////////////////////////////////////////////////////////////////////////

TDirtyBlockPtr MakeBlock(int blockIndex)
{
    return New<TDirtyBlock>(blockIndex, TSharedRef::MakeEmpty());
}

std::vector<TDirtyBlockPtr> MakeBlocks(const std::vector<int>& blockIndexes)
{
    std::vector<TDirtyBlockPtr> blocks;
    blocks.reserve(blockIndexes.size());
    for (int blockIndex : blockIndexes) {
        blocks.push_back(MakeBlock(blockIndex));
    }
    return blocks;
}

std::vector<TDirtyBlockId> PutSync(const IDirtyBlockPoolPtr& pool, TRange<TDirtyBlockPtr> blocks)
{
    auto future = pool->Put(blocks);
    EXPECT_TRUE(future.IsSet());
    return future.GetOrCrash().ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDirtyBlockPoolTest, PutAndFind)
{
    auto pool = CreateDirtyBlockPool(4);
    EXPECT_EQ(pool->GetCapacity(), 4);
    EXPECT_EQ(pool->GetSize(), 0);

    auto blocks = MakeBlocks({10, 20, 30});
    auto blockIds = PutSync(pool, blocks);
    ASSERT_EQ(std::ssize(blockIds), 3);
    EXPECT_EQ(pool->GetSize(), 3);

    for (int index = 0; index < 3; ++index) {
        auto block = pool->Find(blockIds[index], blocks[index]->BlockIndex);
        ASSERT_TRUE(block);
        // The very same block object is returned.
        EXPECT_EQ(block.Get(), blocks[index].Get());
    }
}

TEST(TDirtyBlockPoolTest, FindValidatesBlockIndex)
{
    auto pool = CreateDirtyBlockPool(4);
    auto blocks = MakeBlocks({42});
    auto blockIds = PutSync(pool, blocks);

    EXPECT_TRUE(pool->Find(blockIds[0], 42));
    // Wrong block index -> miss.
    EXPECT_FALSE(pool->Find(blockIds[0], 43));
}

TEST(TDirtyBlockPoolTest, EmptyPut)
{
    auto pool = CreateDirtyBlockPool(2);
    auto blockIds = PutSync(pool, {});
    EXPECT_TRUE(blockIds.empty());
    EXPECT_EQ(pool->GetSize(), 0);
}

TEST(TDirtyBlockPoolTest, PartialPut)
{
    auto pool = CreateDirtyBlockPool(2);
    PutSync(pool, MakeBlocks({1}));

    // Only one slot is free, so only a one-block prefix is accepted.
    auto blockIds = PutSync(pool, MakeBlocks({2, 3, 4}));
    EXPECT_EQ(std::ssize(blockIds), 1);
    EXPECT_EQ(pool->GetSize(), 2);
}

TEST(TDirtyBlockPoolTest, PutWaitsWhenFullAndDrainUnblocks)
{
    auto pool = CreateDirtyBlockPool(2);
    auto firstIds = PutSync(pool, MakeBlocks({1, 2}));
    ASSERT_EQ(std::ssize(firstIds), 2);

    // The pool is full: the put must wait.
    auto blocks = MakeBlocks({3});
    auto future = pool->Put(blocks);
    EXPECT_FALSE(future.IsSet());

    auto drainResult = pool->BeginDrain(1);
    ASSERT_EQ(std::ssize(drainResult), 1);
    // BeginDrain does not free space yet, so the put is still blocked.
    EXPECT_FALSE(future.IsSet());

    pool->EndDrain(drainResult);
    ASSERT_TRUE(future.IsSet());
    auto secondIds = future.GetOrCrash().ValueOrThrow();
    EXPECT_EQ(std::ssize(secondIds), 1);
    EXPECT_EQ(pool->GetSize(), 2);
}

TEST(TDirtyBlockPoolTest, WaitingPutResolvedPartially)
{
    auto pool = CreateDirtyBlockPool(2);
    PutSync(pool, MakeBlocks({1, 2}));

    // Waits, since the pool is full.
    auto blocks = MakeBlocks({3, 4, 5});
    auto future = pool->Put(blocks);
    EXPECT_FALSE(future.IsSet());

    // Freeing a single slot lets the waiter accept a one-block prefix.
    auto drainResult = pool->BeginDrain(1);
    pool->EndDrain(drainResult);

    ASSERT_TRUE(future.IsSet());
    EXPECT_EQ(std::ssize(future.GetOrCrash().ValueOrThrow()), 1);
}

TEST(TDirtyBlockPoolTest, BeginDrainReturnsBlocksAndIsCapped)
{
    auto pool = CreateDirtyBlockPool(4);
    PutSync(pool, MakeBlocks({1, 2, 3}));

    auto drainResult = pool->BeginDrain(10);
    ASSERT_EQ(std::ssize(drainResult), 3);

    // The pool is unordered, so only assert on the returned set. Each drained block carries the
    // id it is stored under, so it is findable under that id.
    std::vector<int> drainedIndexes;
    for (const auto& block : drainResult) {
        drainedIndexes.push_back(block->BlockIndex);
        EXPECT_EQ(pool->Find(block->BlockId, block->BlockIndex), block);
    }
    std::sort(drainedIndexes.begin(), drainedIndexes.end());
    EXPECT_EQ(drainedIndexes, (std::vector<int>{1, 2, 3}));

    // BeginDrain does not remove anything on its own.
    EXPECT_EQ(pool->GetSize(), 3);
}

TEST(TDirtyBlockPoolTest, PartialDrainFreesOneBlock)
{
    auto pool = CreateDirtyBlockPool(2);
    auto blockIds = PutSync(pool, MakeBlocks({1, 2}));

    pool->EndDrain(pool->BeginDrain(1));
    EXPECT_EQ(pool->GetSize(), 1);

    // Exactly one of the two blocks remains (which one is unspecified).
    int findableCount =
        (pool->Find(blockIds[0], 1) ? 1 : 0) +
        (pool->Find(blockIds[1], 2) ? 1 : 0);
    EXPECT_EQ(findableCount, 1);
}

TEST(TDirtyBlockPoolTest, DrainedBlocksBecomeUnfindable)
{
    auto pool = CreateDirtyBlockPool(2);
    auto blockIds = PutSync(pool, MakeBlocks({1, 2}));

    auto drainResult = pool->BeginDrain(2);
    // Still findable before EndDrain.
    EXPECT_TRUE(pool->Find(blockIds[0], 1));
    EXPECT_TRUE(pool->Find(blockIds[1], 2));

    pool->EndDrain(drainResult);
    EXPECT_EQ(pool->GetSize(), 0);
    EXPECT_FALSE(pool->Find(blockIds[0], 1));
    EXPECT_FALSE(pool->Find(blockIds[1], 2));
}

TEST(TDirtyBlockPoolTest, WrapsAroundTheRing)
{
    auto pool = CreateDirtyBlockPool(2);

    // Cycle blocks through the ring several times, draining each before the next put.
    for (int round = 0; round < 5; ++round) {
        auto blocks = MakeBlocks({round});
        auto blockIds = PutSync(pool, blocks);
        ASSERT_EQ(std::ssize(blockIds), 1);

        auto found = pool->Find(blockIds[0], round);
        ASSERT_TRUE(found);
        EXPECT_EQ(found.Get(), blocks[0].Get());

        pool->EndDrain(pool->BeginDrain(1));
        EXPECT_FALSE(pool->Find(blockIds[0], round));
    }
    EXPECT_EQ(pool->GetSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
