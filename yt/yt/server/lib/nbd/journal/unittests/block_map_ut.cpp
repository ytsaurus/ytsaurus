#include <yt/yt/server/lib/nbd/journal/block_map.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NNbd::NJournal {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockMapTest, InitiallyEmpty)
{
    auto blockMap = CreateBlockMap(4);
    for (int blockIndex = 0; blockIndex < 4; ++blockIndex) {
        EXPECT_TRUE(std::holds_alternative<TEmptyBlock>(blockMap->Find(blockIndex)));
    }
}

TEST(TBlockMapTest, PutDirtyThenFind)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutDirty(1, TDirtyBlockId(123));

    auto state = blockMap->Find(1);
    ASSERT_TRUE(std::holds_alternative<TDirtyBlockId>(state));
    EXPECT_EQ(std::get<TDirtyBlockId>(state), TDirtyBlockId(123));

    // Other blocks are untouched.
    EXPECT_TRUE(std::holds_alternative<TEmptyBlock>(blockMap->Find(0)));
    EXPECT_TRUE(std::holds_alternative<TEmptyBlock>(blockMap->Find(2)));
}

TEST(TBlockMapTest, TryMakeCleanThenFind)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutDirty(2, TDirtyBlockId(123));
    EXPECT_TRUE(blockMap->TryMakeClean(2, TDirtyBlockId(123), TStoredBlockId(456)));

    auto state = blockMap->Find(2);
    ASSERT_TRUE(std::holds_alternative<TStoredBlockId>(state));
    EXPECT_EQ(std::get<TStoredBlockId>(state), TStoredBlockId(456));
}

TEST(TBlockMapTest, TryMakeCleanFailsWhenSuperseded)
{
    auto blockMap = CreateBlockMap(1);

    // A newer write replaced the drained dirty id, so the clean transition must be rejected
    // and the newer dirty state must survive.
    blockMap->PutDirty(0, TDirtyBlockId(1));
    blockMap->PutDirty(0, TDirtyBlockId(2));
    EXPECT_FALSE(blockMap->TryMakeClean(0, TDirtyBlockId(1), TStoredBlockId(99)));
    EXPECT_EQ(std::get<TDirtyBlockId>(blockMap->Find(0)), TDirtyBlockId(2));

    // Matching the current dirty id succeeds.
    EXPECT_TRUE(blockMap->TryMakeClean(0, TDirtyBlockId(2), TStoredBlockId(99)));
    EXPECT_EQ(std::get<TStoredBlockId>(blockMap->Find(0)), TStoredBlockId(99));

    // A clean block is not dirty under any id, so a further clean transition is rejected.
    EXPECT_FALSE(blockMap->TryMakeClean(0, TDirtyBlockId(2), TStoredBlockId(100)));
}

TEST(TBlockMapTest, StateTransitions)
{
    auto blockMap = CreateBlockMap(1);

    // Empty -> dirty -> clean -> dirty, each overwriting the previous state.
    EXPECT_TRUE(std::holds_alternative<TEmptyBlock>(blockMap->Find(0)));

    blockMap->PutDirty(0, TDirtyBlockId(7));
    EXPECT_EQ(std::get<TDirtyBlockId>(blockMap->Find(0)), TDirtyBlockId(7));

    EXPECT_TRUE(blockMap->TryMakeClean(0, TDirtyBlockId(7), TStoredBlockId(8)));
    EXPECT_EQ(std::get<TStoredBlockId>(blockMap->Find(0)), TStoredBlockId(8));

    blockMap->PutDirty(0, TDirtyBlockId(9));
    EXPECT_EQ(std::get<TDirtyBlockId>(blockMap->Find(0)), TDirtyBlockId(9));
}

TEST(TBlockMapTest, GetUsedBlockCount)
{
    auto blockMap = CreateBlockMap(4);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);

    // The first write to a block makes it used.
    blockMap->PutDirty(1, TDirtyBlockId(1));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);

    // Writing a distinct block bumps the count again.
    blockMap->PutDirty(3, TDirtyBlockId(2));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);

    // Rewriting an already-used block does not.
    blockMap->PutDirty(1, TDirtyBlockId(3));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);

    // Neither does flushing it clean, nor a subsequent rewrite.
    EXPECT_TRUE(blockMap->TryMakeClean(1, TDirtyBlockId(3), TStoredBlockId(4)));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
    blockMap->PutDirty(1, TDirtyBlockId(5));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
