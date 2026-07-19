#include <yt/yt/server/lib/nbd/journal/block_map.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NNbd::NJournal {
namespace {

////////////////////////////////////////////////////////////////////////////////

TMappedBlockId MakeStored(ui64 blockId)
{
    return ToMappedBlockId(TStoredBlockId(blockId));
}

TMappedBlockId MakeDirty(ui64 blockId)
{
    return ToMappedBlockId(TDirtyBlockId(blockId));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockMapTest, InitiallyEmpty)
{
    auto blockMap = CreateBlockMap(4);
    for (int blockIndex = 0; blockIndex < 4; ++blockIndex) {
        EXPECT_EQ(blockMap->FindBlock(blockIndex), EmptyMappedBlockId);
    }
}

TEST(TBlockMapTest, PutDirtyThenFind)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutDirty(1, TDirtyBlockId(123));

    EXPECT_EQ(blockMap->FindBlock(1), MakeDirty(123));

    // Other blocks are untouched.
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->FindBlock(2), EmptyMappedBlockId);
}

TEST(TBlockMapTest, TryMakeCleanThenFind)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutDirty(2, TDirtyBlockId(123));
    EXPECT_TRUE(blockMap->TryMakeClean(2, TDirtyBlockId(123), TStoredBlockId(456)));

    EXPECT_EQ(blockMap->FindBlock(2), MakeStored(456));
}

TEST(TBlockMapTest, TryMakeCleanFailsWhenSuperseded)
{
    auto blockMap = CreateBlockMap(1);

    // A newer write replaced the drained dirty id, so the clean transition must be rejected
    // and the newer dirty mapping must survive.
    blockMap->PutDirty(0, TDirtyBlockId(1));
    blockMap->PutDirty(0, TDirtyBlockId(2));
    EXPECT_FALSE(blockMap->TryMakeClean(0, TDirtyBlockId(1), TStoredBlockId(99)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(2));

    // Matching the current dirty id succeeds.
    EXPECT_TRUE(blockMap->TryMakeClean(0, TDirtyBlockId(2), TStoredBlockId(99)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeStored(99));

    // A clean block is not dirty under any id, so a further clean transition is rejected.
    EXPECT_FALSE(blockMap->TryMakeClean(0, TDirtyBlockId(2), TStoredBlockId(100)));
}

TEST(TBlockMapTest, TagTransitions)
{
    auto blockMap = CreateBlockMap(1);

    // Empty -> dirty -> clean -> dirty, each overwriting the previous mapping.
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);

    blockMap->PutDirty(0, TDirtyBlockId(7));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(7));

    EXPECT_TRUE(blockMap->TryMakeClean(0, TDirtyBlockId(7), TStoredBlockId(8)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeStored(8));

    blockMap->PutDirty(0, TDirtyBlockId(9));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(9));
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

TEST(TBlockMapTest, TakeSnapshot)
{
    auto blockMap = CreateBlockMap(8);

    // Block 3 clean; blocks 1 and 5 dirty; the rest empty (omitted).
    blockMap->PutDirty(1, TDirtyBlockId(11));
    blockMap->PutDirty(3, TDirtyBlockId(33));
    EXPECT_TRUE(blockMap->TryMakeClean(3, TDirtyBlockId(33), TStoredBlockId(333)));
    blockMap->PutDirty(5, TDirtyBlockId(55));

    auto snapshot = blockMap->TakeSnapshot();

    // Reported by ascending block index: dirty 1, clean 3, dirty 5.
    ASSERT_EQ(std::ssize(snapshot.Blocks), 3);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(1, MakeDirty(11)));
    EXPECT_EQ(snapshot.Blocks[1], std::pair(3, MakeStored(333)));
    EXPECT_EQ(snapshot.Blocks[2], std::pair(5, MakeDirty(55)));
}

TEST(TBlockMapTest, LoadSnapshot)
{
    auto blockMap = CreateBlockMap(8);

    TBlockMapSnapshot snapshot;
    snapshot.Blocks.emplace_back(2, MakeStored(22));
    snapshot.Blocks.emplace_back(6, MakeStored(66));
    blockMap->LoadSnapshot(snapshot);

    EXPECT_EQ(blockMap->FindBlock(2), MakeStored(22));
    EXPECT_EQ(blockMap->FindBlock(6), MakeStored(66));
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
}

TEST(TBlockMapTest, SnapshotThenLoadRoundtrip)
{
    auto blockMap = CreateBlockMap(8);

    // A snapshot fit for LoadSnapshot holds only stored (clean) blocks.
    blockMap->PutDirty(1, TDirtyBlockId(11));
    EXPECT_TRUE(blockMap->TryMakeClean(1, TDirtyBlockId(11), TStoredBlockId(111)));
    blockMap->PutDirty(4, TDirtyBlockId(44));
    EXPECT_TRUE(blockMap->TryMakeClean(4, TDirtyBlockId(44), TStoredBlockId(444)));

    auto snapshot = blockMap->TakeSnapshot();

    auto restored = CreateBlockMap(8);
    restored->LoadSnapshot(snapshot);

    EXPECT_EQ(restored->FindBlock(1), MakeStored(111));
    EXPECT_EQ(restored->FindBlock(4), MakeStored(444));
    EXPECT_EQ(restored->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(restored->GetUsedBlockCount(), 2);
}

TEST(TBlockMapTest, RepeatedSnapshots)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutDirty(1, TDirtyBlockId(11));
    blockMap->PutDirty(2, TDirtyBlockId(22));

    // Back-to-back snapshots with no intervening write are identical and leave the map reusable.
    auto first = blockMap->TakeSnapshot();
    auto second = blockMap->TakeSnapshot();
    EXPECT_EQ(first.Blocks, second.Blocks);
    ASSERT_EQ(std::ssize(first.Blocks), 2);

    // A later write shows up in the next snapshot.
    blockMap->PutDirty(3, TDirtyBlockId(33));
    auto third = blockMap->TakeSnapshot();
    ASSERT_EQ(std::ssize(third.Blocks), 3);
    EXPECT_EQ(third.Blocks[2], std::pair(3, MakeDirty(33)));
}

TEST(TBlockMapTest, TakeSnapshotEmpty)
{
    auto blockMap = CreateBlockMap(4);
    EXPECT_TRUE(blockMap->TakeSnapshot().Blocks.empty());

    // The empty snapshot leaves the map reusable.
    blockMap->PutDirty(0, TDirtyBlockId(7));
    auto snapshot = blockMap->TakeSnapshot();
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(0, MakeDirty(7)));
}

TEST(TBlockMapTest, ZeroBlocks)
{
    auto blockMap = CreateBlockMap(0);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);
    EXPECT_TRUE(blockMap->TakeSnapshot().Blocks.empty());
}

////////////////////////////////////////////////////////////////////////////////

// Copy-on-write snapshot cases: the TakeSnapshot callback injects a write at a chosen scan position, so
// a write "concurrent" with the scan is exercised deterministically. In an 8-block map the scan visits
// index 0 first and index 5 (our target) later, so injecting at index 0 is a write the scan has not yet
// reached and injecting at index 7 is one it has already passed.

TEST(TBlockMapTest, SnapshotWithWriteBeforeScannedSlot)
{
    auto blockMap = CreateBlockMap(8);
    blockMap->PutDirty(5, TDirtyBlockId(50));

    // The snapshot keeps the pre-flip value; the map keeps the new one.
    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutDirty(5, TDirtyBlockId(51));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(51));
}

TEST(TBlockMapTest, SnapshotWithWriteAfterScannedSlot)
{
    auto blockMap = CreateBlockMap(8);
    blockMap->PutDirty(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 7) {
            blockMap->PutDirty(5, TDirtyBlockId(51));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(51));
}

TEST(TBlockMapTest, SnapshotExcludesBlockFirstWrittenDuringScan)
{
    // A block empty at the flip and first written during the scan is not part of that point-in-time,
    // whether the write lands before or after the scan reaches its slot.
    for (int writeAt : {0, 7}) {
        auto blockMap = CreateBlockMap(8);
        auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
            if (scanIndex == writeAt) {
                blockMap->PutDirty(5, TDirtyBlockId(50));
            }
        });
        EXPECT_TRUE(snapshot.Blocks.empty());
        EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(50));
    }
}

TEST(TBlockMapTest, SnapshotKeepsDirtyWhenMadeCleanDuringScan)
{
    // A flush (TryMakeClean) landing during the scan does not change the captured point-in-time value.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutDirty(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->TryMakeClean(5, TDirtyBlockId(50), TStoredBlockId(500)));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeStored(500));
}

TEST(TBlockMapTest, SnapshotStashesOnlyFirstWriteDuringScan)
{
    // Several writes during the scan; only the pre-flip value is captured.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutDirty(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutDirty(5, TDirtyBlockId(51));
            blockMap->PutDirty(5, TDirtyBlockId(52));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(52));
}

TEST(TBlockMapTest, RepeatedSnapshotsWithWritesAreEachPointInTime)
{
    // Regression: the CoW bit must be cleared after each snapshot, so the second snapshot captures its
    // own point-in-time value rather than a stale one.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutDirty(0, TDirtyBlockId(10));

    auto first = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutDirty(0, TDirtyBlockId(11));
        }
    });
    ASSERT_EQ(std::ssize(first.Blocks), 1);
    EXPECT_EQ(first.Blocks[0], std::pair(0, MakeDirty(10)));

    auto second = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutDirty(0, TDirtyBlockId(12));
        }
    });
    ASSERT_EQ(std::ssize(second.Blocks), 1);
    // The value at the second flip is 11 -- not 10 (first flip) and not 12 (written during the scan).
    EXPECT_EQ(second.Blocks[0], std::pair(0, MakeDirty(11)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMappedBlockIdTest, StoredRoundtrip)
{
    auto mapped = MakeStored(123);
    EXPECT_TRUE(IsStoredMappedBlockId(mapped));
    EXPECT_FALSE(IsDirtyMappedBlockId(mapped));
    EXPECT_EQ(ToStoredBlockId(mapped), TStoredBlockId(123));
    EXPECT_NE(mapped, EmptyMappedBlockId);
}

TEST(TMappedBlockIdTest, DirtyRoundtrip)
{
    auto mapped = MakeDirty(456);
    EXPECT_TRUE(IsDirtyMappedBlockId(mapped));
    EXPECT_FALSE(IsStoredMappedBlockId(mapped));
    EXPECT_EQ(ToDirtyBlockId(mapped), TDirtyBlockId(456));
    EXPECT_NE(mapped, EmptyMappedBlockId);
}

TEST(TMappedBlockIdTest, Empty)
{
    EXPECT_FALSE(IsStoredMappedBlockId(EmptyMappedBlockId));
    EXPECT_FALSE(IsDirtyMappedBlockId(EmptyMappedBlockId));
}

TEST(TMappedBlockIdTest, MaxPayloadRoundtrip)
{
    // The largest payload that fits in the low bits round-trips without touching the tag/CoW bits.
    auto payload = NMappedBlockIdLayout::PayloadMask;
    EXPECT_EQ(ToStoredBlockId(MakeStored(payload)), TStoredBlockId(payload));
    EXPECT_EQ(ToDirtyBlockId(MakeDirty(payload)), TDirtyBlockId(payload));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
