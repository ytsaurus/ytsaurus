#include <yt/yt/server/lib/nbd/journal/block_map.h>
#include <yt/yt/server/lib/nbd/journal/block_store_helpers.h>

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

TStoredBlockId MakeStoredInChunk(int chunkIndex, int recordIndex, int fragmentIndex = 0)
{
    return MakeStoredBlockId({
        .ChunkIndex = chunkIndex,
        .RecordIndex = recordIndex,
        .FragmentIndex = fragmentIndex,
    });
}

std::shared_ptr<std::vector<TStoredBlockId>> TrackStoredBlockDeaths(const IBlockMapPtr& blockMap)
{
    auto diedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        diedIds));
    return diedIds;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockMapTest, InitiallyEmpty)
{
    auto blockMap = CreateBlockMap(4);
    for (int blockIndex = 0; blockIndex < 4; ++blockIndex) {
        EXPECT_EQ(blockMap->FindBlock(blockIndex), EmptyMappedBlockId);
    }
}

TEST(TBlockMapTest, PutBlockThenFind)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(1, TDirtyBlockId(123));

    EXPECT_EQ(blockMap->FindBlock(1), MakeDirty(123));

    // Other blocks are untouched.
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->FindBlock(2), EmptyMappedBlockId);
}

TEST(TBlockMapTest, TryPutBlockFromDirty)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(2, TDirtyBlockId(123));
    EXPECT_TRUE(blockMap->TryPutBlock(2, MakeDirty(123), TStoredBlockId(456)));

    EXPECT_EQ(blockMap->FindBlock(2), MakeStored(456));
}

TEST(TBlockMapTest, TryPutBlockFailsWhenDirtySuperseded)
{
    auto blockMap = CreateBlockMap(1);

    // A newer write replaced the drained dirty id, so the clean transition must be rejected
    // and the newer dirty mapping must survive.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_FALSE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(99)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(2));

    // Matching the current dirty id succeeds.
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(99)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeStored(99));

    // A clean block is not dirty under any id, so a further clean transition is rejected.
    EXPECT_FALSE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(100)));
}

TEST(TBlockMapTest, TagTransitions)
{
    auto blockMap = CreateBlockMap(1);

    // Empty -> dirty -> clean -> dirty, each overwriting the previous mapping.
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);

    blockMap->PutBlock(0, TDirtyBlockId(7));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(7));

    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(7), TStoredBlockId(8)));
    EXPECT_EQ(blockMap->FindBlock(0), MakeStored(8));

    blockMap->PutBlock(0, TDirtyBlockId(9));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(9));
}

TEST(TBlockMapTest, GetUsedBlockCount)
{
    auto blockMap = CreateBlockMap(4);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);

    // The first write to a block makes it used.
    blockMap->PutBlock(1, TDirtyBlockId(1));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);

    // Writing a distinct block bumps the count again.
    blockMap->PutBlock(3, TDirtyBlockId(2));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);

    // Rewriting an already-used block does not.
    blockMap->PutBlock(1, TDirtyBlockId(3));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);

    // Neither does flushing it clean, nor a subsequent rewrite.
    EXPECT_TRUE(blockMap->TryPutBlock(1, MakeDirty(3), TStoredBlockId(4)));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
    blockMap->PutBlock(1, TDirtyBlockId(5));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
}

TEST(TBlockMapTest, TakeSnapshot)
{
    auto blockMap = CreateBlockMap(8);

    // Block 3 clean; blocks 1 and 5 dirty; the rest empty (omitted).
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(3, TDirtyBlockId(33));
    EXPECT_TRUE(blockMap->TryPutBlock(3, MakeDirty(33), TStoredBlockId(333)));
    blockMap->PutBlock(5, TDirtyBlockId(55));

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
    blockMap->BeginLoadSnapshot();
    blockMap->LoadSnapshotPart(snapshot);
    blockMap->EndLoadSnapshot();

    EXPECT_EQ(blockMap->FindBlock(2), MakeStored(22));
    EXPECT_EQ(blockMap->FindBlock(6), MakeStored(66));
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 2);
}

TEST(TBlockMapTest, SnapshotThenLoadRoundtrip)
{
    auto blockMap = CreateBlockMap(8);

    // A snapshot fit for LoadSnapshotPart holds only stored (clean) blocks.
    blockMap->PutBlock(1, TDirtyBlockId(11));
    EXPECT_TRUE(blockMap->TryPutBlock(1, MakeDirty(11), TStoredBlockId(111)));
    blockMap->PutBlock(4, TDirtyBlockId(44));
    EXPECT_TRUE(blockMap->TryPutBlock(4, MakeDirty(44), TStoredBlockId(444)));

    auto snapshot = blockMap->TakeSnapshot();

    auto restored = CreateBlockMap(8);
    restored->BeginLoadSnapshot();
    restored->LoadSnapshotPart(snapshot);
    restored->EndLoadSnapshot();

    EXPECT_EQ(restored->FindBlock(1), MakeStored(111));
    EXPECT_EQ(restored->FindBlock(4), MakeStored(444));
    EXPECT_EQ(restored->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(restored->GetUsedBlockCount(), 2);
}

TEST(TBlockMapTest, RepeatedSnapshots)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(2, TDirtyBlockId(22));

    // Back-to-back snapshots with no intervening write are identical and leave the map reusable.
    auto first = blockMap->TakeSnapshot();
    auto second = blockMap->TakeSnapshot();
    EXPECT_EQ(first.Blocks, second.Blocks);
    ASSERT_EQ(std::ssize(first.Blocks), 2);

    // A later write shows up in the next snapshot.
    blockMap->PutBlock(3, TDirtyBlockId(33));
    auto third = blockMap->TakeSnapshot();
    ASSERT_EQ(std::ssize(third.Blocks), 3);
    EXPECT_EQ(third.Blocks[2], std::pair(3, MakeDirty(33)));
}

TEST(TBlockMapTest, TakeSnapshotEmpty)
{
    auto blockMap = CreateBlockMap(4);
    EXPECT_TRUE(blockMap->TakeSnapshot().Blocks.empty());

    // The empty snapshot leaves the map reusable.
    blockMap->PutBlock(0, TDirtyBlockId(7));
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
    blockMap->PutBlock(5, TDirtyBlockId(50));

    // The snapshot keeps the pre-flip value; the map keeps the new one.
    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(5, TDirtyBlockId(51));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(51));
}

TEST(TBlockMapTest, SnapshotWithWriteAfterScannedSlot)
{
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 7) {
            blockMap->PutBlock(5, TDirtyBlockId(51));
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
                blockMap->PutBlock(5, TDirtyBlockId(50));
            }
        });
        EXPECT_TRUE(snapshot.Blocks.empty());
        EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(50));
    }
}

TEST(TBlockMapTest, SnapshotKeepsDirtyWhenMadeCleanDuringScan)
{
    // A flush landing during the scan does not change the captured point-in-time value.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->TryPutBlock(5, MakeDirty(50), TStoredBlockId(500)));
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
    blockMap->PutBlock(5, TDirtyBlockId(50));

    auto snapshot = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(5, TDirtyBlockId(51));
            blockMap->PutBlock(5, TDirtyBlockId(52));
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
    blockMap->PutBlock(0, TDirtyBlockId(10));

    auto first = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(0, TDirtyBlockId(11));
        }
    });
    ASSERT_EQ(std::ssize(first.Blocks), 1);
    EXPECT_EQ(first.Blocks[0], std::pair(0, MakeDirty(10)));

    auto second = blockMap->TakeSnapshot([&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(0, TDirtyBlockId(12));
        }
    });
    ASSERT_EQ(std::ssize(second.Blocks), 1);
    // The value at the second flip is 11 -- not 10 (first flip) and not 12 (written during the scan).
    EXPECT_EQ(second.Blocks[0], std::pair(0, MakeDirty(11)));
}

TEST(TBlockMapTest, OverwritingStoredBlockKillsIt)
{
    auto blockMap = CreateBlockMap(1);
    auto diedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        diedIds));

    // Write + flush to stored 10: nothing is unreferenced yet.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(10)));
    EXPECT_TRUE(diedIds->empty());

    // Overwriting the (clean) block kills stored 10.
    blockMap->PutBlock(0, TDirtyBlockId(2));
    ASSERT_EQ(std::ssize(*diedIds), 1);
    EXPECT_EQ((*diedIds)[0], TStoredBlockId(10));

    // Re-flushing and overwriting again kills the next stored id.
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(11)));
    blockMap->PutBlock(0, TDirtyBlockId(3));
    ASSERT_EQ(std::ssize(*diedIds), 2);
    EXPECT_EQ((*diedIds)[1], TStoredBlockId(11));
}

TEST(TBlockMapTest, OverwritingDirtyBlockKillsNothing)
{
    auto blockMap = CreateBlockMap(1);
    auto diedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        diedIds));

    // A dirty block overwritten before it flushes never had a stored id: no stored death.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_TRUE(diedIds->empty());
}

TEST(TBlockMapTest, LostFlushRaceKillsStoredBlock)
{
    auto blockMap = CreateBlockMap(1);
    auto diedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        diedIds));

    // A newer write supersedes the drained dirty id, so the flush of stored 99 is never adopted --
    // it is dead on arrival.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_FALSE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(99)));
    ASSERT_EQ(std::ssize(*diedIds), 1);
    EXPECT_EQ((*diedIds)[0], TStoredBlockId(99));

    // The winning flush is adopted and stays referenced.
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(100)));
    EXPECT_EQ(std::ssize(*diedIds), 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockMapTest, TryPutBlockRelocatesStoredBlock)
{
    auto blockMap = CreateBlockMap(2);
    auto diedIds = TrackStoredBlockDeaths(blockMap);

    auto oldId = MakeStoredInChunk(1, 0);
    auto newId = MakeStoredInChunk(2, 0);
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), oldId));

    // A matching remap repoints the block and kills the old (superseded) stored id.
    EXPECT_TRUE(blockMap->TryPutBlock(0, ToMappedBlockId(oldId), newId));
    EXPECT_EQ(blockMap->FindBlock(0), ToMappedBlockId(newId));
    ASSERT_EQ(std::ssize(*diedIds), 1);
    EXPECT_EQ((*diedIds)[0], oldId);
}

TEST(TBlockMapTest, TryPutBlockRejectsSupersededRelocation)
{
    auto blockMap = CreateBlockMap(1);
    auto diedIds = TrackStoredBlockDeaths(blockMap);

    auto oldId = MakeStoredInChunk(1, 0);
    auto newId = MakeStoredInChunk(2, 0);
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), oldId));

    // A newer write superseded the block being compacted, so the remap is rejected and the freshly
    // written copy is dead on arrival -- the surviving mapping is untouched.
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_EQ(std::ssize(*diedIds), 1);
    EXPECT_FALSE(blockMap->TryPutBlock(0, ToMappedBlockId(oldId), newId));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(2));
    ASSERT_EQ(std::ssize(*diedIds), 2);
    EXPECT_EQ((*diedIds)[1], newId);
}

TEST(TBlockMapTest, IterateBlocks)
{
    auto blockMap = CreateBlockMap(5);

    auto putStored = [&] (int blockIndex, ui64 dirty, TStoredBlockId storedId) {
        blockMap->PutBlock(blockIndex, TDirtyBlockId(dirty));
        EXPECT_TRUE(blockMap->TryPutBlock(blockIndex, MakeDirty(dirty), storedId));
    };
    putStored(0, 1, MakeStoredInChunk(7, 0));
    putStored(3, 2, MakeStoredInChunk(7, 5));
    putStored(1, 3, MakeStoredInChunk(9, 0));
    blockMap->PutBlock(2, TDirtyBlockId(4));

    // Every used block, stored and dirty alike, in ascending index order; the empty block 4 is skipped.
    std::vector<std::pair<int, TMappedBlockId>> visited;
    blockMap->IterateBlocks([&] (int blockIndex, TMappedBlockId mappedId) {
        visited.emplace_back(blockIndex, mappedId);
    });
    EXPECT_EQ(visited, (std::vector<std::pair<int, TMappedBlockId>>{
        {0, ToMappedBlockId(MakeStoredInChunk(7, 0))},
        {1, ToMappedBlockId(MakeStoredInChunk(9, 0))},
        {2, MakeDirty(4)},
        {3, ToMappedBlockId(MakeStoredInChunk(7, 5))},
    }));
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
