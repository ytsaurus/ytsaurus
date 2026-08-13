#include <yt/yt/server/lib/nbd/journal/block_map.h>
#include <yt/yt/server/lib/nbd/journal/block_store_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/finally.h>

#include <functional>

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

std::shared_ptr<std::vector<TStoredBlockId>> TrackUnreferencedStoredBlocks(const IBlockMapPtr& blockMap)
{
    auto unreferencedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        unreferencedIds));
    return unreferencedIds;
}

////////////////////////////////////////////////////////////////////////////////

//! The block map exposes a snapshot as an open scan; these tests want the whole cut in hand.
TBlockMapSnapshot TakeSnapshot(
    const IBlockMapPtr& blockMap,
    const std::function<void(int blockIndex)>& onScanned = {})
{
    blockMap->BeginSnapshot();
    auto endSnapshotGuard = Finally([&] {
        blockMap->EndSnapshot();
    });

    // A part per slot, so |onScanned| can inject a write at any scan position.
    TBlockMapSnapshot snapshot;
    for (int blockIndex = 0; blockIndex < blockMap->GetBlockCount(); ++blockIndex) {
        if (onScanned) {
            onScanned(blockIndex);
        }
        auto part = blockMap->ScanSnapshotPart(blockIndex, blockIndex + 1);
        snapshot.Blocks.insert(snapshot.Blocks.end(), part.Blocks.begin(), part.Blocks.end());
    }
    return snapshot;
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

TEST(TBlockMapTest, DiscardBlock)
{
    auto blockMap = CreateBlockMap(4);

    EXPECT_FALSE(blockMap->DiscardBlock(0));
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);

    blockMap->PutBlock(1, TDirtyBlockId(11));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
    EXPECT_TRUE(blockMap->DiscardBlock(1));
    EXPECT_EQ(blockMap->FindBlock(1), EmptyMappedBlockId);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);

    EXPECT_FALSE(blockMap->DiscardBlock(1));
    blockMap->PutBlock(1, TDirtyBlockId(12));
    EXPECT_EQ(blockMap->FindBlock(1), MakeDirty(12));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
}

TEST(TBlockMapTest, DiscardBlockUnreferencesStoredBlock)
{
    auto blockMap = CreateBlockMap(2);
    auto unreferencedIds = TrackUnreferencedStoredBlocks(blockMap);

    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(10)));
    EXPECT_TRUE(unreferencedIds->empty());

    EXPECT_TRUE(blockMap->DiscardBlock(0));
    ASSERT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_EQ((*unreferencedIds)[0], TStoredBlockId(10));
}

TEST(TBlockMapTest, DiscardBlockDropsInFlightFlush)
{
    auto blockMap = CreateBlockMap(1);
    auto unreferencedIds = TrackUnreferencedStoredBlocks(blockMap);

    // The dirty block stays in the pool and is still flushed, but its stored copy arrives to an
    // emptied slot: it is not adopted, so it is unreferenced on arrival.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->DiscardBlock(0));
    EXPECT_TRUE(unreferencedIds->empty());

    EXPECT_FALSE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(99)));
    EXPECT_EQ(blockMap->FindBlock(0), EmptyMappedBlockId);
    ASSERT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_EQ((*unreferencedIds)[0], TStoredBlockId(99));
}

TEST(TBlockMapTest, DiscardedBlockIsExcludedFromSnapshot)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(2, TDirtyBlockId(22));
    EXPECT_TRUE(blockMap->DiscardBlock(1));

    auto snapshot = TakeSnapshot(blockMap);
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(2, MakeDirty(22)));
}

TEST(TBlockMapTest, TakeSnapshot)
{
    auto blockMap = CreateBlockMap(8);

    // Block 3 clean; blocks 1 and 5 dirty; the rest empty (omitted).
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(3, TDirtyBlockId(33));
    EXPECT_TRUE(blockMap->TryPutBlock(3, MakeDirty(33), TStoredBlockId(333)));
    blockMap->PutBlock(5, TDirtyBlockId(55));

    auto snapshot = TakeSnapshot(blockMap);

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

    auto snapshot = TakeSnapshot(blockMap);

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
    auto first = TakeSnapshot(blockMap);
    auto second = TakeSnapshot(blockMap);
    EXPECT_EQ(first.Blocks, second.Blocks);
    ASSERT_EQ(std::ssize(first.Blocks), 2);

    // A later write shows up in the next snapshot.
    blockMap->PutBlock(3, TDirtyBlockId(33));
    auto third = TakeSnapshot(blockMap);
    ASSERT_EQ(std::ssize(third.Blocks), 3);
    EXPECT_EQ(third.Blocks[2], std::pair(3, MakeDirty(33)));
}

TEST(TBlockMapTest, RescanReproducesTheSameCut)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(2, TDirtyBlockId(22));

    blockMap->BeginSnapshot();
    auto endSnapshotGuard = Finally([&] {
        blockMap->EndSnapshot();
    });

    auto scan = [&] {
        return blockMap->ScanSnapshotPart(0, blockMap->GetBlockCount()).Blocks;
    };

    auto expected = std::vector<std::pair<int, TMappedBlockId>>{
        {1, MakeDirty(11)},
        {2, MakeDirty(22)},
    };
    EXPECT_EQ(scan(), expected);

    // Every way a slot can move on: overwritten, emptied, and written for the first time.
    blockMap->PutBlock(1, TDirtyBlockId(111));
    EXPECT_TRUE(blockMap->DiscardBlock(2));
    blockMap->PutBlock(3, TDirtyBlockId(33));

    EXPECT_EQ(scan(), expected);
}

TEST(TBlockMapTest, ScanSnapshotPartHonoursItsRange)
{
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(5, TDirtyBlockId(55));

    blockMap->BeginSnapshot();
    auto endSnapshotGuard = Finally([&] {
        blockMap->EndSnapshot();
    });

    EXPECT_EQ(blockMap->ScanSnapshotPart(0, 4).Blocks, (std::vector<std::pair<int, TMappedBlockId>>{
        {1, MakeDirty(11)},
    }));
    EXPECT_EQ(blockMap->ScanSnapshotPart(4, 8).Blocks, (std::vector<std::pair<int, TMappedBlockId>>{
        {5, MakeDirty(55)},
    }));
    EXPECT_TRUE(blockMap->ScanSnapshotPart(2, 5).Blocks.empty());
    EXPECT_TRUE(blockMap->ScanSnapshotPart(3, 3).Blocks.empty());
}

TEST(TBlockMapTest, ScanSnapshotPartDropsABlockFirstWrittenMidPart)
{
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(1, TDirtyBlockId(11));
    blockMap->PutBlock(5, TDirtyBlockId(55));

    blockMap->BeginSnapshot();
    auto endSnapshotGuard = Finally([&] {
        blockMap->EndSnapshot();
    });

    // Block 3 is dropped from the middle of the part and block 1 is restored from the stash, so the
    // survivors on either side must keep their own values rather than a neighbour's.
    blockMap->PutBlock(3, TDirtyBlockId(33));
    blockMap->PutBlock(1, TDirtyBlockId(111));

    EXPECT_EQ(blockMap->ScanSnapshotPart(0, 8).Blocks, (std::vector<std::pair<int, TMappedBlockId>>{
        {1, MakeDirty(11)},
        {5, MakeDirty(55)},
    }));
}

TEST(TBlockMapTest, TakeSnapshotEmpty)
{
    auto blockMap = CreateBlockMap(4);
    EXPECT_TRUE(TakeSnapshot(blockMap).Blocks.empty());

    // The empty snapshot leaves the map reusable.
    blockMap->PutBlock(0, TDirtyBlockId(7));
    auto snapshot = TakeSnapshot(blockMap);
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(0, MakeDirty(7)));
}

TEST(TBlockMapTest, ZeroBlocks)
{
    auto blockMap = CreateBlockMap(0);
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);
    EXPECT_TRUE(TakeSnapshot(blockMap).Blocks.empty());
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
    auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
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

    auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
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
        auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
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

    auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
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

    auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(5, TDirtyBlockId(51));
            blockMap->PutBlock(5, TDirtyBlockId(52));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(52));
}

TEST(TBlockMapTest, SnapshotKeepsBlockDiscardedDuringScan)
{
    // A discard during the scan does not change the captured point-in-time value. Discarding ahead of
    // the scan is what the CoW bit on the emptied slot exists for: without it the scan would skip the
    // slot as empty and the stashed pre-flip value would have nowhere to be restored to.
    for (int discardAt : {0, 7}) {
        auto blockMap = CreateBlockMap(8);
        blockMap->PutBlock(5, TDirtyBlockId(50));

        auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
            if (scanIndex == discardAt) {
                EXPECT_TRUE(blockMap->DiscardBlock(5));
            }
        });
        ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
        EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
        EXPECT_EQ(blockMap->FindBlock(5), EmptyMappedBlockId);
    }
}

TEST(TBlockMapTest, SnapshotKeepsBlockDiscardedAndRewrittenDuringScan)
{
    // Only the first mutation stashes, so a discard followed by a rewrite still yields the pre-flip value.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(5, TDirtyBlockId(50));

    auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->DiscardBlock(5));
            blockMap->PutBlock(5, TDirtyBlockId(51));
        }
    });
    ASSERT_EQ(std::ssize(snapshot.Blocks), 1);
    EXPECT_EQ(snapshot.Blocks[0], std::pair(5, MakeDirty(50)));
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(51));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
}

TEST(TBlockMapTest, UsedBlockCountSurvivesDiscardAndRewriteDuringScan)
{
    // Regression: mid-scan a discarded slot holds a CoW-tagged empty, and the used-block count must
    // still read it as empty. Comparing the raw slot instead skips the re-increment here, leaving the
    // count permanently low.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(5, TDirtyBlockId(50));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);

    TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->DiscardBlock(5));
            EXPECT_EQ(blockMap->GetUsedBlockCount(), 0);
            blockMap->PutBlock(5, TDirtyBlockId(51));
            EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
        }
    });

    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
    EXPECT_EQ(blockMap->FindBlock(5), MakeDirty(51));

    // The same cycle outside a scan must not drift the count either.
    EXPECT_TRUE(blockMap->DiscardBlock(5));
    blockMap->PutBlock(5, TDirtyBlockId(52));
    EXPECT_EQ(blockMap->GetUsedBlockCount(), 1);
}

TEST(TBlockMapTest, SnapshotExcludesBlockWrittenAndDiscardedDuringScan)
{
    // Empty at the flip, so it is not part of that point-in-time however it churns during the scan.
    for (int mutateAt : {0, 7}) {
        auto blockMap = CreateBlockMap(8);
        auto snapshot = TakeSnapshot(blockMap, [&] (int scanIndex) {
            if (scanIndex == mutateAt) {
                blockMap->PutBlock(5, TDirtyBlockId(50));
                EXPECT_TRUE(blockMap->DiscardBlock(5));
            }
        });
        EXPECT_TRUE(snapshot.Blocks.empty());
        EXPECT_EQ(blockMap->FindBlock(5), EmptyMappedBlockId);
    }
}

TEST(TBlockMapTest, RepeatedSnapshotsAfterDiscardDuringScan)
{
    // Regression: the CoW bit must be cleared off an emptied slot too, so the next snapshot sees it as
    // plainly empty rather than as a used block.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(0, TDirtyBlockId(10));

    auto first = TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->DiscardBlock(0));
        }
    });
    ASSERT_EQ(std::ssize(first.Blocks), 1);
    EXPECT_EQ(first.Blocks[0], std::pair(0, MakeDirty(10)));

    EXPECT_TRUE(TakeSnapshot(blockMap).Blocks.empty());
}

TEST(TBlockMapTest, RepeatedSnapshotsWithWritesAreEachPointInTime)
{
    // Regression: the CoW bit must be cleared after each snapshot, so the second snapshot captures its
    // own point-in-time value rather than a stale one.
    auto blockMap = CreateBlockMap(8);
    blockMap->PutBlock(0, TDirtyBlockId(10));

    auto first = TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(0, TDirtyBlockId(11));
        }
    });
    ASSERT_EQ(std::ssize(first.Blocks), 1);
    EXPECT_EQ(first.Blocks[0], std::pair(0, MakeDirty(10)));

    auto second = TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            blockMap->PutBlock(0, TDirtyBlockId(12));
        }
    });
    ASSERT_EQ(std::ssize(second.Blocks), 1);
    // The value at the second flip is 11 -- not 10 (first flip) and not 12 (written during the scan).
    EXPECT_EQ(second.Blocks[0], std::pair(0, MakeDirty(11)));
}

TEST(TBlockMapTest, OverwritingStoredBlockUnreferencesIt)
{
    auto blockMap = CreateBlockMap(1);
    auto unreferencedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        unreferencedIds));

    // Write + flush to stored 10: nothing is unreferenced yet.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(10)));
    EXPECT_TRUE(unreferencedIds->empty());

    // Overwriting the (clean) block unreferences stored 10.
    blockMap->PutBlock(0, TDirtyBlockId(2));
    ASSERT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_EQ((*unreferencedIds)[0], TStoredBlockId(10));

    // Re-flushing and overwriting again unreferences the next stored id.
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(11)));
    blockMap->PutBlock(0, TDirtyBlockId(3));
    ASSERT_EQ(std::ssize(*unreferencedIds), 2);
    EXPECT_EQ((*unreferencedIds)[1], TStoredBlockId(11));
}

TEST(TBlockMapTest, OverwritingDirtyBlockUnreferencesNothing)
{
    auto blockMap = CreateBlockMap(1);
    auto unreferencedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        unreferencedIds));

    // A dirty block overwritten before it flushes never had a stored id: nothing to unreference.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_TRUE(unreferencedIds->empty());
}

TEST(TBlockMapTest, LostFlushRaceUnreferencesStoredBlock)
{
    auto blockMap = CreateBlockMap(1);
    auto unreferencedIds = std::make_shared<std::vector<TStoredBlockId>>();
    blockMap->SubscribeStoredBlockUnreferenced(BIND(
        [] (const std::shared_ptr<std::vector<TStoredBlockId>>& out, TStoredBlockId id) {
            out->push_back(id);
        },
        unreferencedIds));

    // A newer write supersedes the drained dirty id, so the flush of stored 99 is never adopted --
    // it is unreferenced on arrival.
    blockMap->PutBlock(0, TDirtyBlockId(1));
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_FALSE(blockMap->TryPutBlock(0, MakeDirty(1), TStoredBlockId(99)));
    ASSERT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_EQ((*unreferencedIds)[0], TStoredBlockId(99));

    // The winning flush is adopted and stays referenced.
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(2), TStoredBlockId(100)));
    EXPECT_EQ(std::ssize(*unreferencedIds), 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockMapTest, TryPutBlockRelocatesStoredBlock)
{
    auto blockMap = CreateBlockMap(2);
    auto unreferencedIds = TrackUnreferencedStoredBlocks(blockMap);

    auto oldId = MakeStoredInChunk(1, 0);
    auto newId = MakeStoredInChunk(2, 0);
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), oldId));

    // A matching remap repoints the block and unreferences the old (superseded) stored id.
    EXPECT_TRUE(blockMap->TryPutBlock(0, ToMappedBlockId(oldId), newId));
    EXPECT_EQ(blockMap->FindBlock(0), ToMappedBlockId(newId));
    ASSERT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_EQ((*unreferencedIds)[0], oldId);
}

TEST(TBlockMapTest, TryPutBlockRejectsSupersededRelocation)
{
    auto blockMap = CreateBlockMap(1);
    auto unreferencedIds = TrackUnreferencedStoredBlocks(blockMap);

    auto oldId = MakeStoredInChunk(1, 0);
    auto newId = MakeStoredInChunk(2, 0);
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), oldId));

    // A newer write superseded the block being compacted, so the remap is rejected and the freshly
    // written copy is unreferenced on arrival -- the surviving mapping is untouched.
    blockMap->PutBlock(0, TDirtyBlockId(2));
    EXPECT_EQ(std::ssize(*unreferencedIds), 1);
    EXPECT_FALSE(blockMap->TryPutBlock(0, ToMappedBlockId(oldId), newId));
    EXPECT_EQ(blockMap->FindBlock(0), MakeDirty(2));
    ASSERT_EQ(std::ssize(*unreferencedIds), 2);
    EXPECT_EQ((*unreferencedIds)[1], newId);
}

TEST(TBlockMapTest, GetChunkBlocks)
{
    auto blockMap = CreateBlockMap(5);

    auto putStored = [&] (int blockIndex, ui64 dirty, TStoredBlockId storedId) {
        blockMap->PutBlock(blockIndex, TDirtyBlockId(dirty));
        EXPECT_TRUE(blockMap->TryPutBlock(blockIndex, MakeDirty(dirty), storedId));
    };
    putStored(0, 1, MakeStoredInChunk(7, 0));
    putStored(3, 2, MakeStoredInChunk(7, 5));
    putStored(1, 3, MakeStoredInChunk(9, 0));
    // Block 2 stays dirty and block 4 empty; neither belongs to a chunk.
    blockMap->PutBlock(2, TDirtyBlockId(4));

    EXPECT_EQ(blockMap->GetChunkBlocks(7), (std::vector<std::pair<int, TStoredBlockId>>{
        {0, MakeStoredInChunk(7, 0)},
        {3, MakeStoredInChunk(7, 5)},
    }));
    EXPECT_EQ(blockMap->GetChunkBlocks(9), (std::vector<std::pair<int, TStoredBlockId>>{
        {1, MakeStoredInChunk(9, 0)},
    }));
    EXPECT_TRUE(blockMap->GetChunkBlocks(8).empty());
}

TEST(TBlockMapTest, GetChunkBlocksAtLayoutBounds)
{
    auto blockMap = CreateBlockMap(2);

    // Every field at its maximum, so any bleed between the chunk index and the tag bits shows up here.
    auto maxStoredBlockId = MakeStoredInChunk(
        MaxChunksPerDevice - 1,
        MaxRecordsPerChunk - 1,
        MaxBlocksPerRecord - 1);
    blockMap->PutBlock(0, TDirtyBlockId(1));
    EXPECT_TRUE(blockMap->TryPutBlock(0, MakeDirty(1), maxStoredBlockId));

    EXPECT_EQ(blockMap->GetChunkBlocks(MaxChunksPerDevice - 1), (std::vector<std::pair<int, TStoredBlockId>>{
        {0, maxStoredBlockId},
    }));
    EXPECT_TRUE(blockMap->GetChunkBlocks(0).empty());
}

TEST(TBlockMapTest, GetChunkBlocksSeesBlocksWrittenUnderSnapshot)
{
    auto blockMap = CreateBlockMap(4);
    blockMap->PutBlock(1, TDirtyBlockId(11));

    // A slot rewritten mid-scan carries the CoW mark; the chunk lookup must still report its current
    // value rather than the snapshotted one.
    auto storedBlockId = MakeStoredInChunk(3, 0);
    std::vector<std::pair<int, TStoredBlockId>> duringSnapshot;
    TakeSnapshot(blockMap, [&] (int scanIndex) {
        if (scanIndex == 0) {
            EXPECT_TRUE(blockMap->TryPutBlock(1, MakeDirty(11), storedBlockId));
            duringSnapshot = blockMap->GetChunkBlocks(3);
        }
    });

    auto expected = std::vector<std::pair<int, TStoredBlockId>>{{1, storedBlockId}};
    EXPECT_EQ(duringSnapshot, expected);
    EXPECT_EQ(blockMap->GetChunkBlocks(3), expected);
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
