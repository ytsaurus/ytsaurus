#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/nbd/journal/block_compactor.h>
#include <yt/yt/server/lib/nbd/journal/block_map.h>
#include <yt/yt/server/lib/nbd/journal/block_store.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/block_store_helpers.h>
#include <yt/yt/server/lib/nbd/journal/public.h>
#include <yt/yt/server/lib/nbd/journal/snapshot_reader.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/random/random.h>

#include <set>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;

using NCppTests::TApiTestBase;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("NbdTest");

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomString(i64 size)
{
    std::string result;
    result.reserve(size);
    for (i64 index = 0; index < size; ++index) {
        result += static_cast<char>(RandomNumber<ui32>(256));
    }
    return result;
}

std::vector<TSharedRef> MakeRandomBlocks(int count, i64 blockSize)
{
    std::vector<TSharedRef> blocks;
    blocks.reserve(count);
    for (int index = 0; index < count; ++index) {
        blocks.push_back(TSharedRef::FromString(MakeRandomString(blockSize)));
    }
    return blocks;
}

ITransactionPtr StartDeviceTransaction(const NNative::IClientPtr& client)
{
    const auto& connection = client->GetNativeConnection();
    WaitFor(connection->GetMasterCellDirectorySynchronizer()->RecentSync())
        .ThrowOnError();
    TTransactionStartOptions options;
    options.CoordinatorMasterCellTag = connection->GetRandomMasterCellTagWithRoleOrThrow(
        NCellMasterClient::EMasterCellRole::ChunkHost);
    return WaitFor(client->StartTransaction(ETransactionType::Master, options))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStoreTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    TActionQueuePtr ActionQueue_;
    ITransactionPtr Transaction_;
    std::vector<IBlockStorePtr> Stores_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        ActionQueue_ = New<TActionQueue>("BlockStoreTest");
        Transaction_ = StartDeviceTransaction(NativeClient_);
    }

    void TearDown() override
    {
        // Request maintenance stop before the transaction goes away; the queue shutdown below drains
        // whatever tick is still in flight.
        for (const auto& store : Stores_) {
            store->Stop();
        }
        Stores_.clear();
        if (Transaction_) {
            YT_UNUSED_FUTURE(Transaction_->Abort());
            Transaction_.Reset();
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
            ActionQueue_.Reset();
        }
    }

    static TJournalBlockDeviceConfigPtr CreateConfig()
    {
        return New<TJournalBlockDeviceConfig>();
    }

    static TJournalBlockDeviceOptionsPtr CreateOptions(i64 blockSize)
    {
        auto options = New<TJournalBlockDeviceOptions>();
        options->BlockSize = blockSize;
        options->Account = NSecurityClient::TmpAccountName;
        options->MediumName = NChunkClient::DefaultStoreMediumName;
        return options;
    }

    IBlockStorePtr CreateStore(TJournalBlockDeviceConfigPtr config, TJournalBlockDeviceOptionsPtr options)
    {
        // RestoreBlocks validates snapshot block indices against the block count, so keep it well
        // above the indices the tests use.
        TBlockDeviceGeometry geometry{.BlockSize = options->BlockSize, .BlockCount = 1024};
        auto store = CreateJournalBlockStore(
            config->BlockStore,
            geometry,
            std::move(options),
            NativeClient_,
            Transaction_->GetId(),
            NChunkClient::NullChunkListId,
            ActionQueue_->GetInvoker(),
            Logger);
        // Without maintenance no writable chunk is ever created, so every write would exhaust its
        // backoff. Stopped in TearDown.
        store->Start();
        Stores_.push_back(store);
        return store;
    }

    std::vector<TStoredBlockId> WriteBlocks(
        const IBlockStorePtr& store,
        const std::vector<TSharedRef>& blocks)
    {
        return WaitFor(store->WriteBlocks(blocks))
            .ValueOrThrow();
    }

    std::vector<TSharedRef> ReadBlocks(
        const IBlockStorePtr& store,
        const std::vector<TStoredBlockId>& blockIds)
    {
        return WaitFor(store->ReadBlocks(TRange(blockIds), EWorkloadCategory::UserInteractive))
            .ValueOrThrow();
    }

    static void ExpectBlocksEqual(
        TRange<TSharedRef> expected,
        TRange<TSharedRef> actual)
    {
        ASSERT_EQ(std::ssize(actual), std::ssize(expected));
        for (int index = 0; index < std::ssize(expected); ++index) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(actual[index], expected[index]))
                << "mismatch at block " << index;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBlockStoreTest, WriteReadRoundtrip)
{
    constexpr i64 BlockSize = 4_KB;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(16, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    ASSERT_EQ(std::ssize(blockIds), std::ssize(blocks));

    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
}

TEST_F(TBlockStoreTest, ReadSubsetInArbitraryOrder)
{
    constexpr i64 BlockSize = 1024;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(32, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);

    // Read a reversed subset and verify it matches the corresponding source blocks.
    std::vector<TStoredBlockId> queryIds;
    std::vector<TSharedRef> expected;
    for (int index = std::ssize(blocks) - 1; index >= 0; index -= 3) {
        queryIds.push_back(blockIds[index]);
        expected.push_back(blocks[index]);
    }

    ExpectBlocksEqual(expected, ReadBlocks(store, queryIds));
}

TEST_F(TBlockStoreTest, RepeatedReadsAreStable)
{
    constexpr i64 BlockSize = 4_KB;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(8, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);

    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
}

TEST_F(TBlockStoreTest, WriteSpanningMultipleRecords)
{
    // A single WriteBlocks batch that exceeds the per-record fragment cap is split into
    // several journal records; the round-trip must still be exact and ordered.
    constexpr i64 BlockSize = 512;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(2500, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    ASSERT_EQ(std::ssize(blockIds), std::ssize(blocks));

    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
}

TEST_F(TBlockStoreTest, MultipleWriteBatches)
{
    constexpr i64 BlockSize = 4_KB;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto firstBlocks = MakeRandomBlocks(4, BlockSize);
    auto secondBlocks = MakeRandomBlocks(4, BlockSize);
    auto firstIds = WriteBlocks(store, firstBlocks);
    auto secondIds = WriteBlocks(store, secondBlocks);

    ExpectBlocksEqual(firstBlocks, ReadBlocks(store, firstIds));
    ExpectBlocksEqual(secondBlocks, ReadBlocks(store, secondIds));
}

TEST_F(TBlockStoreTest, ChunkRotationOnDataSizeLimit)
{
    // Cap chunks at a single record's worth of data and write to one chunk at a time, so
    // every batch overflows the current chunk and the maintenance executor rotates to a
    // fresh one. All data must survive the rotation and be readable.
    constexpr i64 BlockSize = 4_KB;
    constexpr int BatchCount = 4;
    constexpr int BlocksPerBatch = 2;

    auto config = CreateConfig();
    config->BlockStore->WriteParallelism = 1;
    config->BlockStore->MaxChunkDataSize = BlocksPerBatch * BlockSize;
    config->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(100);
    auto store = CreateStore(config, CreateOptions(BlockSize));

    std::vector<TSharedRef> allBlocks;
    std::vector<TStoredBlockId> allIds;
    std::set<int> chunkIndexes;
    for (int batch = 0; batch < BatchCount; ++batch) {
        auto blocks = MakeRandomBlocks(BlocksPerBatch, BlockSize);
        auto ids = WriteBlocks(store, blocks);
        for (auto id : ids) {
            chunkIndexes.insert(ParseStoredBlockId(id).ChunkIndex);
        }
        allBlocks.insert(allBlocks.end(), blocks.begin(), blocks.end());
        allIds.insert(allIds.end(), ids.begin(), ids.end());

        // Give the maintenance executor time to retire the now-oversized chunk.
        Sleep(TDuration::MilliSeconds(300));
    }

    // The oversized chunks were retired, so the batches landed in more than one chunk.
    EXPECT_GT(std::ssize(chunkIndexes), 1);
    ExpectBlocksEqual(allBlocks, ReadBlocks(store, allIds));
}

TEST_F(TBlockStoreTest, FreeDropUnstagesFullyDeadChunk)
{
    // Staged under the transaction alone (no chunk list), so unstaging a fully-dead chunk destroys it.
    constexpr i64 BlockSize = 4_KB;
    auto config = CreateConfig();
    config->BlockStore->WriteParallelism = 1;
    config->BlockStore->MaxChunkDataSize = 2 * BlockSize;
    config->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(200);
    config->BlockStore->DeadChunkRetentionDelay = TDuration::Zero();
    auto store = CreateStore(config, CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(2, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);

    auto refs = store->GetBlockRefs(blockIds);
    auto chunkId = refs[0].ChunkId;
    for (const auto& ref : refs) {
        ASSERT_EQ(ref.ChunkId, chunkId);
    }

    auto chunkPath = FromObjectId(chunkId);
    ASSERT_TRUE(WaitFor(NativeClient_->NodeExists(chunkPath)).ValueOrThrow());

    for (auto blockId : blockIds) {
        store->ReleaseBlock(blockId);
    }

    WaitForPredicate(
        [&] { return !WaitFor(NativeClient_->NodeExists(chunkPath)).ValueOrThrow(); },
        Format("dead chunk %v was not unstaged", chunkId));
}

TEST_F(TBlockStoreTest, FreeDropKeepsChunkWithLiveBlock)
{
    constexpr i64 BlockSize = 4_KB;
    auto config = CreateConfig();
    config->BlockStore->WriteParallelism = 1;
    config->BlockStore->MaxChunkDataSize = 2 * BlockSize;
    config->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(200);
    config->BlockStore->DeadChunkRetentionDelay = TDuration::Zero();
    auto store = CreateStore(config, CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(2, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    auto chunkId = store->GetBlockRefs(blockIds)[0].ChunkId;
    auto chunkPath = FromObjectId(chunkId);

    store->ReleaseBlock(blockIds[0]);

    Sleep(TDuration::Seconds(2));
    ASSERT_TRUE(WaitFor(NativeClient_->NodeExists(chunkPath)).ValueOrThrow());
    ExpectBlocksEqual({blocks[1]}, ReadBlocks(store, {blockIds[1]}));
}

TEST_F(TBlockStoreTest, CompactionRelocatesLiveBlocksAndDropsChunk)
{
    // One chunk fills with four blocks; three are then overwritten, leaving it 75% garbage. With the
    // threshold at 0.5 the compactor relocates the surviving block into a fresh chunk, and the emptied
    // chunk goes fully dead and is unstaged (destroyed, since it is staged under the transaction alone).
    constexpr i64 BlockSize = 4_KB;
    constexpr int ChunkBlockCount = 4;
    auto config = CreateConfig();
    config->BlockStore->WriteParallelism = 1;
    config->BlockStore->MaxChunkDataSize = ChunkBlockCount * BlockSize;
    config->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(200);
    config->BlockStore->DeadChunkRetentionDelay = TDuration::Zero();
    config->BlockCompactor = New<TJournalBlockCompactorConfig>();
    config->BlockCompactor->GarbageRatioThreshold = 0.5;
    config->BlockCompactor->ScanPeriod = TDuration::MilliSeconds(200);
    auto store = CreateStore(config, CreateOptions(BlockSize));

    auto blockMap = CreateBlockMap(64);
    blockMap->SubscribeStoredBlockUnreferenced(BIND([store] (TStoredBlockId blockId) {
        store->ReleaseBlock(blockId);
    }));
    auto compactor = CreateBlockCompactor(
        config->BlockCompactor,
        blockMap,
        store,
        ActionQueue_->GetInvoker(),
        Logger);
    compactor->Start();
    auto stopCompactorGuard = Finally([&] {
        compactor->Stop();
    });

    i64 nextDirtyId = 1;
    auto publishStored = [&] (int blockIndex, TStoredBlockId storedBlockId) {
        auto dirtyBlockId = TDirtyBlockId(nextDirtyId++);
        blockMap->PutBlock(blockIndex, dirtyBlockId);
        ASSERT_TRUE(blockMap->TryPutBlock(blockIndex, ToMappedBlockId(dirtyBlockId), storedBlockId));
    };

    auto blocks = MakeRandomBlocks(ChunkBlockCount, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    auto oldChunkIndex = ParseStoredBlockId(blockIds[0]).ChunkIndex;
    for (int index = 0; index < ChunkBlockCount; ++index) {
        ASSERT_EQ(ParseStoredBlockId(blockIds[index]).ChunkIndex, oldChunkIndex);
        publishStored(index, blockIds[index]);
    }
    auto oldChunkId = store->GetBlockRefs({blockIds[0]})[0].ChunkId;
    auto oldChunkPath = FromObjectId(oldChunkId);

    // Let the maintenance executor retire the now-oversized chunk out of the writable set, so the
    // overwrites below land in a fresh chunk rather than back into this one.
    WaitForPredicate(
        [&] {
            for (const auto& info : store->GetChunkInfos()) {
                if (info.ChunkId == oldChunkId) {
                    return info.SealState != EChunkSealState::None;
                }
            }
            return false;
        },
        Format("oversized chunk %v was not retired", oldChunkId));

    // Overwrite the first three blocks: their old stored ids go unreferenced, leaving one referenced block.
    auto newBlocks = MakeRandomBlocks(ChunkBlockCount - 1, BlockSize);
    auto newBlockIds = WriteBlocks(store, newBlocks);
    ASSERT_NE(ParseStoredBlockId(newBlockIds[0]).ChunkIndex, oldChunkIndex);
    for (int index = 0; index < ChunkBlockCount - 1; ++index) {
        publishStored(index, newBlockIds[index]);
    }

    WaitForPredicate(
        [&] { return !WaitFor(NativeClient_->NodeExists(oldChunkPath)).ValueOrThrow(); },
        Format("compacted chunk %v was not unstaged", oldChunkId));

    // The surviving block was relocated to a different chunk and still reads back byte-for-byte.
    auto mappedBlockId = blockMap->FindBlock(ChunkBlockCount - 1);
    ASSERT_TRUE(IsStoredMappedBlockId(mappedBlockId));
    auto relocatedBlockId = ToStoredBlockId(mappedBlockId);
    EXPECT_NE(ParseStoredBlockId(relocatedBlockId).ChunkIndex, oldChunkIndex);
    ExpectBlocksEqual({blocks[ChunkBlockCount - 1]}, ReadBlocks(store, {relocatedBlockId}));
}

TEST_F(TBlockStoreTest, CompactionForgetsRestoredChunkWithoutUnstaging)
{
    constexpr i64 BlockSize = 4_KB;
    constexpr int ChunkBlockCount = 4;

    // Source store: write four blocks into one sealed chunk, so a restored store can reference it.
    auto sourceConfig = CreateConfig();
    sourceConfig->BlockStore->WriteParallelism = 1;
    auto sourceStore = CreateStore(sourceConfig, CreateOptions(BlockSize));

    auto blocks = MakeRandomBlocks(ChunkBlockCount, BlockSize);
    auto blockIds = WriteBlocks(sourceStore, blocks);
    auto refs = sourceStore->GetBlockRefs(blockIds);
    auto chunkId = refs[0].ChunkId;
    for (const auto& ref : refs) {
        ASSERT_EQ(ref.ChunkId, chunkId);
    }
    WaitFor(sourceStore->SealChunks({chunkId}))
        .ThrowOnError();
    auto chunkPath = FromObjectId(chunkId);
    ASSERT_TRUE(WaitFor(NativeClient_->NodeExists(chunkPath)).ValueOrThrow());

    // Restored store: reference only the last block but declare the chunk's true written count of four,
    // so the restored (read-only) chunk is 75% garbage.
    auto restoredConfig = CreateConfig();
    restoredConfig->BlockStore->WriteParallelism = 1;
    restoredConfig->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(200);
    restoredConfig->BlockStore->DeadChunkRetentionDelay = TDuration::Zero();
    restoredConfig->BlockCompactor = New<TJournalBlockCompactorConfig>();
    restoredConfig->BlockCompactor->GarbageRatioThreshold = 0.5;
    restoredConfig->BlockCompactor->ScanPeriod = TDuration::MilliSeconds(200);
    auto restoredStore = CreateStore(restoredConfig, CreateOptions(BlockSize));

    int survivorIndex = ChunkBlockCount - 1;
    std::vector snapshotBlocks{TSnapshotBlock{
        .Index = survivorIndex,
        .Ref = refs[survivorIndex],
    }};
    WaitFor(restoredStore->BeginRestoreBlocks())
        .ThrowOnError();
    auto restoredBlockId = WaitFor(restoredStore->RestoreBlocks(snapshotBlocks))
        .ValueOrThrow()[0];
    WaitFor(restoredStore->EndRestoreBlocks({{chunkId, ChunkBlockCount}}))
        .ThrowOnError();
    auto restoredChunkIndex = ParseStoredBlockId(restoredBlockId).ChunkIndex;

    auto blockMap = CreateBlockMap(64);
    blockMap->SubscribeStoredBlockUnreferenced(BIND([restoredStore] (TStoredBlockId blockId) {
        restoredStore->ReleaseBlock(blockId);
    }));
    auto dirtyId = TDirtyBlockId(1);
    blockMap->PutBlock(0, dirtyId);
    ASSERT_TRUE(blockMap->TryPutBlock(0, ToMappedBlockId(dirtyId), restoredBlockId));

    auto compactor = CreateBlockCompactor(
        restoredConfig->BlockCompactor,
        blockMap,
        restoredStore,
        ActionQueue_->GetInvoker(),
        Logger);
    compactor->Start();
    auto stopCompactorGuard = Finally([&] {
        compactor->Stop();
    });

    // The restored chunk drains and is forgotten (dropped from the store's tracking).
    WaitForPredicate(
        [&] {
            for (const auto& info : restoredStore->GetChunkInfos()) {
                if (info.ChunkIndex == restoredChunkIndex) {
                    return false;
                }
            }
            return true;
        },
        "restored chunk was not forgotten");

    // But it was never unstaged: the source store still holds it, so it survives in Cypress.
    EXPECT_TRUE(WaitFor(NativeClient_->NodeExists(chunkPath)).ValueOrThrow());

    // The survivor was relocated into a fresh chunk and reads back byte-for-byte.
    auto mappedBlockId = blockMap->FindBlock(0);
    ASSERT_TRUE(IsStoredMappedBlockId(mappedBlockId));
    auto relocatedBlockId = ToStoredBlockId(mappedBlockId);
    EXPECT_NE(ParseStoredBlockId(relocatedBlockId).ChunkIndex, restoredChunkIndex);
    ExpectBlocksEqual({blocks[survivorIndex]}, ReadBlocks(restoredStore, {relocatedBlockId}));
}

TEST_F(TBlockStoreTest, EmptyWrite)
{
    auto store = CreateStore(CreateConfig(), CreateOptions(4_KB));

    auto blockIds = WriteBlocks(store, /*blocks*/ {});
    EXPECT_TRUE(blockIds.empty());
}

TEST_F(TBlockStoreTest, InvalidBlockSizeIsRejected)
{
    constexpr i64 BlockSize = 4_KB;
    auto store = CreateStore(CreateConfig(), CreateOptions(BlockSize));

    auto badBlocks = MakeRandomBlocks(1, BlockSize + 1);
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(store->WriteBlocks(badBlocks))
            .ThrowOnError(),
        "Invalid block size");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
