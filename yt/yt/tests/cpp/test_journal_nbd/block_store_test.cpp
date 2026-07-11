#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/server/lib/nbd/journal/block_store.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/block_store_helpers.h>
#include <yt/yt/server/lib/nbd/journal/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/random/random.h>

#include <set>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NApi;
using namespace NConcurrency;
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

////////////////////////////////////////////////////////////////////////////////

class TBlockStoreTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    TActionQueuePtr ActionQueue_;
    ITransactionPtr Transaction_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        ActionQueue_ = New<TActionQueue>("BlockStoreTest");
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
    }

    void TearDown() override
    {
        if (Transaction_) {
            YT_UNUSED_FUTURE(Transaction_->Abort());
            Transaction_.Reset();
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
            ActionQueue_.Reset();
        }
    }

    static TJournalBlockDeviceConfigPtr CreateConfig(i64 blockSize)
    {
        auto config = New<TJournalBlockDeviceConfig>();
        config->BlockSize = blockSize;
        return config;
    }

    static TJournalBlockDeviceOptionsPtr CreateOptions()
    {
        auto options = New<TJournalBlockDeviceOptions>();
        options->Account = NSecurityClient::TmpAccountName;
        options->MediumName = NChunkClient::DefaultStoreMediumName;
        return options;
    }

    IBlockStorePtr CreateStore(TJournalBlockDeviceConfigPtr config, TJournalBlockDeviceOptionsPtr options)
    {
        // The store only uses the block size from the geometry; block count is irrelevant here.
        TBlockDeviceGeometry geometry{.BlockSize = config->BlockSize, .BlockCount = 1};
        return CreateJournalBlockStore(
            config->BlockStore,
            geometry,
            std::move(options),
            NativeClient_,
            Transaction_->GetId(),
            NChunkClient::NullChunkListId,
            ActionQueue_->GetInvoker(),
            Logger);
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
        return WaitFor(store->ReadBlocks(TRange(blockIds), /*options*/ {}))
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
    constexpr i64 BlockSize = 4096;
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

    auto blocks = MakeRandomBlocks(16, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    ASSERT_EQ(std::ssize(blockIds), std::ssize(blocks));

    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
}

TEST_F(TBlockStoreTest, ReadSubsetInArbitraryOrder)
{
    constexpr i64 BlockSize = 1024;
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

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
    constexpr i64 BlockSize = 4096;
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

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
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

    auto blocks = MakeRandomBlocks(2500, BlockSize);
    auto blockIds = WriteBlocks(store, blocks);
    ASSERT_EQ(std::ssize(blockIds), std::ssize(blocks));

    ExpectBlocksEqual(blocks, ReadBlocks(store, blockIds));
}

TEST_F(TBlockStoreTest, MultipleWriteBatches)
{
    constexpr i64 BlockSize = 4096;
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

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
    constexpr i64 BlockSize = 4096;
    constexpr int BatchCount = 4;
    constexpr int BlocksPerBatch = 2;

    auto config = CreateConfig(BlockSize);
    config->BlockStore->WriteParallelism = 1;
    config->BlockStore->MaxChunkDataSize = BlocksPerBatch * BlockSize;
    config->BlockStore->ChunkMaintenancePeriod = TDuration::MilliSeconds(100);
    auto store = CreateStore(config, CreateOptions());

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

TEST_F(TBlockStoreTest, EmptyWrite)
{
    auto store = CreateStore(CreateConfig(4096), CreateOptions());

    auto blockIds = WriteBlocks(store, /*blocks*/ {});
    EXPECT_TRUE(blockIds.empty());
}

TEST_F(TBlockStoreTest, InvalidBlockSizeIsRejected)
{
    constexpr i64 BlockSize = 4096;
    auto store = CreateStore(CreateConfig(BlockSize), CreateOptions());

    auto badBlocks = MakeRandomBlocks(1, BlockSize + 1);
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(store->WriteBlocks(badBlocks))
            .ThrowOnError(),
        "Invalid block size");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
