#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/server/lib/nbd/journal/journal_block_device.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/public.h>

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/string/enum.h>

#include <util/random/random.h>

#include <atomic>
#include <thread>

#include <map>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

using NCppTests::TApiTestBase;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("NbdDeviceTest");

constexpr i64 BlockSize = 4_KB;

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

DEFINE_ENUM(EDeviceChunkListKind,
    (Null)
    (Scratch)
);

class TJournalBlockDeviceTest
    : public TApiTestBase
    , public ::testing::WithParamInterface<EDeviceChunkListKind>
{
protected:
    NNative::IClientPtr NativeClient_;
    ITransactionPtr Transaction_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        Transaction_ = StartDeviceTransaction(NativeClient_);
    }

    void TearDown() override
    {
        if (Transaction_) {
            YT_UNUSED_FUTURE(Transaction_->Abort());
            Transaction_.Reset();
        }
    }

    //! The chunk list the default CreateDevice backs a device with, per the test parameter: none or a
    //! fresh scratch chunk list.
    TChunkListId GetDeviceChunkList()
    {
        return GetParam() == EDeviceChunkListKind::Scratch
            ? CreateScratchChunkList()
            : NullChunkListId;
    }

    IBlockDevicePtr CreateDevice(
        i64 deviceSize,
        std::optional<NYPath::TYPath> snapshotPath = {})
    {
        return CreateDevice(deviceSize, GetDeviceChunkList(), std::move(snapshotPath));
    }

    static NYPath::TYPath MakeSnapshotPath()
    {
        return "//tmp/" + GenerateRandomFileName("nbd_snapshot");
    }

    IBlockDevicePtr CreateDevice(
        i64 deviceSize,
        TChunkListId chunkListId,
        std::optional<NYPath::TYPath> snapshotPath = {})
    {
        auto config = New<TJournalBlockDeviceConfig>();
        // Flush eagerly so the clean (flushed-to-store) read path is exercised quickly.
        config->BlockFlusher->FlushPeriod = TDuration::MilliSeconds(50);
        config->BlockFlusher->DirtyFractionThreshold = 0.0;
        // One writable chunk, so writes do not scatter across the fan-out at random. Tests that count
        // a chunk's superseded blocks need every record in the same chunk: a chunk left holding only
        // superseded blocks is not referenced by the snapshot, so its records vanish from the counts.
        config->BlockStore->WriteParallelism = 1;

        TDeviceCreationDescriptor creationDescriptor;
        if (snapshotPath) {
            creationDescriptor = TRestoredDeviceCreationDescriptor{.SnapshotPath = *snapshotPath};
        } else {
            auto options = New<TJournalBlockDeviceOptions>();
            options->DeviceSize = deviceSize;
            options->BlockSize = BlockSize;
            options->Account = NSecurityClient::TmpAccountName;
            options->MediumName = NChunkClient::DefaultStoreMediumName;
            creationDescriptor = TFreshDeviceCreationDescriptor{.Options = std::move(options)};
        }

        auto device = WaitFor(CreateJournalBlockDevice(
            NativeClient_,
            "test-device",
            std::move(config),
            std::move(creationDescriptor),
            Transaction_->GetId(),
            chunkListId,
            Logger))
            .ValueOrThrow();
        WaitFor(device->Initialize())
            .ThrowOnError();
        return device;
    }

    int GetChunkListChildCount(TChunkListId chunkListId)
    {
        auto yson = WaitFor(NativeClient_->GetNode(FromObjectId(chunkListId) + "/@child_count"))
            .ValueOrThrow();
        return ConvertTo<int>(yson);
    }

    TChunkListId CreateScratchChunkList()
    {
        auto cellTag = CellTagFromId(Transaction_->GetId());
        TChunkServiceProxy proxy(NativeClient_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            cellTag));
        auto req = proxy.CreateChunkLists();
        GenerateMutationId(req);
        ToProto(req->mutable_transaction_id(), Transaction_->GetId());
        req->set_kind(ToProto(EChunkListKind::Scratch));
        req->set_count(1);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        return FromProto<TChunkListId>(rsp->chunk_list_ids(0));
    }

    struct TBlockTag
    { };

    static TSharedRef MakeRandomBlock(i64 size)
    {
        auto ref = TSharedMutableRef::Allocate<TBlockTag>(size);
        for (i64 index = 0; index < size; ++index) {
            ref[index] = static_cast<char>(RandomNumber<ui32>(256));
        }
        return ref;
    }

    static void ExpectZero(TRef data)
    {
        for (i64 index = 0; index < std::ssize(data); ++index) {
            EXPECT_EQ(static_cast<ui8>(data[index]), 0u) << "non-zero byte at " << index;
        }
    }

    void Write(const IBlockDevicePtr& device, i64 offset, const TSharedRef& data)
    {
        WaitFor(device->Write(offset, data))
            .ThrowOnError();
    }

    void Trim(const IBlockDevicePtr& device, i64 offset, i64 length)
    {
        WaitFor(device->Trim(offset, length))
            .ThrowOnError();
    }

    TSharedRef Read(const IBlockDevicePtr& device, i64 offset, i64 length)
    {
        return WaitFor(device->Read(offset, length))
            .ValueOrThrow()
            .Data;
    }

    void SaveSnapshot(const IJournalBlockDevicePtr& journalDevice, const NYPath::TYPath& path)
    {
        auto externalCellTag = journalDevice->GetExternalCellTag();

        auto transaction = WaitFor(NativeClient_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        // The geometry is persisted into @device_params, so a device restored from this snapshot
        // reconstructs it; take it from the device being snapshotted.
        auto deviceOptions = New<TJournalBlockDeviceOptions>();
        deviceOptions->DeviceSize = journalDevice->GetTotalSize();
        deviceOptions->BlockSize = journalDevice->GetBlockSize();
        deviceOptions->Account = NSecurityClient::TmpAccountName;
        deviceOptions->MediumName = NChunkClient::DefaultStoreMediumName;

        TCreateSnapshotTableOptions createOptions;
        createOptions.TransactionId = transaction->GetId();
        WaitFor(CreateSnapshotTable(NativeClient_, path, externalCellTag, deviceOptions, createOptions))
            .ThrowOnError();

        TFetchSnapshotSaveTableSpecOptions fetchOptions;
        fetchOptions.TransactionId = transaction->GetId();
        auto saveSpec = WaitFor(FetchSnapshotSaveSpec(NativeClient_, path, fetchOptions))
            .ValueOrThrow();

        WaitFor(journalDevice->SaveSnapshot(saveSpec))
            .ThrowOnError();

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    std::map<i64, std::string> ReadSnapshotTable(const NYPath::TYPath& path)
    {
        auto reader = WaitFor(Client_->CreateTableReader(NYPath::TRichYPath(path)))
            .ValueOrThrow();
        auto nameTable = reader->GetNameTable();

        std::map<i64, std::string> result;
        while (auto batch = NTableClient::ReadRowBatch(reader)) {
            for (auto row : batch->MaterializeRows()) {
                i64 blockIndex = 0;
                std::string payload;
                for (const auto& value : row) {
                    auto name = nameTable->GetName(value.Id);
                    if (name == "block_index") {
                        blockIndex = value.Data.Int64;
                    } else if (name == "payload") {
                        payload = std::string(value.AsStringBuf());
                    }
                }
                result[blockIndex] = payload;
            }
        }
        return result;
    }

    struct TRestoredChunkStats
    {
        int RestoredChunkCount = 0;
        i64 TotalWrittenBlockCount = 0;
        i64 TotalReferencedBlockCount = 0;
    };

    //! Aggregates the restored (snapshot-owned) chunks' block counts from the device orchid.
    TRestoredChunkStats GetRestoredChunkStats(const IBlockDevicePtr& device)
    {
        auto yson = WaitFor(NYTree::AsyncYPathGet(device->GetOrchidService(), "/chunks"))
            .ValueOrThrow();

        TRestoredChunkStats stats;
        for (const auto& [chunkId, node] : NYTree::ConvertToNode(yson)->AsMap()->GetChildren()) {
            auto map = node->AsMap();
            if (map->GetChildValueOrThrow<bool>("restored")) {
                ++stats.RestoredChunkCount;
                stats.TotalWrittenBlockCount += map->GetChildValueOrThrow<i64>("written_block_count");
                stats.TotalReferencedBlockCount += map->GetChildValueOrThrow<i64>("referenced_block_count");
            }
        }
        return stats;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TJournalBlockDeviceTest, WriteReadRoundtrip)
{
    auto device = CreateDevice(16 * BlockSize);

    auto block = MakeRandomBlock(BlockSize);
    Write(device, 3 * BlockSize, block);

    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 3 * BlockSize, BlockSize), block));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, UnwrittenBlocksAreZero)
{
    auto device = CreateDevice(16 * BlockSize);

    auto read = Read(device, 5 * BlockSize, 2 * BlockSize);
    ASSERT_EQ(std::ssize(read), 2 * BlockSize);
    ExpectZero(read);

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, OverwriteReturnsNewest)
{
    auto device = CreateDevice(16 * BlockSize);

    auto first = MakeRandomBlock(BlockSize);
    auto second = MakeRandomBlock(BlockSize);
    Write(device, BlockSize, first);
    Write(device, BlockSize, second);

    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, BlockSize, BlockSize), second));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, ReadFromCleanPathAfterFlush)
{
    constexpr int BlockCount = 16;
    auto device = CreateDevice(64 * BlockSize);

    std::vector<TSharedRef> blocks;
    for (int index = 0; index < BlockCount; ++index) {
        blocks.push_back(MakeRandomBlock(BlockSize));
        Write(device, index * BlockSize, blocks.back());
    }

    // Let the flusher move the dirty blocks to the store, then read them from the clean path.
    Sleep(TDuration::Seconds(2));
    WaitFor(device->Flush())
        .ThrowOnError();

    for (int index = 0; index < BlockCount; ++index) {
        EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, index * BlockSize, BlockSize), blocks[index]))
            << "mismatch at block " << index;
    }

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, MultiBlockReadSpansStates)
{
    auto device = CreateDevice(16 * BlockSize);

    // Write blocks 0 and 2; leave 1 and 3 empty. A single read must stitch clean/dirty/empty.
    auto block0 = MakeRandomBlock(BlockSize);
    auto block2 = MakeRandomBlock(BlockSize);
    Write(device, 0, block0);
    Write(device, 2 * BlockSize, block2);

    auto read = Read(device, 0, 4 * BlockSize);
    ASSERT_EQ(std::ssize(read), 4 * BlockSize);
    EXPECT_TRUE(TRef::AreBitwiseEqual(read.Slice(0, BlockSize), block0));
    ExpectZero(read.Slice(BlockSize, 2 * BlockSize));
    EXPECT_TRUE(TRef::AreBitwiseEqual(read.Slice(2 * BlockSize, 3 * BlockSize), block2));
    ExpectZero(read.Slice(3 * BlockSize, 4 * BlockSize));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RejectsUnalignedRead)
{
    // The device requires block-aligned I/O: the client is told the block size and aligns to it, so
    // an unaligned request is a protocol violation and must be rejected, not emulated.
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Read(BlockSize + 100, 50)).IsOK());
    EXPECT_FALSE(WaitFor(device->Read(0, BlockSize + 1)).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RejectsUnalignedWrite)
{
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Write(300, MakeRandomBlock(200))).IsOK());
    EXPECT_FALSE(WaitFor(device->Write(0, MakeRandomBlock(BlockSize + 1))).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RejectsOutOfBoundsRead)
{
    // A block-aligned request that runs past the end of the device must be rejected, not read out of
    // range (the NBD server guards this too, but the device defends itself).
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Read(16 * BlockSize, BlockSize)).IsOK());      // starts at the end
    EXPECT_FALSE(WaitFor(device->Read(15 * BlockSize, 2 * BlockSize)).IsOK());  // spans past the end

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RejectsOutOfBoundsWrite)
{
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Write(16 * BlockSize, MakeRandomBlock(BlockSize))).IsOK());
    EXPECT_FALSE(WaitFor(device->Write(15 * BlockSize, MakeRandomBlock(2 * BlockSize))).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, TrimZeroesBlocks)
{
    auto device = CreateDevice(16 * BlockSize);
    EXPECT_TRUE(device->IsTrimSupported());

    for (int blockIndex = 0; blockIndex < 4; ++blockIndex) {
        Write(device, blockIndex * BlockSize, MakeRandomBlock(BlockSize));
    }

    auto block0 = Read(device, 0, BlockSize);
    auto block3 = Read(device, 3 * BlockSize, BlockSize);
    Trim(device, BlockSize, 2 * BlockSize);

    ExpectZero(Read(device, BlockSize, 2 * BlockSize));
    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 0, BlockSize), block0));
    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 3 * BlockSize, BlockSize), block3));

    auto rewritten = MakeRandomBlock(BlockSize);
    Write(device, BlockSize, rewritten);
    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, BlockSize, BlockSize), rewritten));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, TrimAfterFlush)
{
    auto device = CreateDevice(16 * BlockSize);
    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    // Let the blocks reach the store first, so the trim discards stored (clean) payloads rather than
    // dirty ones.
    for (int blockIndex = 0; blockIndex < 4; ++blockIndex) {
        Write(device, blockIndex * BlockSize, MakeRandomBlock(BlockSize));
    }
    WaitFor(journalDevice->FlushBlocks())
        .ThrowOnError();

    Trim(device, 0, 4 * BlockSize);
    ExpectZero(Read(device, 0, 4 * BlockSize));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, TrimSparesPartiallyCoveredBlocks)
{
    auto device = CreateDevice(16 * BlockSize);

    for (int blockIndex = 0; blockIndex < 3; ++blockIndex) {
        Write(device, blockIndex * BlockSize, MakeRandomBlock(BlockSize));
    }
    auto block0 = Read(device, 0, BlockSize);
    auto block2 = Read(device, 2 * BlockSize, BlockSize);

    // The range covers only the tail of block 0 and the head of block 2, so just block 1 is discarded:
    // a trim may discard less than asked, never more.
    Trim(device, BlockSize / 2, 2 * BlockSize);

    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 0, BlockSize), block0));
    ExpectZero(Read(device, BlockSize, BlockSize));
    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 2 * BlockSize, BlockSize), block2));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RejectsOutOfBoundsTrim)
{
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Trim(16 * BlockSize, BlockSize)).IsOK());
    EXPECT_FALSE(WaitFor(device->Trim(15 * BlockSize, 2 * BlockSize)).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, TrimmedBlocksAreAbsentFromSnapshot)
{
    auto device = CreateDevice(64 * BlockSize);

    std::map<i64, std::string> written;
    for (int blockIndex : {0, 1, 2, 5, 9}) {
        auto block = MakeRandomBlock(BlockSize);
        Write(device, blockIndex * BlockSize, block);
        written[blockIndex] = std::string(block.ToStringBuf());
    }

    Trim(device, BlockSize, 2 * BlockSize);
    written.erase(1);
    written.erase(2);

    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    auto path = MakeSnapshotPath();
    SaveSnapshot(journalDevice, path);
    EXPECT_EQ(ReadSnapshotTable(path), written);

    WaitFor(device->Finalize())
        .ThrowOnError();
    auto restored = CreateDevice(64 * BlockSize, path);
    ExpectZero(Read(restored, BlockSize, 2 * BlockSize));

    WaitFor(restored->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, SaveSnapshot)
{
    auto device = CreateDevice(64 * BlockSize);

    // Write a mix of blocks -- some contiguous, some sparse -- keyed by block index.
    std::map<i64, std::string> written;
    for (int blockIndex : {0, 1, 2, 5, 9, 15}) {
        auto block = MakeRandomBlock(BlockSize);
        Write(device, blockIndex * BlockSize, block);
        written[blockIndex] = std::string(block.ToStringBuf());
    }

    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    auto path = MakeSnapshotPath();
    SaveSnapshot(journalDevice, path);

    // Every written block's payload survives the flush -> seal -> hunk round-trip; empty blocks are
    // absent from the table.
    EXPECT_EQ(ReadSnapshotTable(path), written);

    // The snapshot must be self-contained: aborting the device's transaction (which staged the
    // journal chunks) must not break the table -- its hunk chunks are now owned by the table.
    WaitFor(Transaction_->Abort())
        .ThrowOnError();
    Transaction_.Reset();
    EXPECT_EQ(ReadSnapshotTable(path), written);

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, SaveSnapshotOfEmptyDevice)
{
    auto device = CreateDevice(16 * BlockSize);

    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    auto path = MakeSnapshotPath();
    SaveSnapshot(journalDevice, path);

    EXPECT_TRUE(ReadSnapshotTable(path).empty());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RestoreFromSnapshot)
{
    // Populate a device and snapshot it.
    auto device = CreateDevice(64 * BlockSize);
    std::map<i64, TSharedRef> written;
    for (int blockIndex : {0, 1, 2, 5, 9, 15}) {
        auto block = MakeRandomBlock(BlockSize);
        Write(device, blockIndex * BlockSize, block);
        written[blockIndex] = block;
    }

    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    auto path = MakeSnapshotPath();
    SaveSnapshot(journalDevice, path);
    WaitFor(device->Finalize())
        .ThrowOnError();

    // Restore a fresh device from the snapshot; its journal chunks are attached to its chunk list
    // for liveness.
    auto restored = CreateDevice(64 * BlockSize, path);

    // Every written block reads back byte-for-byte, without ever having fetched the payloads during
    // restore; unwritten blocks read back as zeros.
    for (const auto& [blockIndex, block] : written) {
        EXPECT_TRUE(TRef::AreBitwiseEqual(Read(restored, blockIndex * BlockSize, BlockSize), block))
            << "mismatch at block " << blockIndex;
    }
    ExpectZero(Read(restored, 3 * BlockSize, BlockSize));

    // The restored chunks recover their written-block counts from the master (no pre-snapshot overwrites
    // here, so written == the live block count).
    auto stats = GetRestoredChunkStats(restored);
    EXPECT_GT(stats.RestoredChunkCount, 0);
    EXPECT_EQ(stats.TotalReferencedBlockCount, std::ssize(written));
    EXPECT_EQ(stats.TotalWrittenBlockCount, std::ssize(written));

    WaitFor(restored->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, RestoreRecoversSupersededBlockCount)
{
    auto device = CreateDevice(64 * BlockSize);
    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    // Write four blocks and let them reach the journal.
    for (int blockIndex : {0, 1, 2, 3}) {
        Write(device, blockIndex * BlockSize, MakeRandomBlock(BlockSize));
    }
    WaitFor(journalDevice->FlushBlocks())
        .ThrowOnError();

    // Overwrite three of them. The superseded versions are already in the append-only journal, so they
    // become garbage there -- counted in written but not referenced.
    for (int blockIndex : {0, 1, 2}) {
        Write(device, blockIndex * BlockSize, MakeRandomBlock(BlockSize));
    }
    WaitFor(journalDevice->FlushBlocks())
        .ThrowOnError();

    auto path = MakeSnapshotPath();
    SaveSnapshot(journalDevice, path);
    WaitFor(device->Finalize())
        .ThrowOnError();

    auto restored = CreateDevice(64 * BlockSize, path);

    // Four blocks survive into the snapshot; seven were ever written to it.
    auto stats = GetRestoredChunkStats(restored);
    EXPECT_EQ(stats.TotalReferencedBlockCount, 4);
    EXPECT_EQ(stats.TotalWrittenBlockCount, 7);
    EXPECT_GT(stats.TotalWrittenBlockCount, stats.TotalReferencedBlockCount);

    WaitFor(restored->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, NewChunksAttachedToChunkList)
{
    auto chunkListId = CreateScratchChunkList();
    auto device = CreateDevice(16 * BlockSize, chunkListId);

    // A write, flushed to the store, lands in a journal chunk that must have been attached to the
    // device's chunk list at creation.
    Write(device, 0, MakeRandomBlock(BlockSize));
    Sleep(TDuration::Seconds(2));
    WaitFor(device->Flush())
        .ThrowOnError();

    EXPECT_GT(GetChunkListChildCount(chunkListId), 0);

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_P(TJournalBlockDeviceTest, SnapshotWithConcurrentWrites)
{
    auto device = CreateDevice(64 * BlockSize);
    auto journalDevice = DynamicPointerCast<IJournalBlockDevice>(device);
    ASSERT_TRUE(journalDevice);

    // Stable region [0, 16): written once, never touched again.
    std::map<i64, TSharedRef> stable;
    for (int blockIndex = 0; blockIndex < 16; ++blockIndex) {
        auto block = MakeRandomBlock(BlockSize);
        Write(device, blockIndex * BlockSize, block);
        stable[blockIndex] = block;
    }

    auto path = MakeSnapshotPath();

    // Churn region [16, 32): hammered by a background thread throughout the snapshot, so the dirty
    // pool never quiesces -- the save must still complete (a point-in-time cut, not a drain-to-zero).
    // Scoped so the thread is stopped and joined before Finalize, even if the snapshot throws: a
    // joinable std::thread's destructor would std::terminate and mask the real failure.
    {
        std::atomic<bool> stop = false;
        std::thread churn([&] {
            while (!stop.load()) {
                int blockIndex = 16 + RandomNumber<ui32>(16);
                YT_UNUSED_FUTURE(device->Write(blockIndex * BlockSize, MakeRandomBlock(BlockSize)));
                Sleep(TDuration::MilliSeconds(1));
            }
        });
        auto joinChurn = Finally([&] {
            stop.store(true);
            churn.join();
        });

        SaveSnapshot(journalDevice, path);
    }

    WaitFor(device->Finalize())
        .ThrowOnError();

    // The stable region, fully written before the snapshot, must be captured exactly.
    auto restored = CreateDevice(64 * BlockSize, path);
    for (const auto& [blockIndex, block] : stable) {
        EXPECT_TRUE(TRef::AreBitwiseEqual(Read(restored, blockIndex * BlockSize, BlockSize), block))
            << "stable block " << blockIndex << " mismatch";
    }
    WaitFor(restored->Finalize())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    ChunkListKind,
    TJournalBlockDeviceTest,
    ::testing::Values(EDeviceChunkListKind::Null, EDeviceChunkListKind::Scratch),
    [] (const testing::TestParamInfo<EDeviceChunkListKind>& info) {
        return FormatEnum(info.param);
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
