#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/server/lib/nbd/journal/journal_block_device.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/public.h>

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/random/random.h>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;

using NCppTests::TApiTestBase;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("NbdDeviceTest");

constexpr i64 BlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

//! Exercises the journal-backed IBlockDevice end-to-end against a real cluster: the same read/write
//! surface the NBD server presents to jobs (empty/dirty/clean block resolution, flushing).
class TJournalBlockDeviceTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    ITransactionPtr Transaction_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
    }

    void TearDown() override
    {
        if (Transaction_) {
            YT_UNUSED_FUTURE(Transaction_->Abort());
            Transaction_.Reset();
        }
    }

    IBlockDevicePtr CreateDevice(i64 size)
    {
        auto config = New<TJournalBlockDeviceConfig>();
        config->BlockSize = BlockSize;
        config->BlockStore->ReplicationFactor = 3;
        // Flush eagerly so the clean (flushed-to-store) read path is exercised quickly.
        config->BlockFlusher->FlushPeriod = TDuration::MilliSeconds(50);
        config->BlockFlusher->DirtyFractionThreshold = 0.0;

        auto options = New<TJournalBlockDeviceOptions>();
        options->Size = size;
        options->Account = NSecurityClient::TmpAccountName;
        options->MediumName = NChunkClient::DefaultStoreMediumName;

        auto device = CreateJournalBlockDevice(
            "test-device",
            std::move(config),
            std::move(options),
            Transaction_->GetId(),
            NChunkClient::NullChunkListId,
            NativeClient_,
            Logger);
        WaitFor(device->Initialize())
            .ThrowOnError();
        return device;
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

    TSharedRef Read(const IBlockDevicePtr& device, i64 offset, i64 length)
    {
        return WaitFor(device->Read(offset, length))
            .ValueOrThrow()
            .Data;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJournalBlockDeviceTest, WriteReadRoundtrip)
{
    auto device = CreateDevice(16 * BlockSize);

    auto block = MakeRandomBlock(BlockSize);
    Write(device, 3 * BlockSize, block);

    EXPECT_TRUE(TRef::AreBitwiseEqual(Read(device, 3 * BlockSize, BlockSize), block));

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_F(TJournalBlockDeviceTest, UnwrittenBlocksAreZero)
{
    auto device = CreateDevice(16 * BlockSize);

    auto read = Read(device, 5 * BlockSize, 2 * BlockSize);
    ASSERT_EQ(std::ssize(read), 2 * BlockSize);
    ExpectZero(read);

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_F(TJournalBlockDeviceTest, OverwriteReturnsNewest)
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

TEST_F(TJournalBlockDeviceTest, ReadFromCleanPathAfterFlush)
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

TEST_F(TJournalBlockDeviceTest, MultiBlockReadSpansStates)
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

TEST_F(TJournalBlockDeviceTest, RejectsUnalignedRead)
{
    // The device requires block-aligned I/O: the client is told the block size and aligns to it, so
    // an unaligned request is a protocol violation and must be rejected, not emulated.
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Read(BlockSize + 100, 50)).IsOK());
    EXPECT_FALSE(WaitFor(device->Read(0, BlockSize + 1)).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_F(TJournalBlockDeviceTest, RejectsUnalignedWrite)
{
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Write(300, MakeRandomBlock(200))).IsOK());
    EXPECT_FALSE(WaitFor(device->Write(0, MakeRandomBlock(BlockSize + 1))).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_F(TJournalBlockDeviceTest, RejectsOutOfBoundsRead)
{
    // A block-aligned request that runs past the end of the device must be rejected, not read out of
    // range (the NBD server guards this too, but the device defends itself).
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Read(16 * BlockSize, BlockSize)).IsOK());      // starts at the end
    EXPECT_FALSE(WaitFor(device->Read(15 * BlockSize, 2 * BlockSize)).IsOK());  // spans past the end

    WaitFor(device->Finalize())
        .ThrowOnError();
}

TEST_F(TJournalBlockDeviceTest, RejectsOutOfBoundsWrite)
{
    auto device = CreateDevice(16 * BlockSize);

    EXPECT_FALSE(WaitFor(device->Write(16 * BlockSize, MakeRandomBlock(BlockSize))).IsOK());
    EXPECT_FALSE(WaitFor(device->Write(15 * BlockSize, MakeRandomBlock(2 * BlockSize))).IsOK());

    WaitFor(device->Finalize())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
