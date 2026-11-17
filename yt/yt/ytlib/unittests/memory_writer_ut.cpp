#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

class TMemoryWriterTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TMemoryWriterTest, BlocksAndFinalStatistics)
{
    auto chunkId = MakeRandomId(EObjectType::Chunk, TCellTag(1));
    auto writer = New<TMemoryWriter>(chunkId);
    EXPECT_EQ(chunkId, writer->GetChunkId());
    EXPECT_FALSE(writer->IsCloseDemanded());
    EXPECT_EQ(NErasure::ECodec::None, writer->GetErasureCodecId());
    WaitFor(writer->Open())
        .ThrowOnError();

    auto data = TSharedRef::FromString(std::string(4_KB, 'a'));
    auto compressed = NCompression::GetCodec(NCompression::ECodec::Lz4)->Compress(data);
    TBlock block(compressed);
    EXPECT_TRUE(writer->WriteBlock(/*options*/ {}, /*workloadDescriptor*/ {}, block));
    EXPECT_TRUE(writer->WriteBlocks(/*options*/ {}, /*workloadDescriptor*/ {}, {block, block}));
    WaitFor(writer->GetReadyEvent())
        .ThrowOnError();

    auto meta = New<TDeferredChunkMeta>();
    int finalizationCount = 0;
    meta->RegisterFinalizer([&] (TDeferredChunkMeta* finalizedMeta) {
        ++finalizationCount;
        EXPECT_EQ((std::vector<int>{0, 1, 2}), *finalizedMeta->BlockIndexMapping());
        TMiscExt misc;
        misc.set_row_count(7);
        misc.set_data_weight(12345);
        misc.set_uncompressed_data_size(3 * data.Size());
        misc.set_compressed_data_size(3 * compressed.Size());
        SetProtoExtension(finalizedMeta->mutable_extensions(), misc);
    });
    WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, meta))
        .ThrowOnError();

    EXPECT_EQ(1, finalizationCount);
    EXPECT_EQ(chunkId, writer->GetChunkId());
    ASSERT_EQ(3u, writer->GetBlocks().size());
    for (const auto& stored : writer->GetBlocks()) {
        EXPECT_EQ(compressed.Begin(), stored.Data.Begin());
        EXPECT_EQ(block.Checksum, stored.Checksum);
    }
    const auto& statistics = writer->GetDataStatistics();
    EXPECT_EQ(1, statistics.chunk_count());
    EXPECT_EQ(7, statistics.row_count());
    EXPECT_EQ(12345, statistics.data_weight());
    EXPECT_EQ(3 * data.Size(), statistics.uncompressed_data_size());
    EXPECT_EQ(3 * compressed.Size(), statistics.compressed_data_size());
    EXPECT_EQ(statistics.compressed_data_size(), statistics.regular_disk_space());
    EXPECT_EQ(statistics.regular_disk_space(), writer->GetChunkInfo().disk_space());
    EXPECT_TRUE(writer->GetWrittenChunkReplicasInfo().Replicas.empty());
    EXPECT_EQ(NHydra::NullRevision, writer->GetWrittenChunkReplicasInfo().ConfirmationRevision);
}

TEST_F(TMemoryWriterTest, RoundedCompressedSize)
{
    auto writer = New<TMemoryWriter>(MakeRandomId(EObjectType::Chunk, TCellTag(1)));
    WaitFor(writer->Open())
        .ThrowOnError();
    auto data = TSharedRef::FromString(std::string(120, 'x'));
    EXPECT_TRUE(writer->WriteBlock(/*options*/ {}, /*workloadDescriptor*/ {}, TBlock(data)));
    auto meta = New<TDeferredChunkMeta>();
    TMiscExt misc;
    misc.set_row_count(3);
    misc.set_data_weight(42);
    misc.set_uncompressed_data_size(1000);
    misc.set_compressed_data_size(119);
    SetProtoExtension(meta->mutable_extensions(), misc);
    WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, meta))
        .ThrowOnError();
    EXPECT_EQ(119, writer->GetDataStatistics().compressed_data_size());
    EXPECT_EQ(120, writer->GetDataStatistics().regular_disk_space());
}

TEST_F(TMemoryWriterTest, InvalidIdentity)
{
    EXPECT_THROW_WITH_SUBSTRING(
        New<TMemoryWriter>(MakeRandomId(EObjectType::Table, TCellTag(1))),
        "ordinary chunk ID");
}

TEST_F(TMemoryWriterTest, MissingMiscMetadata)
{
    auto writer = New<TMemoryWriter>();
    EXPECT_EQ(NullChunkId, writer->GetChunkId());
    EXPECT_FALSE(writer->IsCloseDemanded());
    WaitFor(writer->Open())
        .ThrowOnError();
    TBlock block(TSharedRef::FromString(std::string("abc")));
    EXPECT_TRUE(writer->WriteBlocks(/*options*/ {}, /*workloadDescriptor*/ {}, {block, block}));
    WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, New<TDeferredChunkMeta>()))
        .ThrowOnError();

    const auto& statistics = writer->GetDataStatistics();
    EXPECT_EQ(1, statistics.chunk_count());
    EXPECT_EQ(0, statistics.row_count());
    EXPECT_EQ(0, statistics.data_weight());
    EXPECT_EQ(0, statistics.uncompressed_data_size());
    EXPECT_EQ(6, statistics.compressed_data_size());
    EXPECT_EQ(6, statistics.regular_disk_space());
    EXPECT_EQ(6, writer->GetChunkInfo().disk_space());
    EXPECT_TRUE(writer->GetWrittenChunkReplicasInfo().Replicas.empty());
}

TEST_F(TMemoryWriterTest, NullMetadata)
{
    auto writer = New<TMemoryWriter>();
    WaitFor(writer->Open())
        .ThrowOnError();
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, nullptr))
            .ThrowOnError(),
        "requires chunk metadata");
}

TEST_PI(TMemoryWriterTest, InvalidStatistics, ::testing::Values(false, true), bool)
{
    auto erasure = GetParam();
    auto writer = New<TMemoryWriter>();
    WaitFor(writer->Open())
        .ThrowOnError();
    auto meta = New<TDeferredChunkMeta>();
    TMiscExt misc;
    if (erasure) {
        misc.set_erasure_codec(static_cast<int>(NErasure::ECodec::ReedSolomon_3_3));
    } else {
        misc.set_row_count(-1);
    }

    SetProtoExtension(meta->mutable_extensions(), misc);
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, meta))
            .ThrowOnError(),
        "nonnegative non-erasure chunk statistics");
}

TEST_F(TMemoryWriterTest, InconsistentMetadata)
{
    auto writer = New<TMemoryWriter>(MakeRandomId(EObjectType::Chunk, TCellTag(1)));
    WaitFor(writer->Open())
        .ThrowOnError();
    auto meta = New<TDeferredChunkMeta>();
    TMiscExt misc;
    misc.set_row_count(1);
    misc.set_data_weight(1);
    misc.set_uncompressed_data_size(1);
    misc.set_compressed_data_size(1);
    SetProtoExtension(meta->mutable_extensions(), misc);
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(writer->Close(/*options*/ {}, /*workloadDescriptor*/ {}, meta))
            .ThrowOnError(),
        "block size disagrees");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
