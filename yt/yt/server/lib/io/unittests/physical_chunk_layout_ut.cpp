#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/test_framework/framework.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size, TRandomGenerator* generator)
{
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator->Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

std::vector<NChunkClient::TBlock> CreateBlocks(int count, TRandomGenerator* generator)
{
    std::vector<NChunkClient::TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        int size = 10 + generator->Generate<uint>() % 11;
        blocks.push_back(NChunkClient::TBlock(TSharedRef::FromString(GenerateRandomString(size, generator))));
    }

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhysicalChunkLayout, SerializeAndDeserializeBlocks)
{
    constexpr int BlocksCount = 100;

    auto chunkId = MakeRandomId(NCypressClient::EObjectType::Chunk, NObjectClient::TCellTag(0xf003));
    auto generator = TRandomGenerator(42);

    auto originalBlocks = CreateBlocks(BlocksCount, &generator);
    NChunkClient::NProto::TBlocksExt protoBlocksExt;

    auto writeRequest = SerializeBlocks(0, originalBlocks, protoBlocksExt);

    struct TMyTag {};
    auto blocksBlob = MergeRefsToRef<TMyTag>(std::move(writeRequest.Buffers));
    auto deserializedBlocks = DeserializeBlocks(
        blocksBlob,
        TBlockRange{
            .StartBlockIndex = 0,
            .EndBlockIndex = BlocksCount,
        },
        /*validateBlockChecksums*/ true,
        Format("%v", chunkId),
        New<TBlocksExt>(protoBlocksExt),
        /*dumpBrokenBlockCallback*/ {});

    EXPECT_EQ(BlocksCount, std::ssize(deserializedBlocks));
    for (int i = 0; i < BlocksCount; ++i) {
        EXPECT_EQ(originalBlocks[i].GetOrComputeChecksum(), deserializedBlocks[i].GetOrComputeChecksum());
    }
}

TEST(TPhysicalChunkLayout, SerializeAndDeserializeMeta)
{
    auto chunkId = MakeRandomId(NCypressClient::EObjectType::Chunk, NObjectClient::TCellTag(0xf003));

    NChunkClient::NProto::TBlocksExt protoBlocksExt;

    auto finalizedMeta = FinalizeChunkMeta(New<NChunkClient::TDeferredChunkMeta>(), protoBlocksExt);
    auto metaBlob = SerializeChunkMeta(chunkId, finalizedMeta);

    auto deserializedMeta = DeserializeMeta(
        metaBlob,
        Format("%v.meta", chunkId),
        chunkId,
        /*dumpBrokenMeta*/ {});

    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(*deserializedMeta, *finalizedMeta));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
