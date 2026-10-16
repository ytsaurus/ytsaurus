#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChunkClient {
namespace {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TInputChunkTest, SerializesOrdinaryChunkAsTable)
{
    auto inputChunk = New<TInputChunk>();
    inputChunk->SetChunkId(MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
    inputChunk->SetChunkFormat(EChunkFormat::TableUnversionedSchemalessHorizontal);

    NProto::TChunkSpec chunkSpec;
    ToProto(&chunkSpec, inputChunk);

    EXPECT_TRUE(chunkSpec.IsInitialized());
    EXPECT_TRUE(chunkSpec.chunk_meta().has_extensions());
    EXPECT_EQ(EChunkType::Table, NYT::FromProto<EChunkType>(chunkSpec.chunk_meta().type()));
}

TEST(TInputChunkTest, SerializesDistributedJournalChunkAsJournal)
{
    auto inputChunk = New<TInputChunk>();
    inputChunk->SetChunkId(MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42)));
    inputChunk->SetChunkFormat(EChunkFormat::JournalDistributed);

    NProto::TChunkSpec chunkSpec;
    ToProto(&chunkSpec, inputChunk);

    EXPECT_EQ(EChunkType::Journal, NYT::FromProto<EChunkType>(chunkSpec.chunk_meta().type()));
    EXPECT_EQ(
        EChunkFormat::JournalDistributed,
        NYT::FromProto<EChunkFormat>(chunkSpec.chunk_meta().format()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
