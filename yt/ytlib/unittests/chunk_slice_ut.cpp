#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_slice.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/yson/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Function is defined here due to ADL.
void PrintTo(const NChunkClient::TChunkSlice& slice, ::std::ostream* os)
{
    *os << "chunk slice with " << slice.GetRowCount() << " rows";
}

namespace NChunkClient {
namespace {

using namespace NObjectClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadLimit;

using testing::PrintToString;

////////////////////////////////////////////////////////////////////////////////

MATCHER_P(HasRowCount, rowCount, "has row count " + std::string(negation ? "not " : "") + PrintToString(rowCount))
{
    return arg.GetRowCount() == rowCount;
}

MATCHER_P(HasLowerLimit, lowerLimit, "has lower limit " + std::string(negation ? "not " : "") + lowerLimit)
{
    auto actualLimitAsNode = ConvertToNode(arg.LowerLimit());
    auto actualLimitAsText = ConvertToYsonString(actualLimitAsNode, EYsonFormat::Text).GetData();

    auto expectedLimitAsNode = ConvertToNode(TYsonString(lowerLimit));

    *result_listener << "where HasLowerLimit(R\"_(" << actualLimitAsText.c_str() << ")_\")";

    return AreNodesEqual(actualLimitAsNode, expectedLimitAsNode);
}

MATCHER_P(HasUpperLimit, upperLimit, "has upper limit " + std::string(negation ? "not " : "") + upperLimit)
{
    auto actualLimitAsNode = ConvertToNode(arg.UpperLimit());
    auto actualLimitAsText = ConvertToYsonString(actualLimitAsNode, EYsonFormat::Text).GetData();

    auto expectedLimitAsNode = ConvertToNode(TYsonString(upperLimit));

    *result_listener << "where HasUpperLimit(R\"_(" << actualLimitAsText.c_str() << ")_\")";

    return AreNodesEqual(actualLimitAsNode, expectedLimitAsNode);
}

class TChunkSliceTest
    : public ::testing::Test
{
public:
    static const bool DoSliceByKeys = true;
    static const bool DoSliceByRows = false;

    TChunkSliceTest()
    {
        EmptyChunk_ = CreateChunkSpec(
            ETableChunkFormat::SchemalessHorizontal,
            1, // keyRepetitions
            0); // chunkRows

        OneKeyChunk_ = CreateChunkSpec(
            ETableChunkFormat::SchemalessHorizontal,
            300); // keyRepetitions

        TwoKeyChunk_ = CreateChunkSpec(
            ETableChunkFormat::SchemalessHorizontal,
            170); // keyRepetitions

        ChunkWithLimits_ = CreateChunkSpec(
            ETableChunkFormat::SchemalessHorizontal,
            2, // keyRepetitions
            300, // chunkRows
            "{lower_limit={key=[\"10010\"];row_index=100};upper_limit={}}");

        Chunk2WithLimits_ = CreateChunkSpec(
            ETableChunkFormat::SchemalessHorizontal,
            2, // keyRepetitions
            300, // chunkRows
            "{lower_limit={};upper_limit={key=[\"10280\"];row_index=240}}",
            10150); // minKey
    }

protected:
    TChunkSpec EmptyChunk_;
    TChunkSpec OneKeyChunk_;
    TChunkSpec TwoKeyChunk_;
    TChunkSpec ChunkWithLimits_;
    TChunkSpec Chunk2WithLimits_;

    TGuid GenerateId(EObjectType type)
    {
        static i64 counter = 0;
        return MakeId(type, 0, counter++, 0);
    }

    TGuid GenerateChunkId()
    {
        return GenerateId(EObjectType::Chunk);
    }

    TString FormatKey(i64 key)
    {
        return Format("%05d", key);
    }

    TOwningKey KeyWithValue(i64 key)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(FormatKey(key)));
        return builder.FinishRow();
    }

    TChunkSpec CreateChunkSpec(
        ETableChunkFormat version,
        i64 keyRepetitions,         // the number of times that each key is repeated
        i64 chunkRows = 300,        // the number of rows in the chunk
        const TString& ysonRange = TString("{lower_limit={};upper_limit={}}"),
                                    // range for chunk, specified as yson text
        i64 minKey = 10000,         // minimal value of key
        i64 blockRows = 79,         // the number of rows per block
        i64 rowBytes = 13)          // the size of one block
    {
        TChunkMeta chunkMeta;
        chunkMeta.set_type(static_cast<int>(EChunkType::Table));
        chunkMeta.set_version(static_cast<int>(version));

        i64 maxKey = minKey + (chunkRows - 1) / keyRepetitions;
        i64 numBlocks = (chunkRows + blockRows - 1) / blockRows;
        TBoundaryKeysExt boundaryKeys;

        ToProto(boundaryKeys.mutable_min(), KeyWithValue(minKey));
        ToProto(boundaryKeys.mutable_max(), KeyWithValue(maxKey));

        SetProtoExtension<TBoundaryKeysExt>(chunkMeta.mutable_extensions(), boundaryKeys);

        TBlockMetaExt blockMetaExt;
        for (i64 i = 0; i < numBlocks; ++i) {
            blockMetaExt.add_blocks();
            auto block = blockMetaExt.mutable_blocks(i);

            i64 rowIndex = std::min((i + 1) * blockRows, chunkRows);
            i64 rowCount = rowIndex - i * blockRows;
            block->set_row_count(rowCount);
            block->set_uncompressed_size(rowCount * rowBytes);
            block->set_chunk_row_count(rowIndex);
            block->set_block_index(i);
            ToProto(block->mutable_last_key(), KeyWithValue(minKey + (rowIndex - 1) / keyRepetitions));
        }
        SetProtoExtension<TBlockMetaExt>(chunkMeta.mutable_extensions(), blockMetaExt);

        TMiscExt miscExt;
        miscExt.set_row_count(chunkRows);
        miscExt.set_uncompressed_data_size(chunkRows * rowBytes);
        //miscExt.set_data_weight(chunkRows * rowBytes);
        SetProtoExtension<TMiscExt>(chunkMeta.mutable_extensions(), miscExt);

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), GenerateChunkId());
        auto range = ConvertTo<NChunkClient::TReadRange>(TYsonString(ysonRange));
        ToProto(chunkSpec.mutable_lower_limit(), range.LowerLimit());
        ToProto(chunkSpec.mutable_upper_limit(), range.UpperLimit());
        chunkSpec.mutable_chunk_meta()->Swap(&chunkMeta);

        return chunkSpec;
    }

    std::vector<NChunkClient::TChunkSlice> SliceChunk(
        const TChunkSpec& chunkSpec,
        i64 sliceDataWeight,
        int keyColumnCount,
        bool sliceByKeys)
    {
        NProto::TSliceRequest sliceRequest;

        sliceRequest.mutable_chunk_id()->CopyFrom(chunkSpec.chunk_id());
        if (chunkSpec.has_lower_limit()) {
            sliceRequest.mutable_lower_limit()->CopyFrom(chunkSpec.lower_limit());
        }
        if (chunkSpec.has_upper_limit()) {
            sliceRequest.mutable_upper_limit()->CopyFrom(chunkSpec.upper_limit());
        }
        sliceRequest.set_erasure_codec(chunkSpec.erasure_codec());

        return NChunkClient::SliceChunk(sliceRequest, chunkSpec.chunk_meta(), sliceDataWeight, keyColumnCount, sliceByKeys);
    }
};

TEST_F(TChunkSliceTest, OneKeyChunkSmallSlice)
{
    const auto& chunkSpec = OneKeyChunk_;
    auto keySlices = SliceChunk(
        chunkSpec,
        500, // sliceDataWeight
        1,   // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(1, keySlices.size());

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({key=["10000"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({key=["10000";<type=max>#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(300));

    auto rowSlices = SliceChunk(
        chunkSpec,
        500, // sliceDataWeight
        1,   // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(7, rowSlices.size());

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10000"];"row_index"=0})_"));

    Cerr << ConvertToYsonString(rowSlices[0].UpperLimit(), EYsonFormat::Pretty).GetData() << Endl;
    Cerr << ConvertToYsonString(rowSlices[6].LowerLimit(), EYsonFormat::Pretty).GetData() << Endl;
    Cerr << ConvertToYsonString(rowSlices[6].UpperLimit(), EYsonFormat::Pretty).GetData() << Endl;

    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#];"row_index"=39})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(39));

    EXPECT_THAT(rowSlices[6], HasLowerLimit(R"_({"key"=["10000"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[6], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[6], HasRowCount(63));
}

TEST_F(TChunkSliceTest, OneKeyChunkLargeSlice)
{
    const auto& chunkSpec = OneKeyChunk_;
    auto keySlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 1);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10000"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(300));

    auto rowSlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 2);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10000"];"row_index"=0})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(237));

    EXPECT_THAT(rowSlices[1], HasLowerLimit(R"_({"key"=["10000"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[1], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[1], HasRowCount(63));
}

TEST_F(TChunkSliceTest, TwoKeyChunkSmallSlice)
{
    const auto& chunkSpec = TwoKeyChunk_;
    auto keySlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 1);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10000"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10001";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(300));

    auto rowSlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 7);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10000"];"row_index"=0})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10000";<"type"="max">#];"row_index"=39})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(39));

    EXPECT_THAT(rowSlices[6], HasLowerLimit(R"_({"key"=["10001"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[6], HasUpperLimit(R"_({"key"=["10001";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[6], HasRowCount(63));
}

TEST_F(TChunkSliceTest, TwoKeyChunkLargeSlice)
{
    const auto& chunkSpec = TwoKeyChunk_;
    auto keySlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 1);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10000"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10001";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(300));

    auto rowSlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 2);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10000"];"row_index"=0})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10001";<"type"="max">#];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(237));

    EXPECT_THAT(rowSlices[1], HasLowerLimit(R"_({"key"=["10001"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[1], HasUpperLimit(R"_({"key"=["10001";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[1], HasRowCount(63));
}

TEST_F(TChunkSliceTest, ChunkWithLimitSmallSlice)
{
    const auto& chunkSpec = ChunkWithLimits_;
    auto keySlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 3);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10039"];"row_index"=100})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10078";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(58));

    EXPECT_THAT(keySlices[2], HasLowerLimit(R"_({"key"=["10118";<"type"="max">#];"row_index"=100})_"));
    EXPECT_THAT(keySlices[2], HasUpperLimit(R"_({"key"=["10149";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[2], HasRowCount(63));

    auto rowSlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 4);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10039"];"row_index"=100})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10078";<"type"="max">#];"row_index"=158})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(58));

    EXPECT_THAT(rowSlices[3], HasLowerLimit(R"_({"key"=["10118"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[3], HasUpperLimit(R"_({"key"=["10149";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[3], HasRowCount(63));
}

TEST_F(TChunkSliceTest, ChunkWithLimitLargeSlice)
{
    const auto & chunkSpec = ChunkWithLimits_;
    auto keySlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 1);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10039"];"row_index"=100})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10149";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(200));

    auto rowSlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 1);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10039"];"row_index"=100})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10149";<"type"="max">#];"row_index"=300})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(200));
}

TEST_F(TChunkSliceTest, Chunk2WithLimitSmallSlice)
{
    const auto & chunkSpec = Chunk2WithLimits_;
    auto keySlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 4);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10150"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10189";<"type"="max">#];"row_index"=240})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(79));

    EXPECT_THAT(keySlices[3], HasLowerLimit(R"_({"key"=["10268";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[3], HasUpperLimit(R"_({"key"=["10280"];"row_index"=240})_"));
    EXPECT_THAT(keySlices[3], HasRowCount(3));

    auto rowSlices = SliceChunk(
        chunkSpec,
        500, // sliceDataSize
        1,   // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 7);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10150"];"row_index"=0})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10189";<"type"="max">#];"row_index"=39})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(39));

    EXPECT_THAT(rowSlices[6], HasLowerLimit(R"_({"key"=["10268"];"row_index"=237})_"));
    EXPECT_THAT(rowSlices[6], HasUpperLimit(R"_({"key"=["10280"];"row_index"=240})_"));
    EXPECT_THAT(rowSlices[6], HasRowCount(3));
}

TEST_F(TChunkSliceTest, Chunk2WithLimitLargeSlice)
{
    const auto& chunkSpec = Chunk2WithLimits_;
    auto keySlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByKeys);
    EXPECT_EQ(keySlices.size(), 2);

    EXPECT_THAT(keySlices[0], HasLowerLimit(R"_({"key"=["10150"]})_"));
    EXPECT_THAT(keySlices[0], HasUpperLimit(R"_({"key"=["10268";<"type"="max">#];"row_index"=240})_"));
    EXPECT_THAT(keySlices[0], HasRowCount(237));

    EXPECT_THAT(keySlices[1], HasLowerLimit(R"_({"key"=["10268";<"type"="max">#]})_"));
    EXPECT_THAT(keySlices[1], HasUpperLimit(R"_({"key"=["10280"];"row_index"=240})_"));
    EXPECT_THAT(keySlices[1], HasRowCount(3));

    auto rowSlices = SliceChunk(
        chunkSpec,
        2500, // sliceDataSize
        1,    // keyColumnCount
        DoSliceByRows);
    EXPECT_EQ(rowSlices.size(), 2);

    EXPECT_THAT(rowSlices[0], HasLowerLimit(R"_({"key"=["10150";];"row_index"=0;})_"));
    EXPECT_THAT(rowSlices[0], HasUpperLimit(R"_({"key"=["10268";<"type"="max";>#;];"row_index"=237;})_"));
    EXPECT_THAT(rowSlices[0], HasRowCount(237));

    EXPECT_THAT(rowSlices[1], HasLowerLimit(R"_({"key"=["10268";];"row_index"=237;})_"));
    EXPECT_THAT(rowSlices[1], HasUpperLimit(R"_({"key"=["10280";];"row_index"=240;})_"));
    EXPECT_THAT(rowSlices[1], HasRowCount(3));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT
