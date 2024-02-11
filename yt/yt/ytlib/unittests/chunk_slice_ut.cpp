#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/yson/string.h>

#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NChunkClient {
namespace {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(const std::vector<int>& values, bool addMax = false)
{
    TUnversionedOwningRowBuilder builder;
    for (int value : values) {
        builder.AddValue(MakeUnversionedInt64Value(value));
    }
    if (addMax) {
        builder.AddValue(MakeUnversionedValueHeader(EValueType::Max));
    }
    return builder.FinishRow();
}

TOwningKeyBound MakeKeyBound(const std::vector<TUnversionedValue>& values, bool isInclusive, bool isUpper)
{
    TUnversionedOwningRowBuilder builder;
    for (auto value : values) {
        builder.AddValue(value);
    }
    return TOwningKeyBound::FromRow(builder.FinishRow(), isInclusive, isUpper);
}

TLegacyReadLimit MakeReadLimit(
    std::optional<TUnversionedOwningRow> key,
    std::optional<i64> rowIndex)
{
    TLegacyReadLimit readLimit;
    if (key) {
        readLimit.SetLegacyKey(*key);
    }
    if (rowIndex) {
        readLimit.SetRowIndex(*rowIndex);
    }

    return readLimit;
}

void ValidateCovering(const std::vector<TChunkSlice>& slices, bool sliceByRows)
{
    for (int index = 0; index + 1 < std::ssize(slices); ++index) {
        const auto& slice = slices[index];
        const auto& nextSlice = slices[index + 1];

        if (sliceByRows) {
            EXPECT_TRUE(slice.UpperLimit.GetRowIndex() == nextSlice.LowerLimit.GetRowIndex());
        } else {
            EXPECT_TRUE(slice.UpperLimit.KeyBound().Invert() == nextSlice.LowerLimit.KeyBound());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TChunkBuilder
{
public:
    TChunkBuilder(int keyColumnCount)
    {
        ChunkMeta_.set_format(ToProto<int>(EChunkFormat::TableUnversionedSchemalessHorizontal));

        {
            std::vector<TColumnSchema> columns;
            columns.reserve(keyColumnCount);
            for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
                std::optional<ESortOrder> sortOrder;
                if (columnIndex < keyColumnCount) {
                    sortOrder = ESortOrder::Ascending;
                }
                TColumnSchema columnSchema(
                    /*name*/Format("c%v", columnIndex),
                    /*type*/EValueType::Int64,
                    /*sortOrder*/sortOrder);
                columns.push_back(std::move(columnSchema));
            }

            TTableSchema chunkSchema(std::move(columns));
            NTableClient::NProto::TTableSchemaExt chunkSchemaExt;
            ToProto(&chunkSchemaExt, chunkSchema);
            SetProtoExtension(ChunkMeta_.mutable_extensions(), chunkSchemaExt);
        }
    }

    void AddBlock(
        TUnversionedOwningRow firstKey,
        TUnversionedOwningRow lastKey,
        i64 rowCount,
        i64 dataWeight)
    {
        if (!FirstKey_) {
            FirstKey_ = firstKey;
        }
        LastKey_ = lastKey;

        ChunkRowCount_ += rowCount;
        ChunkDataWeight_ += dataWeight;

        int blockIndex = Blocks_.data_blocks_size();
        auto* block = Blocks_.add_data_blocks();
        block->set_block_index(blockIndex);
        ToProto(block->mutable_last_key(), lastKey);
        block->set_row_count(rowCount);
        block->set_uncompressed_size(dataWeight);
        block->set_chunk_row_count(ChunkRowCount_);
    }

    NChunkClient::NProto::TChunkMeta Finish()
    {
        {
            NProto::TMiscExt miscExt;
            miscExt.set_data_weight(ChunkDataWeight_);
            miscExt.set_row_count(ChunkRowCount_);
            SetProtoExtension(ChunkMeta_.mutable_extensions(), miscExt);
        }

        {
            NTableClient::NProto::TBoundaryKeysExt boundaryKeysExt;
            ToProto(boundaryKeysExt.mutable_min(), FirstKey_);
            ToProto(boundaryKeysExt.mutable_max(), LastKey_);
            SetProtoExtension(ChunkMeta_.mutable_extensions(), boundaryKeysExt);
        }

        SetProtoExtension(ChunkMeta_.mutable_extensions(), Blocks_);

        return ChunkMeta_;
    }

private:
    NChunkClient::NProto::TChunkMeta ChunkMeta_;
    NTableClient::NProto::TDataBlockMetaExt Blocks_;
    TUnversionedOwningRow FirstKey_;
    TUnversionedOwningRow LastKey_;
    i64 ChunkRowCount_ = 0;
    i64 ChunkDataWeight_ = 0;
};

TEST(TChunkSlicerTest, SliceByRowsOneBlock)
{
    constexpr int DataWeightPerRow = 12345;

    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100 * DataWeightPerRow);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(34 * DataWeightPerRow);
    req.set_key_column_count(1);
    req.set_slice_by_keys(false);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/true);
    ASSERT_EQ(slices.size(), 3u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 0);
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 34);
    EXPECT_EQ(slices[0].DataWeight, 34 * DataWeightPerRow);
    EXPECT_EQ(slices[0].RowCount, 34);

    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 34);
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 68);
    EXPECT_EQ(slices[1].DataWeight, 34 * DataWeightPerRow);
    EXPECT_EQ(slices[1].RowCount, 34);

    EXPECT_EQ(slices[2].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[2].LowerLimit.GetRowIndex(), 68);
    EXPECT_EQ(slices[2].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[2].UpperLimit.GetRowIndex(), 100);
    EXPECT_EQ(slices[2].DataWeight, 32 * DataWeightPerRow);
    EXPECT_EQ(slices[2].RowCount, 32);
}

TEST(TChunkSlicerTest, SliceByRowsThreeBlocks)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({3}),
        MakeRow({4}),
        400,
        400);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({6}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();
    NProto::TSliceRequest req;

    req.set_slice_data_weight(200);
    req.set_key_column_count(1);
    req.set_slice_by_keys(false);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/true);
    ASSERT_EQ(slices.size(), 3u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 0);
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({4}));
    EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 200);
    EXPECT_EQ(slices[0].DataWeight, 200);
    EXPECT_EQ(slices[0].RowCount, 200);

    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
    EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 200);
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({4}));
    EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 400);
    EXPECT_EQ(slices[1].DataWeight, 200);
    EXPECT_EQ(slices[1].RowCount, 200);

    EXPECT_EQ(slices[2].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
    EXPECT_EQ(slices[2].LowerLimit.GetRowIndex(), 400);
    EXPECT_EQ(slices[2].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({6}));
    EXPECT_EQ(slices[2].UpperLimit.GetRowIndex(), 600);
    EXPECT_EQ(slices[2].DataWeight, 200);
    EXPECT_EQ(slices[2].RowCount, 200);
}

TEST(TChunkSlicerTest, SliceByRowsWithRowIndexLimits1)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({3}),
        MakeRow({4}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({6}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({7}),
        MakeRow({8}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({9}),
        MakeRow({10}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 150));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 350));
        req.set_slice_data_weight(100);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 2u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 150);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({6}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 250);
        EXPECT_EQ(slices[0].DataWeight, 100);
        EXPECT_EQ(slices[0].RowCount, 100);

        EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({4}));
        EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 250);
        EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({8}));
        EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 350);
        EXPECT_EQ(slices[1].DataWeight, 100);
        EXPECT_EQ(slices[1].RowCount, 100);
    }
    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 150));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 350));
        req.set_slice_data_weight(10000);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 1u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 150);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({8}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 350);
        EXPECT_EQ(slices[0].DataWeight, 200);
        EXPECT_EQ(slices[0].RowCount, 200);
    }
}

TEST(TChunkSlicerTest, SliceByRowsWithRowIndexLimits2)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();
    NProto::TSliceRequest req;

    ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 23));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 34));
    req.set_slice_data_weight(1000);
    req.set_key_column_count(1);
    req.set_slice_by_keys(false);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/true);
    ASSERT_EQ(slices.size(), 1u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 23);
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 34);
    EXPECT_EQ(slices[0].DataWeight, 11);
    EXPECT_EQ(slices[0].RowCount, 11);
}

TEST(TChunkSlicerTest, SliceByRowsWithRowIndexLimits3)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({3}),
        MakeRow({4}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({6}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();
    NProto::TSliceRequest req;

    ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 100));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 200));
    req.set_slice_data_weight(90);
    req.set_key_column_count(1);
    req.set_slice_by_keys(false);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/true);
    ASSERT_EQ(slices.size(), 2u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
    EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 100);
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({4}));
    EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 190);
    EXPECT_EQ(slices[0].DataWeight, 90);
    EXPECT_EQ(slices[0].RowCount, 90);

    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
    EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 190);
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({4}));
    EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 200);
    EXPECT_EQ(slices[1].DataWeight, 10);
    EXPECT_EQ(slices[1].RowCount, 10);
}

TEST(TChunkSlicerTest, SliceByRowsWithKeyLimits1)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({7}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({9}),
        MakeRow({11}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({13}),
        MakeRow({15}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({17}),
        MakeRow({19}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({6}), std::nullopt));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({14}), std::nullopt));
        req.set_slice_data_weight(90);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 4u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 100);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({7}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 190);
        EXPECT_EQ(slices[0].DataWeight, 90);
        EXPECT_EQ(slices[0].RowCount, 90);

        EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
        EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 190);
        EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({11}));
        EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 280);
        EXPECT_EQ(slices[1].DataWeight, 90);
        EXPECT_EQ(slices[1].RowCount, 90);

        EXPECT_EQ(slices[2].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({7}));
        EXPECT_EQ(slices[2].LowerLimit.GetRowIndex(), 280);
        EXPECT_EQ(slices[2].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({14}));
        EXPECT_EQ(slices[2].UpperLimit.GetRowIndex(), 370);
        EXPECT_EQ(slices[2].DataWeight, 90);
        EXPECT_EQ(slices[2].RowCount, 90);

        EXPECT_EQ(slices[3].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({11}));
        EXPECT_EQ(slices[3].LowerLimit.GetRowIndex(), 370);
        EXPECT_EQ(slices[3].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({14}));
        EXPECT_EQ(slices[3].UpperLimit.GetRowIndex(), 400);
        EXPECT_EQ(slices[3].DataWeight, 30);
        EXPECT_EQ(slices[3].RowCount, 30);
    }

    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({6}), std::nullopt));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({14}), std::nullopt));
        req.set_slice_data_weight(10000);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 1u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 100);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({14}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 400);
        EXPECT_EQ(slices[0].DataWeight, 300);
        EXPECT_EQ(slices[0].RowCount, 300);
    }
}

TEST(TChunkSlicerTest, SliceByRowsWithKeyLimits2)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({2}), std::nullopt));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({2}), std::nullopt));
        req.set_slice_data_weight(90);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 2u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 0);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({2}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 90);
        EXPECT_EQ(slices[0].DataWeight, 90);
        EXPECT_EQ(slices[0].RowCount, 90);

        EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({2}));
        EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 90);
        EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({2}));
        EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 100);
        EXPECT_EQ(slices[1].DataWeight, 10);
        EXPECT_EQ(slices[1].RowCount, 10);
    }
    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({0}), std::nullopt));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({4}), std::nullopt));
        req.set_slice_data_weight(90);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 2u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 0);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({3}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 90);
        EXPECT_EQ(slices[0].DataWeight, 90);
        EXPECT_EQ(slices[0].RowCount, 90);

        EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
        EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 90);
        EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({3}));
        EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 100);
        EXPECT_EQ(slices[1].DataWeight, 10);
        EXPECT_EQ(slices[1].RowCount, 10);
    }
}

TEST(TChunkSlicerTest, SliceByRowsWithRowIndexAndKeyLimits)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({7}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({9}),
        MakeRow({11}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({13}),
        MakeRow({15}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({17}),
        MakeRow({19}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    {
        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({6}), 50));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({18}), 320));
        req.set_slice_data_weight(110);
        req.set_key_column_count(1);
        req.set_slice_by_keys(false);

        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/true);
        ASSERT_EQ(slices.size(), 2u);

        EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
        EXPECT_EQ(slices[0].LowerLimit.GetRowIndex(), 100);
        EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({11}));
        EXPECT_EQ(slices[0].UpperLimit.GetRowIndex(), 210);
        EXPECT_EQ(slices[0].DataWeight, 110);
        EXPECT_EQ(slices[0].RowCount, 110);

        EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({7}));
        EXPECT_EQ(slices[1].LowerLimit.GetRowIndex(), 210);
        EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({15}));
        EXPECT_EQ(slices[1].UpperLimit.GetRowIndex(), 320);
        EXPECT_EQ(slices[1].DataWeight, 110);
        EXPECT_EQ(slices[1].RowCount, 110);
    }
}

TEST(TChunkSlicerTest, SliceByKeysOneBlock)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].DataWeight, 100);
    EXPECT_EQ(slices[0].RowCount, 100);
}

TEST(TChunkSlicerTest, SliceByKeysTwoBlocks)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({2}),
        MakeRow({3}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 2u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].DataWeight, 100);
    EXPECT_EQ(slices[0].RowCount, 100);

    // NB: Second block actually contains key 2, however we don't know about it
    // since only block last keys are stored in meta.
    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() > MakeRow({2}));
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({3}));
    EXPECT_EQ(slices[1].DataWeight, 100);
    EXPECT_EQ(slices[1].RowCount, 100);
}

TEST(TChunkSlicerTest, SliceByKeysWiderRequest)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({2}),
        MakeRow({3}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(2);
    req.set_slice_by_keys(true);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 2u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].DataWeight, 100);
    EXPECT_EQ(slices[0].RowCount, 100);

    // NB: Second block actually contains key 2, however we don't know about it
    // since only block last keys are stored in meta.
    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() > MakeRow({2}));
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({3}));
    EXPECT_EQ(slices[1].DataWeight, 100);
    EXPECT_EQ(slices[1].RowCount, 100);
}

TEST(TChunkSlicerTest, SliceByKeysManiac1)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({1}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({1}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({1}));
    EXPECT_EQ(slices[0].DataWeight, 200);
    EXPECT_EQ(slices[0].RowCount, 200);
}

TEST(TChunkSlicerTest, SliceByKeysManiac2)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({2}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({2}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({2}),
        MakeRow({3}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);
    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 2u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({2}));
    EXPECT_EQ(slices[0].DataWeight, 300);
    EXPECT_EQ(slices[0].RowCount, 300);

    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() > MakeRow({2}));
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({3}));
    EXPECT_EQ(slices[1].DataWeight, 100);
    EXPECT_EQ(slices[1].RowCount, 100);
}

TEST(TChunkSlicerTest, SliceByKeysManiac3)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({3}),
        MakeRow({3}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({1}), std::nullopt));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({2}), std::nullopt));
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);

    // We can take first block only since there is no key 2 in second block.
    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({2}));
    EXPECT_EQ(slices[0].DataWeight, 100);
    EXPECT_EQ(slices[0].RowCount, 100);
}

TEST(TChunkSlicerTest, SliceByKeysManiac4)
{
    TChunkBuilder chunkBuilder(2);
    chunkBuilder.AddBlock(
        MakeRow({1, 1}),
        MakeRow({1, 2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({1, 3}),
        MakeRow({1, 4}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({1}));
    EXPECT_EQ(slices[0].DataWeight, 200);
    EXPECT_EQ(slices[0].RowCount, 200);
}

TEST(TChunkSlicerTest, SliceByKeysWithRowIndexLimits1)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({2}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({3}),
        MakeRow({4}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({6}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 50));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 240));
    req.set_slice_data_weight(100);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 2u);

    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({4}));
    EXPECT_EQ(slices[0].DataWeight, 150);
    EXPECT_EQ(slices[0].RowCount, 150);

    EXPECT_EQ(slices[1].LowerLimit.KeyBound(), TKeyBound::FromRow() > MakeRow({4}));
    EXPECT_EQ(slices[1].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({6}));
    EXPECT_EQ(slices[1].DataWeight, 40);
    EXPECT_EQ(slices[1].RowCount, 40);
}

TEST(TChunkSlicerTest, SliceByKeysWithRowIndexLimits2)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({1}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({1}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({1}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    ToProto(req.mutable_lower_limit(), MakeReadLimit(std::nullopt, 134));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(std::nullopt, 135));
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);
    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({1}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() <= MakeRow({1}));
    EXPECT_EQ(slices[0].DataWeight, 1);
    EXPECT_EQ(slices[0].RowCount, 1);
}

TEST(TChunkSlicerTest, SliceByKeysWithKeyLimits1)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({7}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({9}),
        MakeRow({11}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({11}),
        MakeRow({13}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({15}),
        MakeRow({17}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({6}), std::nullopt));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({12}), std::nullopt));
    req.set_slice_data_weight(10000);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);
    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({12}));
    EXPECT_EQ(slices[0].DataWeight, 300);
    EXPECT_EQ(slices[0].RowCount, 300);
}

TEST(TChunkSlicerTest, SliceByKeysWithKeyLimits2)
{
    TChunkBuilder chunkBuilder(1);
    chunkBuilder.AddBlock(
        MakeRow({1}),
        MakeRow({3}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({5}),
        MakeRow({7}),
        100,
        100);
    chunkBuilder.AddBlock(
        MakeRow({9}),
        MakeRow({11}),
        100,
        100);

    auto chunkMeta = chunkBuilder.Finish();

    NProto::TSliceRequest req;
    ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({6}), std::nullopt));
    ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({6}), std::nullopt));
    req.set_slice_data_weight(1);
    req.set_key_column_count(1);
    req.set_slice_by_keys(true);

    auto slices = SliceChunk(req, chunkMeta);
    ValidateCovering(slices, /*sliceByRows*/false);
    ASSERT_EQ(slices.size(), 1u);
    EXPECT_EQ(slices[0].LowerLimit.KeyBound(), TKeyBound::FromRow() >= MakeRow({6}));
    EXPECT_EQ(slices[0].UpperLimit.KeyBound(), TKeyBound::FromRow() < MakeRow({6}));
    EXPECT_EQ(slices[0].DataWeight, 100);
    EXPECT_EQ(slices[0].RowCount, 100);
}

TEST(TChunkSlicerTest, StressTest)
{
    static std::mt19937 rng(42);

    constexpr int Iterations = 50'000;
    constexpr int MaxBlocks = 10;
    constexpr int MaxRowsPerBlock = 10;
    constexpr int KeysRange = 20;

    struct TBlock
    {
        int LowerLimit;
        int UpperLimit;
        int StartRowIndex;
        int EndRowIndex;
    };

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        TChunkBuilder chunkBuilder(1);
        std::vector<TBlock> blocks;

        int keysRange = (rng() % KeysRange) + 1;
        int lastKey = rng() % keysRange;
        int totalRowCount = 0;

        int blockCount = (rng() % MaxBlocks) + 1;
        for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
            int upperLimit = lastKey + rng() % (keysRange - lastKey + 1);
            int rowCount = rng() % MaxRowsPerBlock + 1;
            blocks.push_back(TBlock{
                .LowerLimit = lastKey,
                .UpperLimit = upperLimit,
                .StartRowIndex = totalRowCount,
                .EndRowIndex = totalRowCount + rowCount
            });
            chunkBuilder.AddBlock(
                MakeRow({lastKey}),
                MakeRow({upperLimit}),
                rowCount,
                rowCount
            );

            lastKey = upperLimit;
            totalRowCount += rowCount;
        }

        auto chunkMeta = chunkBuilder.Finish();

        int sliceLowerLimit = rng() % keysRange;
        int sliceUpperLimit = rng() % keysRange;
        if (sliceLowerLimit > sliceUpperLimit) {
            std::swap(sliceLowerLimit, sliceUpperLimit);
        }
        int sliceStartRowIndex = rng() % totalRowCount;
        int sliceEndRowIndex = rng() % totalRowCount;
        if (sliceStartRowIndex > sliceEndRowIndex) {
            std::swap(sliceStartRowIndex, sliceEndRowIndex);
        }

        bool sliceByKeys = rng() % 2;

        NProto::TSliceRequest req;
        ToProto(req.mutable_lower_limit(), MakeReadLimit(MakeRow({sliceLowerLimit}), sliceStartRowIndex));
        ToProto(req.mutable_upper_limit(), MakeReadLimit(MakeRow({sliceUpperLimit}), sliceEndRowIndex));
        req.set_slice_data_weight(rng() % totalRowCount);
        req.set_key_column_count(1);
        req.set_slice_by_keys(sliceByKeys);
        auto slices = SliceChunk(req, chunkMeta);
        ValidateCovering(slices, /*sliceByRows*/!sliceByKeys);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkSliceLimitTest, LegacyNewInterop)
{
    auto rowBuffer = New<TRowBuffer>();

    TLegacyInputSliceLimit legacyLimit;
    legacyLimit.RowIndex = 42;

    constexpr int KeyLength = 2;
    auto intValue = MakeUnversionedInt64Value(27);
    auto maxValue = MakeUnversionedSentinelValue(EValueType::Max);

    auto makeRow = [] (const std::vector<TUnversionedValue>& values) {
        TUnversionedOwningRowBuilder builder;
        for (auto value : values) {
            builder.AddValue(value);
        }
        return builder.FinishRow();
    };

    legacyLimit.Key = rowBuffer->CaptureRow(makeRow({intValue, maxValue, maxValue}));

    NProto::TReadLimit protoLimit;
    ToProto(&protoLimit, legacyLimit);

    TInputSliceLimit newLimit(protoLimit, rowBuffer, TRange<TLegacyKey>(), TRange<TLegacyKey>(), KeyLength, /*isUpper*/ true);

    EXPECT_EQ(std::make_optional(42), newLimit.RowIndex);
    EXPECT_EQ(MakeKeyBound({intValue}, /*isInclusive*/ true, /*isUpper*/ true), newLimit.KeyBound);

    ToProto(&protoLimit, newLimit);

    legacyLimit = TLegacyInputSliceLimit(protoLimit, rowBuffer, TRange<TLegacyKey>());
    EXPECT_EQ(std::make_optional(42), legacyLimit.RowIndex);
    EXPECT_EQ(makeRow({intValue, maxValue}), legacyLimit.Key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkClient
} // namespace NYT
