#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;

using namespace ::testing;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TMiscExt;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TColumnarStatisticsTest
    : public Test
{
protected:
    void SetUp() override
    {
        TableRowCount_ = 0;
        TableSchema_.Reset();
    }

    TInputChunkPtr CreateChunk(
        const std::vector<i64>& columnDataWeights,
        const std::vector<std::string>& columnNames,
        EChunkFormat chunkFormat = EChunkFormat::TableUnversionedSchemalessHorizontal,
        i64 compressedDatasize = 10_MB,
        i64 uncompressedDataSize = 10_MB,
        i64 rowCount = 1000,
        i64 additionalDataWeight = 0)
    {
        YT_VERIFY(columnDataWeights.size() == columnNames.size());

        TChunkSpec spec;

        ToProto(spec.mutable_chunk_id(), MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
        spec.set_table_index(0);
        spec.set_table_row_index(TableRowCount_);
        TableRowCount_ += rowCount;

        auto* chunkMeta = spec.mutable_chunk_meta();

        chunkMeta->set_type(ToProto(EChunkType::Table));
        chunkMeta->set_format(ToProto(chunkFormat));

        TMiscExt miscExt;
        miscExt.set_compressed_data_size(compressedDatasize);
        miscExt.set_uncompressed_data_size(uncompressedDataSize);
        miscExt.set_data_weight(
            std::accumulate(
                columnDataWeights.begin(),
                columnDataWeights.end(),
                0LL) +
            additionalDataWeight);
        miscExt.set_row_count(rowCount);

        SetProtoExtension(chunkMeta->mutable_extensions(), miscExt);

        TColumnarStatistics statistics;
        statistics.ColumnDataWeights = columnDataWeights;
        auto heavyColumnarStatisticsExt = GetHeavyColumnStatisticsExt(
            statistics,
            [&] (int index) {
                return TColumnStableName(columnNames.at(index));
            },
            /*columnCount*/ std::ssize(columnDataWeights),
            /*maxHeavyColumns*/ std::ssize(columnDataWeights)
        );
        SetProtoExtension(chunkMeta->mutable_extensions(), heavyColumnarStatisticsExt);

        return New<TInputChunk>(std::move(spec));
    }

    static std::vector<TColumnStableName> ConvertToColumnStableNames(const std::vector<std::string>& columns)
    {
        return {columns.begin(), columns.end()};
    }

    static TColumnSchema MakeColumnSchema(
        const std::string& name,
        EValueType type,
        const std::optional<std::string>& group = std::nullopt)
    {
        TColumnSchema column(name, type);
        if (group.has_value()) {
            column.SetGroup(*group);
        }
        return column;
    }

    i64 EstimateReadDataSize(const std::vector<std::string>& columns)
    {
        YT_VERIFY(TableSchema_);
        YT_VERIFY(Chunk_);

        auto statistics = GetColumnarStatistics(Chunk_, ConvertToColumnStableNames(columns), TableSchema_);
        YT_VERIFY(statistics.ReadDataSizeEstimate);

        return *statistics.ReadDataSizeEstimate;
    }

    int TableRowCount_ = 0;
    TTableSchemaPtr TableSchema_;
    TInputChunkPtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TColumnarStatisticsTest, ColumnDataWeights)
{
    auto chunk = CreateChunk({5_MB, 10_MB, 200_MB}, {"col1", "col2", "col3"});

    {
        auto statistics = GetColumnarStatistics(chunk, {});
        EXPECT_EQ(std::ssize(statistics.ColumnDataWeights), 0);
    }

    {
        auto statistics = GetColumnarStatistics(
            chunk,
            ConvertToColumnStableNames({
                "col1",
                "col2",
                "col3",
                "unknown_column",
                NonexistentColumnName,
            }));
        ASSERT_EQ(std::ssize(statistics.ColumnDataWeights), 5);
        EXPECT_NEAR(statistics.ColumnDataWeights[0], 5_MB, 2_MB);
        EXPECT_NEAR(statistics.ColumnDataWeights[1], 10_MB, 3_MB);
        EXPECT_NEAR(statistics.ColumnDataWeights[2], 200_MB, 3_MB);
        EXPECT_NEAR(statistics.ColumnDataWeights[3], 5_MB, 2_MB);
        EXPECT_EQ(statistics.ColumnDataWeights[4], 0);

        EXPECT_FALSE(statistics.ReadDataSizeEstimate.has_value());
    }
}

TEST_F(TColumnarStatisticsTest, ColumnarChunkStrictSchemaNoGroups)
{
    TableSchema_ = New<TTableSchema>(std::vector{
        TColumnSchema("small", EValueType::Int64),
        TColumnSchema("large1", EValueType::String),
        TColumnSchema("large2", EValueType::String),
    });

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 10_MB,
        /*uncompressedDataSize*/ 100_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 512_KB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 1_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 8_MB, 1_MB);

    EXPECT_NEAR(
        EstimateReadDataSize({"large1"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large1"}),
        EstimateReadDataSize({"small", "large1"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"small", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large1"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"large1", NonexistentColumnName}),
        EstimateReadDataSize({"large1"}),
        128_KB);

    EXPECT_EQ(EstimateReadDataSize({NonexistentColumnName}), 0);
}

TEST_F(TColumnarStatisticsTest, ColumnarChunkNonStrictSchemaNoGroups)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        },
        /*strict*/ false);

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 10_MB,
        /*uncompressedDataSize*/ 120_MB,
        /*rowCount*/ 1000,
        /*additionalDataWeight*/ 20_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 512_KB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 1_MB, 256_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 8_MB, 1_MB);
    EXPECT_NEAR(EstimateReadDataSize({"unknown"}), 2_MB, 512_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"large1"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large1"}),
        EstimateReadDataSize({"small", "large1"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"small", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large1"}) + EstimateReadDataSize({"large2"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large2"}) + EstimateReadDataSize({"unknown1"}),
        EstimateReadDataSize({"small", "large2", "unknow2", "unknown1"}),
        128_KB);

    EXPECT_GT(EstimateReadDataSize({"large1", "unknown"}), EstimateReadDataSize({"large1"}));

    EXPECT_GT(EstimateReadDataSize({"large1"}), EstimateReadDataSize({"small"}));
    EXPECT_GT(EstimateReadDataSize({"large2"}), EstimateReadDataSize({"small"}));
    EXPECT_GT(EstimateReadDataSize({"large2"}), EstimateReadDataSize({"large1"}));
}

TEST_F(TColumnarStatisticsTest, ColumnarChunkStrictSchemaWithGroups)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            MakeColumnSchema("small", EValueType::Int64, "group1"),
            MakeColumnSchema("large1", EValueType::String, "group2"),
            MakeColumnSchema("large2", EValueType::String, "group1"),
        });

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 10_MB,
        /*uncompressedDataSize*/ 100_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 9_MB, 512_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 1_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 9_MB, 512_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}) + EstimateReadDataSize({"large1"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_EQ(EstimateReadDataSize({NonexistentColumnName}), 0);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}),
        EstimateReadDataSize({"large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"large1", "large2"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small", "large1"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small", "large2"}),
        EstimateReadDataSize({"small"}),
        128_KB);
}

TEST_F(TColumnarStatisticsTest, ColumnarChunkNonStrictSchemaWithGroups1)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            MakeColumnSchema("small", EValueType::Int64, "group1"),
            MakeColumnSchema("large1", EValueType::String, "group2"),
            MakeColumnSchema("large2", EValueType::String, "group1"),
        },
        /*strict*/ false);

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 12_MB,
        /*uncompressedDataSize*/ 120_MB,
        /*rowCount*/ 1000,
        /*additionalDataWeight*/ 20_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 9_MB, 512_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 1_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 9_MB, 512_KB);
    EXPECT_NEAR(EstimateReadDataSize({"unknown"}), 2_MB, 128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small", "large2"}),
        EstimateReadDataSize({"small"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small"}),
        EstimateReadDataSize({"large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"large1", "large2"}),
        EstimateReadDataSize({"small", "large1", "large2"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"unknown1", "unknown2"}),
        EstimateReadDataSize({"unknown1"}),
        128_KB);

    EXPECT_NEAR(
        EstimateReadDataSize({"small", "large1", "large2", "unknown1"}),
        EstimateReadDataSize({"small", "large1", "large2", "unknown2"}),
        128_KB);

    EXPECT_GT(
        EstimateReadDataSize({"small", "large1", "large2", "unknown1"}),
        EstimateReadDataSize({"small", "large2", "unknown1", "unknown2"}));
}

TEST_F(TColumnarStatisticsTest, ColumnarChunkNonStrictSchemaNoGroups2)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        },
        /*strict*/ false);

    Chunk_ = CreateChunk(
        {100_MB, 500_MB, 500_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 520_MB,
        /*uncompressedDataSize*/ 120_MB,
        /*rowCount*/ 1000,
        /*additionalDataWeight*/ 50_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 50_MB, 5_MB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 215_MB, 15_MB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 215_MB, 15_MB);
    EXPECT_NEAR(EstimateReadDataSize({"unknown1"}), 25_MB, 5_MB);

    EXPECT_EQ(EstimateReadDataSize({"unknown1"}), EstimateReadDataSize({"unknown2", "unknown3"}));
}

TEST_F(TColumnarStatisticsTest, HorizontalChunkNonStrict)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        },
        /*strict*/ false);

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedSchemalessHorizontal,
        /*compressedDatasize*/ 12_MB,
        /*uncompressedDataSize*/ 120_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"small", "large1"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"small", "large2"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"unknown"}), 12_MB, 128_KB);
}

TEST_F(TColumnarStatisticsTest, HorizontalChunkStrict)
{
    TableSchema_ = New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        });

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedSchemalessHorizontal,
        /*compressedDatasize*/ 12_MB,
        /*uncompressedDataSize*/ 120_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"small", "large1"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({"small", "large2"}), 12_MB, 128_KB);
    EXPECT_NEAR(EstimateReadDataSize({NonexistentColumnName}), 12_MB, 128_KB);
}

TEST_F(TColumnarStatisticsTest, CompressedSizeGreaterThanUncompressedSize)
{
    TableSchema_ = New<TTableSchema>(std::vector{
        TColumnSchema("small", EValueType::Int64),
        TColumnSchema("large1", EValueType::String),
        TColumnSchema("large2", EValueType::String),
    });

    Chunk_ = CreateChunk(
        {5_MB, 10_MB, 85_MB},
        {"small", "large1", "large2"},
        /*chunkFormat*/ EChunkFormat::TableUnversionedColumnar,
        /*compressedDatasize*/ 200_MB,
        /*uncompressedDataSize*/ 100_MB);

    EXPECT_NEAR(EstimateReadDataSize({"small"}), 10_MB, 1_MB);
    EXPECT_NEAR(EstimateReadDataSize({"large1"}), 20_MB, 2_MB);
    EXPECT_NEAR(EstimateReadDataSize({"large2"}), 170_MB, 5_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
