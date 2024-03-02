#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_client/schemaless_block_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_block_writer.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {
namespace {

using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBlocksTestBase
    : public ::testing::Test
{
protected:
    void CheckResult(THorizontalBlockReader& reader, const std::vector<TUnversionedRow>& rows)
    {
        std::vector<TUnversionedRow> actual;
        do {
            auto row = reader.GetRow(&MemoryPool_);
            actual.push_back(row);
        } while (reader.NextRow());
        CheckSchemafulResult(actual, rows);
    }

protected:
    TSharedRef Data_;
    NProto::TDataBlockMeta Meta_;

    TChunkedMemoryPool MemoryPool_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBlocksTestOneRow
    : public TSchemalessBlocksTestBase
{
protected:
    void SetUp() override
    {
        THorizontalBlockWriter blockWriter(New<TTableSchema>());

        auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, 5);
        row[0] = MakeUnversionedStringValue("a", 0);
        row[1] = MakeUnversionedInt64Value(1, 3);
        row[2] = MakeUnversionedDoubleValue(1.5, 2);
        row[3] = MakeUnversionedInt64Value(8, 5);
        row[4] = MakeUnversionedInt64Value(7, 7);

        blockWriter.WriteRow(row);

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data_ = codec->Compress(block.Data);
        Meta_ = block.Meta;
    }

    template <EValueType WriterType, EValueType ReaderType>
    void TestComplexAnyCompatibility()
    {
        std::vector<int> idMapping = {
            0,
        };

        auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, 1);
        row[0] = MakeUnversionedStringLikeValue(WriterType, "[]", 0);

        THorizontalBlockWriter blockWriter(New<TTableSchema>());
        blockWriter.WriteRow(row);
        auto block = blockWriter.FlushBlock();

        auto* codec = GetCodec(ECodec::None);
        Data_ = codec->Compress(block.Data);
        Meta_ = block.Meta;


        auto expectedRow = TMutableUnversionedRow::Allocate(&MemoryPool_, 1);
        expectedRow[0] = MakeUnversionedStringLikeValue(ReaderType, "[]", 0);

        std::vector<bool> compositeColumnFlags;
        if constexpr (ReaderType == EValueType::Composite) {
            compositeColumnFlags.push_back(true);
        }

        THorizontalBlockReader blockReader(
            Data_,
            Meta_,
            compositeColumnFlags,
            /*hunkColumnFlags*/ std::vector<bool>{},
            /*hunkChunkMetas*/ {},
            /*hunkChunkRefs*/ {},
            idMapping,
            /*sortOrders*/ {},
            /*commonKeyPrefix*/ 0,
            /*keyWideningOptions*/ {},
            /*extraColumnCount*/ 0);

        CheckResult(blockReader, std::vector<TUnversionedRow>{expectedRow});
    }
};

TEST_F(TSchemalessBlocksTestOneRow, ComplexWriter_AnyReader_Compatibility)
{
    TestComplexAnyCompatibility<EValueType::Composite, EValueType::Any>();
}

TEST_F(TSchemalessBlocksTestOneRow, AnyWriter_ComplexReader_Compatibility)
{
    TestComplexAnyCompatibility<EValueType::Any, EValueType::Composite>();
}

TEST_F(TSchemalessBlocksTestOneRow, ReadColumnFilter)
{
    // Reorder value columns in reading schema.
    std::vector<int> idMapping = {
        -1,
        -1,
        0,
        -1,
        -1,
        -1,
        -1,
        1
    };

    auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, 2);
    row[0] = MakeUnversionedDoubleValue(1.5, 0);
    row[1] = MakeUnversionedInt64Value(7, 1);

    std::vector<TUnversionedRow> rows;
    rows.push_back(row);

    THorizontalBlockReader blockReader(
        Data_,
        Meta_,
        /*compositeColumnFlags*/ std::vector<bool>{},
        /*hunkColumnFlags*/ std::vector<bool>{},
        /*hunkChunkMetas*/ {},
        /*hunkChunkRefs*/ {},
        idMapping,
        /*sortOrders*/ {},
        /*commonKeyPrefix*/ 0,
        /*keyWideningOptions*/ {});

    CheckResult(blockReader, rows);
}

TEST_F(TSchemalessBlocksTestOneRow, SkipToKey)
{
    // Reorder value columns in reading schema.
    std::vector<int> idMapping = {
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7};

    std::vector<ESortOrder> sortOrders(2, ESortOrder::Ascending);
    THorizontalBlockReader blockReader(
        Data_,
        Meta_,
        std::vector<bool>{},
        /*hunkColumnFlags*/ std::vector<bool>{},
        /*hunkChunkMetas*/ {},
        /*hunkChunkRefs*/ {},
        idMapping,
        sortOrders,
        sortOrders.size(),
        /*keyWideningOptions*/ {});

    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue("a"));
        builder.AddValue(MakeUnversionedInt64Value(0));

        EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));
    } {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue("a"));
        builder.AddValue(MakeUnversionedInt64Value(1));

        EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));
    } {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue("a"));
        builder.AddValue(MakeUnversionedInt64Value(2));

        EXPECT_FALSE(blockReader.SkipToKey(builder.FinishRow()));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBlocksTestManyRows
    : public TSchemalessBlocksTestBase
{
protected:
    void SetUp() override
    {
        THorizontalBlockWriter blockWriter(New<TTableSchema>());

        for (auto row : MakeRows(0, 1000)) {
            blockWriter.WriteRow(row);
        }

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data_ = codec->Compress(block.Data);
        Meta_ = block.Meta;
    }

    std::vector<TUnversionedRow> MakeRows(int beginIndex, int endIndex)
    {
        std::vector<TUnversionedRow> result;
        for (int i = beginIndex; i < endIndex ; ++i) {
            auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, 2);
            row[0] = MakeUnversionedInt64Value(i, 0);
            row[1] = MakeUnversionedStringValue("big data", 1);
            result.push_back(row);
        }
        return result;
    }
};

TEST_F(TSchemalessBlocksTestManyRows, SkipToKey)
{
    // Reorder value columns in reading schema.
    std::vector<int> idMapping = {0, 1};

    std::vector<ESortOrder> sortOrders(2, ESortOrder::Ascending);
    THorizontalBlockReader blockReader(
        Data_,
        Meta_,
        std::vector<bool>{},
        /*hunkColumnFlags*/ std::vector<bool>{},
        /*hunkChunkMetas*/ {},
        /*hunkChunkRefs*/ {},
        idMapping,
        sortOrders,
        sortOrders.size(),
        /*keyWideningOptions*/ {});

    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42));
    EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));

    CheckResult(blockReader, MakeRows(42, 1000));
}

TEST_F(TSchemalessBlocksTestManyRows, SkipToWiderKey)
{
    // Reorder value columns in reading schema.
    std::vector<int> idMapping = {0, 1};

    std::vector<ESortOrder> sortOrders(2, ESortOrder::Ascending);
    THorizontalBlockReader blockReader(
        Data_,
        Meta_,
        std::vector<bool>{},
        /*hunkColumnFlags*/ std::vector<bool>{},
        /*hunkChunkMetas*/ {},
        /*hunkChunkRefs*/ {},
        idMapping,
        sortOrders,
        /*commonKeyPrefix*/ 1,
        /*keyWideningOptions*/ {});

    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42));
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null));
    EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));

    CheckResult(blockReader, MakeRows(42, 1000));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
