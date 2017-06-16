#include <yt/core/test_framework/framework.h>
#include "table_client_helpers.h"

#include <yt/ytlib/table_client/schemaless_block_reader.h>
#include <yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NTableClient {

using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBlocksTestBase
    : public ::testing::Test
{
protected:
    void CheckResult(THorizontalSchemalessBlockReader& reader, const std::vector<TUnversionedRow>& rows)
    {
        int i = 0;
        do {
            EXPECT_LT(i, rows.size());
            auto row = reader.GetRow(&MemoryPool);
            ExpectSchemafulRowsEqual(rows[i], row);
            ++i;
        } while (reader.NextRow());
    }

    TSharedRef Data;
    NProto::TBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBlocksTestOneRow
    :public TSchemalessBlocksTestBase
{
protected:
    virtual void SetUp() override
    {
        THorizontalSchemalessBlockWriter blockWriter;

        auto row = TMutableUnversionedRow::Allocate(&MemoryPool, 5);
        row[0] = MakeUnversionedStringValue("a", 0);
        row[1] = MakeUnversionedInt64Value(1, 3);
        row[2] = MakeUnversionedDoubleValue(1.5, 2);
        row[3] = MakeUnversionedInt64Value(8, 5);
        row[4] = MakeUnversionedInt64Value(7, 7);

        blockWriter.WriteRow(row);

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data = codec->Compress(block.Data);
        Meta = block.Meta;
    }

};

TEST_F(TSchemalessBlocksTestOneRow, ReadColumnFilter)
{
    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> idMapping = {
        {0, -1}, 
        {1, -1}, 
        {2,  0}, 
        {3, -1}, 
        {4, -1}, 
        {5, -1},
        {6, -1},
        {7,  1}};

    auto row = TMutableUnversionedRow::Allocate(&MemoryPool, 2);
    row[0] = MakeUnversionedDoubleValue(1.5, 0);
    row[1] = MakeUnversionedInt64Value(7, 1);

    std::vector<TUnversionedRow> rows;
    rows.push_back(row);

    THorizontalSchemalessBlockReader blockReader(
        Data,
        Meta,
        idMapping,
        0,
        0);

    CheckResult(blockReader, rows);
}

TEST_F(TSchemalessBlocksTestOneRow, SkipToKey)
{
    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> idMapping = {
        {0, 0}, 
        {1, 1}, 
        {2, 2}, 
        {3, 3}, 
        {4, 4}, 
        {5, 5}, 
        {6, 6}, 
        {7, 7}};

    THorizontalSchemalessBlockReader blockReader(
        Data,
        Meta,
        idMapping,
        2,
        2);

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
    :public TSchemalessBlocksTestBase
{
protected:
    virtual void SetUp() override
    {
        THorizontalSchemalessBlockWriter blockWriter;

        for (auto row : MakeRows(0, 1000)) {
            blockWriter.WriteRow(row);
        }

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data = codec->Compress(block.Data);
        Meta = block.Meta;
    }

    std::vector<TUnversionedRow> MakeRows(int beginIndex, int endIndex)
    {
        std::vector<TUnversionedRow> result;
        for (int i = beginIndex; i < endIndex ; ++i) {
            auto row = TMutableUnversionedRow::Allocate(&MemoryPool, 2);
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
    std::vector<TColumnIdMapping> idMapping = {{0, 0}, {1, 1}};

    THorizontalSchemalessBlockReader blockReader(
        Data,
        Meta,
        idMapping,
        2,
        2);

    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42));
    EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));

    CheckResult(blockReader, MakeRows(42, 1000));
}

TEST_F(TSchemalessBlocksTestManyRows, SkipToWiderKey)
{
    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> idMapping = {{0, 0}, {1, 1}};

    THorizontalSchemalessBlockReader blockReader(
        Data,
        Meta,
        idMapping,
        1,
        2);

    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42));
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null));
    EXPECT_TRUE(blockReader.SkipToKey(builder.FinishRow()));

    CheckResult(blockReader, MakeRows(42, 1000));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
