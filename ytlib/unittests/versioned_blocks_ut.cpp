#include <yt/core/test_framework/framework.h>
#include "table_client_helpers.h"

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/versioned_block_reader.h>
#include <yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NTransactionClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlocksTestBase
    : public ::testing::Test
{
protected:
    void CheckResult(TSimpleVersionedBlockReader& reader, const std::vector<TVersionedRow>& rows)
    {
        int i = 0;
        do {
            EXPECT_LT(i, rows.size());
            auto row = reader.GetRow(&MemoryPool);
            ExpectSchemafulRowsEqual(rows[i], row);
        } while (reader.NextRow());
    }

    TTableSchema Schema;

    TSharedRef Data;
    NProto::TBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlocksTestOneRow
    : public TVersionedBlocksTestBase
{
protected:
    virtual void SetUp() override
    {
        Schema = TTableSchema({
            TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Boolean),
            TColumnSchema("v3", EValueType::Int64)
        });

        TSimpleVersionedBlockWriter blockWriter(Schema);

        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 5, 3, 1);
        row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
        row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
        row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
        row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 3);
        // v2
        row.BeginValues()[2] = MakeVersionedBooleanValue(true, 5, 4);
        row.BeginValues()[3] = MakeVersionedBooleanValue(false, 3, 4);
        // v3
        row.BeginValues()[4] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);

        row.BeginWriteTimestamps()[2] = 3;
        row.BeginWriteTimestamps()[1] = 5;
        row.BeginWriteTimestamps()[0] = 11;

        row.BeginDeleteTimestamps()[0] = 9;

        blockWriter.WriteRow(row, nullptr, nullptr);

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data = codec->Compress(block.Data);
        Meta = block.Meta;
    }

    TKeyComparer KeyComparer_ = [] (TKey lhs, TKey rhs) {
        return CompareRows(lhs, rhs);
    };
};

TEST_F(TVersionedBlocksTestOneRow, ReadByTimestamp1)
{
    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> schemaIdMapping = {{5, 5}, {3, 6}, {4, 7}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        Schema.GetKeyColumnCount(),
        Schema.GetKeyColumnCount() + 2, // Two padding key columns.
        schemaIdMapping,
        KeyComparer_,
        7,
        false,
        true);

    auto row = TMutableVersionedRow::Allocate(&MemoryPool, 5, 3, 1, 0);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
    row.BeginKeys()[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);
    row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 6);
    row.BeginValues()[2] = MakeVersionedBooleanValue(true, 5, 7);
    row.BeginWriteTimestamps()[0] = 5;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    CheckResult(blockReader, rows);
}


TEST_F(TVersionedBlocksTestOneRow, ReadByTimestamp2)
{
    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 5}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        Schema.GetKeyColumnCount(),
        Schema.GetKeyColumnCount(),
        schemaIdMapping,
        KeyComparer_,
        9,
        false,
        true);

    auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 0, 0, 1);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginDeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    CheckResult(blockReader, rows);
}

TEST_F(TVersionedBlocksTestOneRow, ReadLastCommitted)
{
    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 3}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        Schema.GetKeyColumnCount(),
        Schema.GetKeyColumnCount(),
        schemaIdMapping,
        KeyComparer_,
        SyncLastCommittedTimestamp,
        false,
        true);

    auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 0, 1, 1);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginWriteTimestamps()[0] = 11;
    row.BeginDeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    CheckResult(blockReader, rows);
}

TEST_F(TVersionedBlocksTestOneRow, ReadAllCommitted)
{
    // Read only last non-key column.
    std::vector<TColumnIdMapping> schemaIdMapping = {{5, 3}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        Schema.GetKeyColumnCount(),
        Schema.GetKeyColumnCount(),
        schemaIdMapping,
        KeyComparer_,
        AllCommittedTimestamp,
        true,
        true);

    auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 1, 3, 1);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

    // v2
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 3);

    row.BeginWriteTimestamps()[2] = 3;
    row.BeginWriteTimestamps()[1] = 5;
    row.BeginWriteTimestamps()[0] = 11;

    row.BeginDeleteTimestamps()[0] = 9;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    CheckResult(blockReader, rows);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
