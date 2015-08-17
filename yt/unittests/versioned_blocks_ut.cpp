#include "stdafx.h"
#include "framework.h"

#include "versioned_table_client_ut.h"

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/versioned_block_writer.h>
#include <ytlib/table_client/versioned_block_reader.h>

#include <ytlib/transaction_client/public.h>

#include <core/compression/codec.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NTransactionClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlocksTestBase
    : public TVersionedTableClientTestBase
{
protected:
    void CheckResult(TSimpleVersionedBlockReader& reader, const std::vector<TVersionedRow>& rows)
    {
        int i = 0;
        do {
            EXPECT_LT(i, rows.size());
            auto row = reader.GetRow(&MemoryPool);
            ExpectRowsEqual(rows[i], row);
        } while (reader.NextRow());
    }

    TTableSchema Schema;
    TKeyColumns KeyColumns;

    TSharedRef Data;
    NProto::TBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlocksTestOneRow
    :public TVersionedBlocksTestBase
{
protected:
    virtual void SetUp() override
    {
        Schema.Columns() = {
            TColumnSchema("k1", EValueType::String),
            TColumnSchema("k2", EValueType::Int64),
            TColumnSchema("k3", EValueType::Double),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64)
        };

        KeyColumns = {"k1", "k2", "k3"};

        TSimpleVersionedBlockWriter blockWriter(Schema, KeyColumns);

        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 3, 3, 1);
        row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
        row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
        row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
        row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 3);
        // v2
        row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

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

};

TEST_F(TVersionedBlocksTestOneRow, ReadByTimestamp1)
{
    // Reorder value columns in reading schema.
    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 5}, {3, 6}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        KeyColumns.size(),
        KeyColumns.size() + 2, // Two padding key columns.
        schemaIdMapping,
        7);

    TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 5, 2, 1, 0);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
    row.BeginKeys()[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 5);
    row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 6);
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
        KeyColumns.size(),
        KeyColumns.size(),
        schemaIdMapping,
        9);

    TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 0, 0, 1);
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
        KeyColumns.size(),
        KeyColumns.size(),
        schemaIdMapping,
        SyncLastCommittedTimestamp);

    TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 0, 1, 1);
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
    std::vector<TColumnIdMapping> schemaIdMapping = {{4, 3}};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        KeyColumns.size(),
        KeyColumns.size(),
        schemaIdMapping,
        AllCommittedTimestamp);

    TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 1, 3, 1);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedInt64Value(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

    // v2
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

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
