#include "stdafx.h"
#include "framework.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/versioned_block_writer.h>
#include <ytlib/new_table_client/versioned_block_reader.h>

#include <ytlib/transaction_client/public.h>

//#include <ytlib/new_table_client/chunk_reader.h>
//#include <ytlib/new_table_client/chunk_writer.h>
//#include <ytlib/new_table_client/name_table.h>
//#include <ytlib/new_table_client/reader.h>
//#include <ytlib/new_table_client/unversioned_row.h>

//#include <ytlib/chunk_client/memory_reader.h>
//#include <ytlib/chunk_client/memory_writer.h>

#include <core/compression/codec.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using namespace NTransactionClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlockTestBase
    : public ::testing::Test
{
protected:
    void CheckRowEquals(TVersionedRow expected, TVersionedRow actual)
    {
        if (!expected) {
            EXPECT_FALSE(actual);
            return;
        }

        EXPECT_EQ(0, CompareRows(expected.BeginKeys(), expected.EndKeys(), actual.BeginKeys(), actual.EndKeys()));
        EXPECT_EQ(expected.GetTimestampCount(), actual.GetTimestampCount());
        for (int i = 0; i < expected.GetTimestampCount(); ++i) {
            EXPECT_EQ(expected.BeginTimestamps()[i], actual.BeginTimestamps()[i]);
        }

        EXPECT_EQ(expected.GetValueCount(), actual.GetValueCount());
        for (int i = 0; i < expected.GetValueCount(); ++i) {
            EXPECT_EQ(CompareRowValues(expected.BeginValues()[i], actual.BeginValues()[i]), 0);
            EXPECT_EQ(expected.BeginValues()[i].Timestamp, actual.BeginValues()[i].Timestamp);
        }
    }

    void CheckResult(TSimpleVersionedBlockReader& reader, const std::vector<TVersionedRow>& rows)
    {
        int i = 0;
        do {
            EXPECT_LT(i, rows.size());
            auto row = reader.GetRow(&MemoryPool);
            CheckRowEquals(rows[i], row);
        } while (reader.NextRow());
    }

    TTableSchema Schema;
    TKeyColumns KeyColumns;

    TSharedRef Data;
    NProto::TBlockMeta Meta;

    TChunkedMemoryPool MemoryPool;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlockTestOneRow
    :public TVersionedBlockTestBase
{
protected:
    virtual void SetUp() override
    {
        Schema.Columns() = {
            TColumnSchema("k1", EValueType::String),
            TColumnSchema("k2", EValueType::Integer),
            TColumnSchema("k3", EValueType::Double),
            TColumnSchema("v1", EValueType::Integer),
            TColumnSchema("v2", EValueType::Integer)
        };

        KeyColumns = {"k1", "k2", "k3"};

        TSimpleVersionedBlockWriter blockWriter(Schema, KeyColumns);

        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 3, 3);
        row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
        row.BeginKeys()[1] = MakeUnversionedIntegerValue(1, 1);
        row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);

        // v1
        row.BeginValues()[0] = MakeVersionedIntegerValue(8, 11, 3);
        row.BeginValues()[1] = MakeVersionedIntegerValue(7, 3, 3);
        // v2
        row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

        row.BeginTimestamps()[0] = 11;
        row.BeginTimestamps()[1] = 9 | TombstoneTimestampMask;
        row.BeginTimestamps()[2] = 3;

        blockWriter.WriteRow(row, nullptr, nullptr);

        auto block = blockWriter.FlushBlock();
        auto* codec = GetCodec(ECodec::None);

        Data = codec->Compress(block.Data);
        Meta = block.Meta;
    }

};

TEST_F(TVersionedBlockTestOneRow, ReadByTimestamp)
{
    // Reorder value columns in reading schema.
    std::vector<int> schemaIdMapping = {0, 1, 2, 4, 3};

    TSimpleVersionedBlockReader blockReader(
        Data,
        Meta,
        Schema,
        KeyColumns,
        schemaIdMapping,
        7);

    TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 2, 1);
    row.BeginKeys()[0] = MakeUnversionedStringValue("a", 0);
    row.BeginKeys()[1] = MakeUnversionedIntegerValue(1, 1);
    row.BeginKeys()[2] = MakeUnversionedDoubleValue(1.5, 2);
    row.BeginValues()[0] = MakeVersionedSentinelValue(EValueType::Null, 5, 3);
    row.BeginValues()[1] = MakeVersionedIntegerValue(7, 3, 4);
    row.BeginTimestamps()[0] = 3 | IncrementalTimestampMask;

    std::vector<TVersionedRow> rows;
    rows.push_back(row);

    CheckResult(blockReader, rows);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
