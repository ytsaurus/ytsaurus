#include "stdafx.h"
#include "framework.h"

#include "versioned_table_client_ut.h"

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>
#include <ytlib/new_table_client/versioned_chunk_writer.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_writer.h>
#include <ytlib/new_table_client/cached_versioned_chunk_meta.h>

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/transaction_client/public.h>

#include <core/compression/public.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

Stroka A("a");
Stroka B("b");

class TVersionedChunksTest
    : public TVersionedTableClientTestBase
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

        MemoryWriter = New<TMemoryWriter>();
        MemoryWriter->Open();

        ChunkWriter = CreateVersionedChunkWriter(
            New<TChunkWriterConfig>(),
            New<TChunkWriterOptions>(),
            Schema,
            KeyColumns,
            MemoryWriter);

        EXPECT_TRUE(ChunkWriter->Open().Get().IsOK());
    }

    TTableSchema Schema;
    TKeyColumns KeyColumns;

    IVersionedReaderPtr ChunkReader;
    IVersionedWriterPtr ChunkWriter;

    IReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    TChunkedMemoryPool MemoryPool;

    void CheckResult(const std::vector<TVersionedRow>& expected, const std::vector<TVersionedRow>& actual)
    {
        EXPECT_EQ(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            ExpectRowsEqual(expected[i], actual[i]);
        }
    }

    void FillKey(TVersionedRow row, TNullable<Stroka> k1, TNullable<i64> k2, TNullable<double> k3)
    {
        row.BeginKeys()[0] = k1 
            ? MakeUnversionedStringValue(*k1, 0)
            : MakeUnversionedSentinelValue(EValueType::Null, 0);
        row.BeginKeys()[1] = k2 
            ? MakeUnversionedInt64Value(*k2, 1)
            : MakeUnversionedSentinelValue(EValueType::Null, 1);
        row.BeginKeys()[2] = k3 
            ? MakeUnversionedDoubleValue(*k3, 2)
            : MakeUnversionedSentinelValue(EValueType::Null, 2);
    }

    void WriteThreeRows()
    {
        std::vector<TVersionedRow> rows;
        {
            TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 3, 3);
            FillKey(row, MakeNullable(A), MakeNullable(1), MakeNullable(1.5));

            // v1
            row.BeginValues()[0] = MakeVersionedIntegerValue(8, 11, 3);
            row.BeginValues()[1] = MakeVersionedIntegerValue(7, 3, 3);
            // v2
            row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

            row.BeginTimestamps()[0] = 11;
            row.BeginTimestamps()[1] = 9 | TombstoneTimestampMask;
            row.BeginTimestamps()[2] = 3;

            rows.push_back(row);
        } {
            TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 3, 1);
            FillKey(row, MakeNullable(A), MakeNullable(2), Null);

            // v1
            row.BeginValues()[0] = MakeVersionedIntegerValue(2, 1, 3);
            // v2
            row.BeginValues()[1] = MakeVersionedIntegerValue(100, 10, 4);
            row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

            row.BeginTimestamps()[0] = 1;

            rows.push_back(row);
        } {
            TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 5, 3);
            FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));

            // v1
            row.BeginValues()[0] = MakeVersionedIntegerValue(9, 15, 3);
            row.BeginValues()[1] = MakeVersionedIntegerValue(8, 12, 3);
            row.BeginValues()[2] = MakeVersionedIntegerValue(7, 3, 3);
            // v2
            row.BeginValues()[3] = MakeVersionedSentinelValue(EValueType::Null, 12, 4);
            row.BeginValues()[4] = MakeVersionedSentinelValue(EValueType::Null, 8, 4);

            row.BeginTimestamps()[0] = 20 | TombstoneTimestampMask;
            row.BeginTimestamps()[1] = 3;
            row.BeginTimestamps()[2] = 2 | TombstoneTimestampMask;

            rows.push_back(row);
        }

        ChunkWriter->Write(rows);

        GetRowAndResetWriter();
    }

    void GetRowAndResetWriter()
    {
        EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

        // Initialize reader.
        MemoryReader = CreateMemoryReader(
            MemoryWriter->GetChunkMeta(),
            MemoryWriter->GetBlocks());
    }

    int CreateManyRows(std::vector<TVersionedRow>* rows, int startIndex)
    {
        const int N = 100000;
        for (int i = 0; i < N; ++i) {
            TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 3, 3);
            FillKey(row, MakeNullable(A), MakeNullable(startIndex + i), Null);

            // v1
            row.BeginValues()[0] = MakeVersionedIntegerValue(8, 11, 3);
            row.BeginValues()[1] = MakeVersionedIntegerValue(7, 3, 3);
            // v2
            row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

            row.BeginTimestamps()[0] = 11;
            row.BeginTimestamps()[1] = 9 | TombstoneTimestampMask;
            row.BeginTimestamps()[2] = 3;

            rows->push_back(row);
        }
        return startIndex + N;
    }

    void WriteManyRows()
    {
        int startIndex = 0;
        for (int i = 0; i < 3; ++i) {
            std::vector<TVersionedRow> rows;
            startIndex = CreateManyRows(&rows, startIndex);
            ChunkWriter->Write(rows);
        }

        GetRowAndResetWriter();
    }

};

TEST_F(TVersionedChunksTest, ReadEmptyWiderSchema)
{
    std::vector<TVersionedRow> expected;
    WriteThreeRows();

    auto schema = Schema;
    schema.Columns().push_back(TColumnSchema("kN", EValueType::Double));

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        schema,
        KeyColumns).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(B, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(15, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.GetRowAndReset());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_FALSE(chunkReader->Read(&actual));

    CheckResult(expected, actual);
}

TEST_F(TVersionedChunksTest, ReadLastCommitted)
{
    std::vector<TVersionedRow> expected;
    {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 1, 1);
        FillKey(row, MakeNullable(A), MakeNullable(1), MakeNullable(1.5));

        // v1
        row.BeginValues()[0] = MakeVersionedIntegerValue(8, 11, 3);
        row.BeginTimestamps()[0] = 11;

        expected.push_back(row);
    } {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 2, 1);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);

        // v1
        row.BeginValues()[0] = MakeVersionedIntegerValue(2, 1, 3);
        // v2
        row.BeginValues()[1] = MakeVersionedIntegerValue(100, 10, 4);

        row.BeginTimestamps()[0] = 1 | IncrementalTimestampMask;

        expected.push_back(row);
    } {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginTimestamps()[0] = 20 | TombstoneTimestampMask;

        expected.push_back(row);
    }

    WriteThreeRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema,
        KeyColumns).Get().ValueOrThrow();

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        TReadLimit(),
        TReadLimit(),
        TColumnFilter());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_FALSE(chunkReader->Read(&actual));

    CheckResult(expected, actual);
}

TEST_F(TVersionedChunksTest, ReadByTimestamp)
{
    std::vector<TVersionedRow> expected;
    {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 1, 1);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);

        // v1
        row.BeginValues()[0] = MakeVersionedIntegerValue(2, 1, 3);
        row.BeginTimestamps()[0] = 1 | IncrementalTimestampMask;

        expected.push_back(row);
    } {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginTimestamps()[0] = 2 | TombstoneTimestampMask;

        expected.push_back(row);
    }

    WriteThreeRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema,
        KeyColumns).Get().ValueOrThrow();

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        TReadLimit(),
        TReadLimit(),
        TColumnFilter(),
        2); // timestamp

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_FALSE(chunkReader->Read(&actual));

    CheckResult(expected, actual);
}

TEST_F(TVersionedChunksTest, ReadAllLimitsSchema)
{
    std::vector<TVersionedRow> expected;
    {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 1, 1);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);

        // v2
        row.BeginValues()[0] = MakeVersionedIntegerValue(100, 10, 3);

        row.BeginTimestamps()[0] = 1 | IncrementalTimestampMask;

        expected.push_back(row);
    } {
        TVersionedRow row = TVersionedRow::Allocate(&MemoryPool, 3, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginTimestamps()[0] = 20 | TombstoneTimestampMask;

        expected.push_back(row);
    }

    WriteThreeRows();

    TTableSchema schema;
    schema.Columns().emplace_back("k1", EValueType::String);
    schema.Columns().emplace_back("k2", EValueType::Int64);
    schema.Columns().emplace_back("k3", EValueType::Double);
    schema.Columns().emplace_back("v2", EValueType::Int64);

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        schema,
        KeyColumns).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(1, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.GetRowAndReset());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_FALSE(chunkReader->Read(&actual));

    CheckResult(expected, actual);
}

TEST_F(TVersionedChunksTest, ReadEmpty)
{
    std::vector<TVersionedRow> expected;
    WriteThreeRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema,
        KeyColumns).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(B, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(15, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.GetRowAndReset());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_FALSE(chunkReader->Read(&actual));

    CheckResult(expected, actual);
}

TEST_F(TVersionedChunksTest, ReadManyRows)
{
    std::vector<TVersionedRow> expected;
    int startIndex = 0;
    for (int i = 0; i < 3; ++i) {
        std::vector<TVersionedRow> rows;
        startIndex = CreateManyRows(&rows, startIndex);
        expected.insert(expected.end(), rows.begin(), rows.end());
    }

    WriteManyRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema,
        KeyColumns).Get().ValueOrThrow();

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        chunkMeta,
        TReadLimit(),
        TReadLimit(),
        TColumnFilter(),
        AllCommittedTimestamp);

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    auto it = expected.begin();
    std::vector<TVersionedRow> actual;
    actual.reserve(1000);

    while (chunkReader->Read(&actual)) {
        if (actual.empty()) {
            EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
            continue;
        }
        std::vector<TVersionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
