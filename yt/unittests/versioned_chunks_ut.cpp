#include "framework.h"
#include "versioned_table_client_ut.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/versioned_writer.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/public.h>

namespace NYT {
namespace NTableClient {
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
    TTableSchema Schema;
    TKeyColumns KeyColumns;

    IVersionedReaderPtr ChunkReader;
    IVersionedWriterPtr ChunkWriter;

    IChunkReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    TChunkedMemoryPool MemoryPool;

    virtual void SetUp() override
    {
        Schema = TTableSchema({
            TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64)
        });

        MemoryWriter = New<TMemoryWriter>();
        ChunkWriter = CreateVersionedChunkWriter(
            New<TChunkWriterConfig>(),
            New<TChunkWriterOptions>(),
            Schema,
            MemoryWriter);

        EXPECT_TRUE(ChunkWriter->Open().Get().IsOK());
    }

    void CheckResult(const std::vector<TVersionedRow>& expected, IVersionedReaderPtr reader)
    {
        auto it = expected.begin();
        std::vector<TVersionedRow> actual;
        actual.reserve(1000);

        while (reader->Read(&actual)) {
            if (actual.empty()) {
                EXPECT_TRUE(reader->GetReadyEvent().Get().IsOK());
                continue;
            }
            std::vector<TVersionedRow> ex(it, it + actual.size());
            CheckResult(ex, actual);
            it += actual.size();
        }

        EXPECT_TRUE(it == expected.end());
    }

    void CheckResult(const std::vector<TVersionedRow>& expected, const std::vector<TVersionedRow>& actual)
    {
        EXPECT_EQ(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            ExpectRowsEqual(expected[i], actual[i]);
        }
    }

    void FillKey(TMutableVersionedRow row, TNullable<Stroka> k1, TNullable<i64> k2, TNullable<double> k3)
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
            auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 3, 3, 1);
            FillKey(row, MakeNullable(A), MakeNullable(1), MakeNullable(1.5));

            // v1
            row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
            row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 3);
            // v2
            row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

            row.BeginWriteTimestamps()[2] = 3;
            row.BeginWriteTimestamps()[1] = 5;
            row.BeginWriteTimestamps()[0] = 11;

            row.BeginDeleteTimestamps()[0] = 9;

            rows.push_back(row);
        } {
            auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 3, 3, 0);
            FillKey(row, MakeNullable(A), MakeNullable(2), Null);

            // v1
            row.BeginValues()[0] = MakeVersionedInt64Value(2, 1, 3);
            // v2
            row.BeginValues()[1] = MakeVersionedInt64Value(100, 10, 4);
            row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

            row.BeginWriteTimestamps()[2] = 1;
            row.BeginWriteTimestamps()[1] = 5;
            row.BeginWriteTimestamps()[0] = 10;

            rows.push_back(row);
        } {
            auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 5, 4, 2);
            FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));

            // v1
            row.BeginValues()[0] = MakeVersionedInt64Value(9, 15, 3);
            row.BeginValues()[1] = MakeVersionedInt64Value(8, 12, 3);
            row.BeginValues()[2] = MakeVersionedInt64Value(7, 3, 3);
            // v2
            row.BeginValues()[3] = MakeVersionedSentinelValue(EValueType::Null, 12, 4);
            row.BeginValues()[4] = MakeVersionedSentinelValue(EValueType::Null, 8, 4);

            row.BeginWriteTimestamps()[3] = 3;
            row.BeginWriteTimestamps()[2] = 8;
            row.BeginWriteTimestamps()[1] = 12;
            row.BeginWriteTimestamps()[0] = 15;

            row.BeginDeleteTimestamps()[1] = 2;
            row.BeginDeleteTimestamps()[0] = 20;

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

    TVersionedRow CreateSingleRow(int index)
    {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 3, 3, 1);
        FillKey(row, MakeNullable(A), MakeNullable(index), Null);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
        row.BeginValues()[1] = MakeVersionedInt64Value(7, 3, 3);
        // v2
        row.BeginValues()[2] = MakeVersionedSentinelValue(EValueType::Null, 5, 4);

        row.BeginWriteTimestamps()[2] = 3;
        row.BeginWriteTimestamps()[1] = 5;
        row.BeginWriteTimestamps()[0] = 11;

        row.BeginDeleteTimestamps()[0] = 9;
        return row;
    }

    int CreateManyRows(std::vector<TVersionedRow>* rows, int startIndex)
    {
        const int N = 100000;
        for (int i = 0; i < N; ++i) {
            TVersionedRow row = CreateSingleRow(startIndex + i);
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

    TKeyComparer KeyComparer_ = [] (TKey lhs, TKey rhs) {
        return CompareRows(lhs, rhs);
    };
};

TEST_F(TVersionedChunksTest, ReadEmptyWiderSchema)
{
    std::vector<TVersionedRow> expected;
    WriteThreeRows();

    auto schema = Schema;
    schema.AppendColumn(TColumnSchema("kN", EValueType::Double));

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        schema).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(B, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(15, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.FinishRow());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter(),
        New<TChunkReaderPerformanceCounters>());

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
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 4, 1, 1, 1);
        FillKey(row, MakeNullable(A), MakeNullable(1), MakeNullable(1.5));
        row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
        row.BeginWriteTimestamps()[0] = 11;
        row.BeginDeleteTimestamps()[0] = 9;

        expected.push_back(row);
    } {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 4, 2, 1, 0);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);
        row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(2, 1, 3);
        // v2
        row.BeginValues()[1] = MakeVersionedInt64Value(100, 10, 4);

        row.BeginWriteTimestamps()[0] = 10;

        expected.push_back(row);
    } {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 4, 0, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginKeys()[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
        row.BeginDeleteTimestamps()[0] = 20;

        expected.push_back(row);
    }

    WriteThreeRows();

    TTableSchema schema({
        TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("kN", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("v1", EValueType::Int64),
        TColumnSchema("v2", EValueType::Int64),
        TColumnSchema("vN", EValueType::Double)
    });

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        schema).Get().ValueOrThrow();

    TColumnFilter filter;
    filter.All = false;
    filter.Indexes = {1, 2, 3, 4, 5, 6};

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        TReadLimit(),
        TReadLimit(),
        filter,
        New<TChunkReaderPerformanceCounters>());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_TRUE(chunkReader->Read(&actual));
    CheckResult(expected, actual);

    EXPECT_FALSE(chunkReader->Read(&actual));
    EXPECT_TRUE(actual.empty());
}

TEST_F(TVersionedChunksTest, ReadByTimestamp)
{
    std::vector<TVersionedRow> expected;
    {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 1, 1, 0);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);

        // v1
        row.BeginValues()[0] = MakeVersionedInt64Value(2, 1, 3);
        row.BeginWriteTimestamps()[0] = 1;

        expected.push_back(row);
    } {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 0, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginDeleteTimestamps()[0] = 2;

        expected.push_back(row);
    }

    WriteThreeRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema).Get().ValueOrThrow();

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        TReadLimit(),
        TReadLimit(),
        TColumnFilter(),
        New<TChunkReaderPerformanceCounters>(),
        2); // timestamp

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_TRUE(chunkReader->Read(&actual));
    CheckResult(expected, actual);

    EXPECT_FALSE(chunkReader->Read(&actual));
    EXPECT_TRUE(actual.empty());
}

TEST_F(TVersionedChunksTest, ReadAllLimitsSchema)
{
    std::vector<TVersionedRow> expected;
    {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 1, 1, 0);
        FillKey(row, MakeNullable(A), MakeNullable(2), Null);

        // v2
        row.BeginValues()[0] = MakeVersionedInt64Value(100, 10, 3);

        row.BeginWriteTimestamps()[0] = 10;

        expected.push_back(row);
    } {
        auto row = TMutableVersionedRow::Allocate(&MemoryPool, 3, 0, 0, 1);
        FillKey(row, MakeNullable(B), MakeNullable(1), MakeNullable(1.5));
        row.BeginDeleteTimestamps()[0] = 20;

        expected.push_back(row);
    }

    WriteThreeRows();

    TTableSchema schema({
        TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("v2", EValueType::Int64)
    });

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        schema).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(1, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.FinishRow());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter(),
        New<TChunkReaderPerformanceCounters>());

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> actual;
    actual.reserve(10);

    EXPECT_TRUE(chunkReader->Read(&actual));
    CheckResult(expected, actual);

    EXPECT_FALSE(chunkReader->Read(&actual));
    EXPECT_TRUE(actual.empty());
}

TEST_F(TVersionedChunksTest, ReadEmpty)
{
    std::vector<TVersionedRow> expected;
    WriteThreeRows();

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        Schema).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder lowerKeyBuilder;
    lowerKeyBuilder.AddValue(MakeUnversionedStringValue(B, 0));
    lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(15, 1));
    lowerKeyBuilder.AddValue(MakeUnversionedDoubleValue(2, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerKeyBuilder.FinishRow());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        std::move(lowerLimit),
        TReadLimit(),
        TColumnFilter(),
        New<TChunkReaderPerformanceCounters>());

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
        Schema).Get().ValueOrThrow();

    {
        auto chunkReader = CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            MemoryReader,
            GetNullBlockCache(),
            chunkMeta,
            TReadLimit(),
            TReadLimit(),
            TColumnFilter(),
            New<TChunkReaderPerformanceCounters>(),
            AllCommittedTimestamp);

        EXPECT_TRUE(chunkReader->Open().Get().IsOK());
        CheckResult(expected, chunkReader);
    }

    {
        int firstRow = 250000;
        TUnversionedOwningRowBuilder lowerKeyBuilder;
        lowerKeyBuilder.AddValue(MakeUnversionedStringValue(A, 0));
        lowerKeyBuilder.AddValue(MakeUnversionedInt64Value(firstRow, 1));

        TReadLimit lowerLimit;
        lowerLimit.SetKey(lowerKeyBuilder.FinishRow());

        auto chunkReader = CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            MemoryReader,
            GetNullBlockCache(),
            chunkMeta,
            lowerLimit,
            TReadLimit(),
            TColumnFilter(),
            New<TChunkReaderPerformanceCounters>(),
            AllCommittedTimestamp);

        EXPECT_TRUE(chunkReader->Open().Get().IsOK());

        expected.erase(expected.begin(), expected.begin() + firstRow);
        CheckResult(expected, chunkReader);
    }

    {
        std::vector<TOwningKey> owningKeys;
        TUnversionedOwningRowBuilder builder;
        expected.clear();

        // Before the first key.
        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 1));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        owningKeys.push_back(builder.FinishRow());
        expected.push_back(TVersionedRow());

        // The first key.
        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(0, 1));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        owningKeys.push_back(builder.FinishRow());
        expected.push_back(CreateSingleRow(0));

        // Somewhere in the middle.
        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(150000, 1));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        owningKeys.push_back(builder.FinishRow());
        expected.push_back(CreateSingleRow(150000));

        // After the last key.
        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(350000, 1));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        owningKeys.push_back(builder.FinishRow());
        expected.push_back(TVersionedRow());

        // After the previous key, that was after the last.
        builder.AddValue(MakeUnversionedStringValue(B, 0));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 1));
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        owningKeys.push_back(builder.FinishRow());
        expected.push_back(TVersionedRow());

        std::vector<TKey> keys;
        for (const auto& owningKey : owningKeys) {
            keys.push_back(owningKey.Get());
        }

        auto sharedKeys = MakeSharedRange(std::move(keys), std::move(owningKeys));

        auto chunkReader = CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            MemoryReader,
            GetNullBlockCache(),
            chunkMeta,
            sharedKeys,
            TColumnFilter(),
            New<TChunkReaderPerformanceCounters>(),
            KeyComparer_,
            AllCommittedTimestamp);

        EXPECT_TRUE(chunkReader->Open().Get().IsOK());
        CheckResult(expected, chunkReader);
    }
}

TEST_F(TVersionedChunksTest, WideSchemaBoundaryRow)
{
    std::vector<TVersionedRow> rows;
    rows.push_back(CreateSingleRow(0));
    ChunkWriter->Write(rows);
    GetRowAndResetWriter();

    TTableSchema widerSchema({
        TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("k4", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("v1", EValueType::Int64),
    });

    auto chunkMeta = TCachedVersionedChunkMeta::Load(
        MemoryReader,
        widerSchema).Get().ValueOrThrow();

    TUnversionedOwningRowBuilder keyBuilder;
    keyBuilder.AddValue(rows.front().BeginKeys()[0]);
    keyBuilder.AddValue(rows.front().BeginKeys()[1]);
    keyBuilder.AddValue(rows.front().BeginKeys()[2]);
    keyBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 3));
    TReadLimit limit;
    limit.SetKey(keyBuilder.FinishRow());

    auto chunkReader = CreateVersionedChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        GetNullBlockCache(),
        chunkMeta,
        TReadLimit(),
        limit,
        TColumnFilter(),
        New<TChunkReaderPerformanceCounters>(),
        AllCommittedTimestamp);

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    std::vector<TVersionedRow> expected;
    CheckResult(expected, chunkReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
