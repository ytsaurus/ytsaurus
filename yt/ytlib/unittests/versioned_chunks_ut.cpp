#include <util/random/shuffle.h>
#include <yt/core/test_framework/framework.h>
#include "table_client_helpers.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/versioned_writer.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/public.h>
#include <yt/core/misc/random.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TString A("a");
TString B("b");

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): rewrite this legacy test.
class TVersionedChunksLookupTestBase
    : public ::testing::Test
{
protected:
    TTableSchema Schema;

    IVersionedReaderPtr ChunkReader;
    IVersionedWriterPtr ChunkWriter;

    IChunkReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    TChunkedMemoryPool MemoryPool;

    EOptimizeFor OptimizeFor;

    explicit TVersionedChunksLookupTestBase(EOptimizeFor optimizeFor)
        : Schema({
            TColumnSchema("k1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Double)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64)
        })
        , OptimizeFor(optimizeFor)
    { }

    virtual void SetUp() override
    {
        MemoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 1025;
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = OptimizeFor;
        ChunkWriter = CreateVersionedChunkWriter(
            config,
            options,
            Schema,
            MemoryWriter);

        EXPECT_TRUE(ChunkWriter->Open().Get().IsOK());
    }

    void GetRowAndResetWriter()
    {
        EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

        // Initialize reader.
        MemoryReader = CreateMemoryReader(
            MemoryWriter->GetChunkMeta(),
            MemoryWriter->GetBlocks());
    }

    void FillKey(TMutableVersionedRow row, TNullable<TString> k1, TNullable<i64> k2, TNullable<double> k3)
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

    void DoTest()
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
            TWorkloadDescriptor(),
            TReadSessionId(),
            Schema).Get().ValueOrThrow();
        {
            TChunkedMemoryPool pool;
            TUnversionedOwningRowBuilder builder;

            std::vector<TOwningKey> owningKeys;

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

            auto row = TMutableVersionedRow::Allocate(&pool, 3, 1, 1, 1);
            FillKey(row, MakeNullable(A), MakeNullable(0), Null);
            row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
            row.BeginWriteTimestamps()[0] = 11;
            row.BeginDeleteTimestamps()[0] = 9;
            expected.push_back(row);

            // Somewhere in the middle.
            builder.AddValue(MakeUnversionedStringValue(A, 0));
            builder.AddValue(MakeUnversionedInt64Value(150000, 1));
            builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
            owningKeys.push_back(builder.FinishRow());

            row = TMutableVersionedRow::Allocate(&pool, 3, 1, 1, 1);
            FillKey(row, MakeNullable(A), MakeNullable(150000), Null);
            row.BeginValues()[0] = MakeVersionedInt64Value(8, 11, 3);
            row.BeginWriteTimestamps()[0] = 11;
            row.BeginDeleteTimestamps()[0] = 9;
            expected.push_back(row);

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
                keys.push_back(owningKey);
            }

            auto sharedKeys = MakeSharedRange(std::move(keys), std::move(owningKeys));

            auto chunkState = New<TChunkState>(
                GetNullBlockCache(),
                TChunkSpec(),
                nullptr,
                nullptr,
                New<TChunkReaderPerformanceCounters>(),
                KeyComparer_);
            auto chunkReader = CreateVersionedChunkReader(
                New<TChunkReaderConfig>(),
                MemoryReader,
                std::move(chunkState),
                std::move(chunkMeta),
                TReadSessionId(),
                sharedKeys,
                TColumnFilter(),
                MaxTimestamp,
                false);

            EXPECT_TRUE(chunkReader->Open().Get().IsOK());
            EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
            CheckResult(&expected, chunkReader);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedChunksLookupTest
    : public TVersionedChunksLookupTestBase
{
public:
    TSimpleVersionedChunksLookupTest()
        : TVersionedChunksLookupTestBase(EOptimizeFor::Lookup)
    { }
};


TEST_F(TSimpleVersionedChunksLookupTest, Test)
{
    DoTest();
}

////////////////////////////////////////////////////////////////////////////////

class TColumnarVersionedChunksLookupTest
    : public TVersionedChunksLookupTestBase
{
public:
    TColumnarVersionedChunksLookupTest()
        : TVersionedChunksLookupTestBase(EOptimizeFor::Scan)
    { }
};

TEST_F(TColumnarVersionedChunksLookupTest, Test)
{
    DoTest();
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunksHeavyTest
    : public ::testing::Test
{
public:
    TVersionedChunksHeavyTest()
        : RowBuffer_(New<TRowBuffer>())
    {
        ColumnSchemas_ = {
            TColumnSchema("k0", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k1", EValueType::Uint64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k4", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending),

            TColumnSchema("v1", EValueType::Int64).SetAggregate(TString("min")),
            TColumnSchema("v2", EValueType::Int64),
            TColumnSchema("v3", EValueType::Uint64),
            TColumnSchema("v4", EValueType::String),
            TColumnSchema("v5", EValueType::Double),
            TColumnSchema("v6", EValueType::Boolean)
        };

        InitialRows_ = CreateRows();
    }

protected:
    std::vector<TColumnSchema> ColumnSchemas_;

    std::vector<TString> StringData_;

    TRowBufferPtr RowBuffer_;

    std::vector<TVersionedRow> InitialRows_;


    void TestRangeReader(
        EOptimizeFor optimizeFor,
        TTableSchema writeSchema,
        TTableSchema readSchema,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        bool produceAllVersions)
    {
        auto expected = CreateExpected(InitialRows_, writeSchema, readSchema, lowerKey, upperKey, timestamp, produceAllVersions);

        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 1025;
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        auto chunkWriter = CreateVersionedChunkWriter(
            config,
            options,
            writeSchema,
            memoryWriter);

        EXPECT_TRUE(chunkWriter->Open().Get().IsOK());

        chunkWriter->Write(InitialRows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        auto memoryReader = CreateMemoryReader(
            memoryWriter->GetChunkMeta(),
            memoryWriter->GetBlocks());

        auto chunkMeta = TCachedVersionedChunkMeta::Load(
            memoryReader,
            TWorkloadDescriptor(),
            TReadSessionId(),
            readSchema).Get().ValueOrThrow();

        auto chunkState = New<TChunkState>(
            GetNullBlockCache(),
            TChunkSpec(),
            nullptr,
            nullptr,
            New<TChunkReaderPerformanceCounters>(),
            nullptr);
        auto chunkReader = CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            memoryReader,
            std::move(chunkState),
            std::move(chunkMeta),
            TReadSessionId(),
            lowerKey,
            upperKey,
            TColumnFilter(),
            timestamp,
            produceAllVersions);

        CheckResult(&expected, chunkReader);
    }

    TString NextStringValue(std::vector<char>& value)
    {
        int index = value.size() - 1;
        while (index >= 0) {
            if (value[index] < 'z') {
                ++value[index];
                return TString(value.data(), value.size());
            } else {
                value[index] = 'a';
                --index;
            }
        }
        Y_UNREACHABLE();
    }

    std::vector<TVersionedRow> CreateRows(int count = 10000)
    {
        std::vector<char> stringValue(100, 'a');
        srand(0);

        std::vector<TTimestamp> timestamps = {10, 20, 30, 40, 50, 60, 70, 80, 90};

        std::vector<TVersionedRow> rows;
        for (int rowIndex = 0; rowIndex < count; ++rowIndex) {
            TVersionedRowBuilder builder(RowBuffer_);
            builder.AddKey(MakeUnversionedInt64Value(-10 + (rowIndex / 16), 0)); // k0

            builder.AddKey(MakeUnversionedUint64Value((rowIndex / 8) * 128, 1)); // k1

            if (rowIndex / 4 == 0) {
                StringData_.push_back(NextStringValue(stringValue));
            }
            builder.AddKey(MakeUnversionedStringValue(StringData_.back(), 2)); // k2

            builder.AddKey(MakeUnversionedDoubleValue((rowIndex / 2) * 3.14, 3)); // k3

            builder.AddKey(MakeUnversionedBooleanValue(rowIndex % 2 == 1, 4)); // k4

            Shuffle(timestamps.begin(), timestamps.end());
            int deleteTimestampCount = std::rand() % 3;
            std::vector<TTimestamp> deleteTimestamps(timestamps.begin(), timestamps.begin() + deleteTimestampCount);
            for (auto timestamp : deleteTimestamps) {
                builder.AddDeleteTimestamp(timestamp);
            }

            std::vector<TTimestamp> writeTimestamps(
                timestamps.begin() + deleteTimestampCount,
                timestamps.begin() + deleteTimestampCount + 3);

            // v1
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedInt64Value(-10 + (rowIndex / 16) + i, writeTimestamps[i], 5, i % 2));
            }

            // v2
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedInt64Value(-10 + (rowIndex / 16) + i, writeTimestamps[i], 6));
            }

            // v3
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedUint64Value(rowIndex * 10 + i, writeTimestamps[i], 7));
            }

            // v4
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedStringValue(StringData_.back(), writeTimestamps[i], 8));
            }

            // v5
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedDoubleValue(rowIndex * 3.14 + i, writeTimestamps[i], 9));
            }

            // v6
            for (int i = 0; i < std::rand() % 3; ++i) {
                builder.AddValue(MakeVersionedBooleanValue(i % 2 == 1, writeTimestamps[i], 10));
            }

            rows.push_back(builder.FinishRow());
        }

        return rows;
    }

    std::vector<TVersionedRow> CreateExpected(
        const std::vector<TVersionedRow>& rows,
        TTableSchema writeSchema,
        TTableSchema readSchema,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        bool produceAllVersions)
    {
        std::vector<TVersionedRow> expected;

        std::vector<int> idMapping(writeSchema.Columns().size(), -1);
        for (int i = 0; i < writeSchema.Columns().size(); ++i) {
            auto writeColumnSchema = writeSchema.Columns()[i];
            for (int j = 0; j < readSchema.Columns().size(); ++j) {
                auto readColumnSchema = readSchema.Columns()[j];
                if (writeColumnSchema.Name() == readColumnSchema.Name()) {
                    idMapping[i] = j;
                }
            }
        }

        for (auto row : rows) {
            std::vector<TUnversionedValue> key;
            YCHECK(row.GetKeyCount() <= readSchema.GetKeyColumnCount());
            for (int i = 0; i < row.GetKeyCount() && i < readSchema.GetKeyColumnCount(); ++i) {
                YCHECK(row.BeginKeys()[i].Type == readSchema.Columns()[i].GetPhysicalType());
                key.push_back(row.BeginKeys()[i]);
            }

            for (int i = row.GetKeyCount(); i < readSchema.GetKeyColumnCount(); ++i) {
                key.push_back(MakeUnversionedSentinelValue(EValueType::Null, i));
            }

            if (CompareRows(key.data(), key.data() + key.size(), lowerKey.Begin(), lowerKey.End()) < 0)
                continue;

            if (CompareRows(key.data(), key.data() + key.size(), upperKey.Begin(), upperKey.End()) >= 0)
                break;

            TVersionedRowBuilder builder(RowBuffer_, timestamp == AllCommittedTimestamp);
            for (const auto& value : key) {
                builder.AddKey(value);
            }

            if (timestamp == AllCommittedTimestamp) {
                for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                    builder.AddDeleteTimestamp(*deleteIt);
                }

                for (auto valueIt = row.BeginValues(); valueIt != row.EndValues(); ++valueIt) {
                    if (idMapping[valueIt->Id] > 0) {
                        auto value = *valueIt;
                        value.Id = idMapping[valueIt->Id];
                        builder.AddValue(value);
                    }
                }

                expected.push_back(builder.FinishRow());
            } else {
                // Find delete timestamp.
                TTimestamp deleteTimestamp = NullTimestamp;
                for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                    if (*deleteIt <= timestamp) {
                        deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
                    }
                }
                if (deleteTimestamp != NullTimestamp) {
                    builder.AddDeleteTimestamp(deleteTimestamp);
                }

                TTimestamp writeTimestamp = NullTimestamp;
                for (auto writeIt = row.BeginWriteTimestamps(); writeIt != row.EndWriteTimestamps(); ++writeIt) {
                    if (*writeIt <= timestamp && *writeIt > deleteTimestamp) {
                        writeTimestamp = std::max(*writeIt, writeTimestamp);
                    }
                }
                if (writeTimestamp != NullTimestamp) {
                    builder.AddWriteTimestamp(writeTimestamp);
                }

                std::vector<TVersionedValue> values;
                for (auto valueIt = row.BeginValues(); valueIt != row.EndValues(); ++valueIt) {
                    if (idMapping[valueIt->Id] > 0 && valueIt->Timestamp <= timestamp && valueIt->Timestamp > deleteTimestamp) {
                        auto value = *valueIt;
                        value.Id = idMapping[valueIt->Id];
                        values.push_back(value);
                    }
                }

                if (deleteTimestamp == NullTimestamp && writeTimestamp == NullTimestamp) {
                    // Row didn't exist at this timestamp.
                    expected.push_back(TVersionedRow());
                    continue;
                }

                std::sort(
                    values.begin(),
                    values.end(),
                    [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                        // Sort in reverse order
                        return lhs.Timestamp > rhs.Timestamp;
                    });

                std::set<int> usedIds;
                for (const auto& value : values) {
                    if (usedIds.insert(value.Id).second || readSchema.Columns()[value.Id].Aggregate()) {
                        builder.AddValue(value);
                    }
                }

                expected.push_back(builder.FinishRow());
            }
        }
        return expected;
    }

    void DoFullScanCompaction(EOptimizeFor optimizeFor)
    {
        auto writeSchema = TTableSchema(ColumnSchemas_);
        auto readSchema = TTableSchema(ColumnSchemas_);

        TestRangeReader(optimizeFor, writeSchema, readSchema, MinKey(), MaxKey(), AllCommittedTimestamp, true);
    }

    void DoTimestampFullScanExtraKeyColumn(EOptimizeFor optimizeFor, TTimestamp timestamp)
    {
        auto writeSchema = TTableSchema(ColumnSchemas_);

        auto columnSchemas = ColumnSchemas_;
        columnSchemas.insert(
            columnSchemas.begin() + 5,
            TColumnSchema("extraKey", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending));

        auto readSchema = TTableSchema(columnSchemas);

        TestRangeReader(optimizeFor, writeSchema, readSchema, MinKey(), MaxKey(), timestamp, false);
    }

    void DoEmptyReadWideSchema(EOptimizeFor optimizeFor)
    {
        auto writeSchema = TTableSchema(ColumnSchemas_);

        auto columnSchemas = ColumnSchemas_;
        columnSchemas.insert(
            columnSchemas.begin() + 5,
            TColumnSchema("extraKey", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending));
        auto readSchema = TTableSchema(columnSchemas);

        TUnversionedOwningRowBuilder lowerKeyBuilder;
        for (auto it = InitialRows_[1].BeginKeys(); it != InitialRows_[1].EndKeys(); ++it) {
            lowerKeyBuilder.AddValue(*it);
        }
        lowerKeyBuilder.AddValue(MakeUnversionedBooleanValue(false));
        auto lowerKey = lowerKeyBuilder.FinishRow();

        TUnversionedOwningRowBuilder upperKeyBuilder;
        for (auto it = InitialRows_[1].BeginKeys(); it != InitialRows_[1].EndKeys(); ++it) {
            upperKeyBuilder.AddValue(*it);
        }
        upperKeyBuilder.AddValue(MakeUnversionedBooleanValue(true));
        auto upperKey = upperKeyBuilder.FinishRow();

        TestRangeReader(optimizeFor, writeSchema, readSchema, lowerKey, upperKey, 25, false);
    }
};

TEST_F(TVersionedChunksHeavyTest, FullScanCompactionScan)
{
    DoFullScanCompaction(EOptimizeFor::Scan);
}

TEST_F(TVersionedChunksHeavyTest, FullScanCompactionLookup)
{
    DoFullScanCompaction(EOptimizeFor::Lookup);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScan)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, 50);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScanSyncLastCommitted)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, SyncLastCommittedTimestamp);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnLookup)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Lookup, 50);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnLookupSyncLastCommitted)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Lookup, SyncLastCommittedTimestamp);
}

TEST_F(TVersionedChunksHeavyTest, GroupsLimitsAndSchemaChange)
{
    auto writeColumnSchemas = ColumnSchemas_;
    writeColumnSchemas[5].SetGroup(TString("G1"));
    writeColumnSchemas[9].SetGroup(TString("G1"));

    writeColumnSchemas[6].SetGroup(TString("G2"));
    writeColumnSchemas[7].SetGroup(TString("G2"));
    writeColumnSchemas[8].SetGroup(TString("G2"));
    auto writeSchema = TTableSchema(writeColumnSchemas);

    auto readColumnSchemas = ColumnSchemas_;
    readColumnSchemas.erase(readColumnSchemas.begin() + 7, readColumnSchemas.end());
    readColumnSchemas.insert(readColumnSchemas.end(), TColumnSchema("extraValue", EValueType::Boolean));
    auto readSchema = TTableSchema(readColumnSchemas);

    int lowerIndex = InitialRows_.size() / 3;

    auto lowerKey = TOwningKey(InitialRows_[lowerIndex].BeginKeys(), InitialRows_[lowerIndex].EndKeys());
    auto upperRowIndex = InitialRows_.size() - 1;
    auto upperKey = TOwningKey(
        InitialRows_[upperRowIndex].BeginKeys(),
        InitialRows_[upperRowIndex].EndKeys());

    TestRangeReader(EOptimizeFor::Scan, writeSchema, readSchema, lowerKey, upperKey, SyncLastCommittedTimestamp, false);
}

TEST_F(TVersionedChunksHeavyTest, EmptyReadWideSchemaScan)
{
    DoEmptyReadWideSchema(EOptimizeFor::Scan);
}

TEST_F(TVersionedChunksHeavyTest, EmptyReadWideSchemaLookup)
{
    DoEmptyReadWideSchema(EOptimizeFor::Lookup);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
