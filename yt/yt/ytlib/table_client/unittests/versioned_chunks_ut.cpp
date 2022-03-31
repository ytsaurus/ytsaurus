#include "helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/ytlib/new_table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/versioned_writer.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/random.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

#include <util/random/shuffle.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;

using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

TString A("a");
TString B("b");

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue DoMakeMinSentinel()
{
    return MakeUnversionedSentinelValue(EValueType::Min);
}

TUnversionedValue DoMakeNullSentinel()
{
    return MakeUnversionedSentinelValue(EValueType::Null);
}

TUnversionedValue DoMakeMaxSentinel()
{
    return MakeUnversionedSentinelValue(EValueType::Max);
}

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): rewrite this legacy test.
class TVersionedChunksLookupTestBase
    : public ::testing::Test
{
protected:
    TTableSchemaPtr Schema;

    IVersionedReaderPtr ChunkReader;
    IVersionedWriterPtr ChunkWriter;

    IChunkReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    TChunkedMemoryPool MemoryPool;

    EOptimizeFor OptimizeFor;

    explicit TVersionedChunksLookupTestBase(EOptimizeFor optimizeFor)
        : Schema(New<TTableSchema>(std::vector{
            TColumnSchema("k1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Double)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64)
        }))
        , OptimizeFor(optimizeFor)
    { }

    void SetUp() override
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
            MemoryWriter,
            /*dataSink*/ std::nullopt);
    }

    void GetRowAndResetWriter()
    {
        EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

        // Initialize reader.
        MemoryReader = CreateMemoryReader(
            MemoryWriter->GetChunkMeta(),
            MemoryWriter->GetBlocks());
    }

    void FillKey(TMutableVersionedRow row, std::optional<TString> k1, std::optional<i64> k2, std::optional<double> k3)
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
        FillKey(row, std::make_optional(A), std::make_optional(index), std::nullopt);

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

    TKeyComparer KeyComparer_;

    void DoTest(bool testNewReader = false)
    {
        std::vector<TVersionedRow> expected;
        int startIndex = 0;
        for (int i = 0; i < 3; ++i) {
            std::vector<TVersionedRow> rows;
            startIndex = CreateManyRows(&rows, startIndex);
            expected.insert(expected.end(), rows.begin(), rows.end());
        }

        WriteManyRows();

        auto chunkMeta = MemoryReader->GetMeta(/*chunkReadOptions*/ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr))
            .Get()
            .ValueOrThrow();

        {
            TChunkedMemoryPool pool;
            TUnversionedOwningRowBuilder builder;

            std::vector<TLegacyOwningKey> owningKeys;

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
            FillKey(row, std::make_optional(A), std::make_optional(0), std::nullopt);
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
            FillKey(row, std::make_optional(A), std::make_optional(150000), std::nullopt);
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

            std::vector<TLegacyKey> keys;
            for (const auto& owningKey : owningKeys) {
                keys.push_back(owningKey);
            }

            auto sharedKeys = MakeSharedRange(std::move(keys), std::move(owningKeys));

            auto chunkState = New<TChunkState>(
                GetNullBlockCache(),
                TChunkSpec(),
                nullptr,
                NullTimestamp,
                nullptr,
                New<TChunkReaderPerformanceCounters>(),
                KeyComparer_,
                nullptr,
                Schema);

            IVersionedReaderPtr chunkReader;
            if (testNewReader) {
                chunkReader = NNewTableClient::CreateVersionedChunkReader(
                    sharedKeys,
                    MaxTimestamp,
                    chunkMeta,
                    Schema,
                    TColumnFilter(),
                    nullptr,
                    chunkState->BlockCache,
                    TChunkReaderConfig::GetDefault(),
                    MemoryReader,
                    chunkState->PerformanceCounters,
                    /* chunkReadOptions */ {},
                    false);
            } else {
                chunkReader = CreateVersionedChunkReader(
                    TChunkReaderConfig::GetDefault(),
                    MemoryReader,
                    std::move(chunkState),
                    std::move(chunkMeta),
                    /* chunkReadOptions */ {},
                    sharedKeys,
                    TColumnFilter(),
                    MaxTimestamp,
                    false);
            }

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

TEST_F(TColumnarVersionedChunksLookupTest, TestNew)
{
    DoTest(true);
}

////////////////////////////////////////////////////////////////////////////////

template <class TIter>
void FillRandomUniqueSequence(TFastRng64& rng, TIter begin, TIter end, size_t min, size_t max)
{
    YT_VERIFY(end - begin <= static_cast<ssize_t>(max - min));

    if (begin == end) {
        return;
    }

    YT_VERIFY(min < max);

    TIter current = begin;

    do {
        while (current < end) {
            *current++ = rng.Uniform(min, max);
        }

        std::sort(begin, end);
        current = std::unique(begin, end);
    } while (current < end);
}

TSharedRange<size_t> GetRandomUniqueSequence(TFastRng64& rng, size_t length, size_t min, size_t max)
{
    std::vector<ui64> sequence(length);
    FillRandomUniqueSequence(rng, sequence.begin(), sequence.end(), min, max);
    return MakeSharedRange(std::move(sequence));
}

struct TRandomValueGenerator
{
    TFastRng64& Rng;
    TRowBufferPtr RowBuffer_;
    i64 DomainSize;

    static constexpr int NullRatio = 30;

    TUnversionedValue GetRandomValue(EValueType type)
    {
        YT_VERIFY(DomainSize > 0);

        switch (type) {
            case EValueType::Int64:
                return MakeUnversionedInt64Value(Rng.Uniform(-DomainSize, DomainSize));
            case EValueType::Uint64:
                return MakeUnversionedUint64Value(Rng.Uniform(DomainSize, 2 * DomainSize));
            case EValueType::String: {
                constexpr char Letters[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

                auto length = Rng.Uniform(std::min<i64>(DomainSize, 200));

                auto data = RowBuffer_->GetPool()->AllocateUnaligned(length);
                for (size_t index = 0; index < length; ++index) {
                    // Last character in Letters is zero.
                    data[index] = Letters[Rng.Uniform(sizeof(Letters) - 1)];
                }

                return MakeUnversionedStringValue(TStringBuf(data, length));
            }
            case EValueType::Double:
                return MakeUnversionedDoubleValue(Rng.GenRandReal1() * DomainSize);
            case EValueType::Boolean:
                return MakeUnversionedBooleanValue(static_cast<bool>(Rng.Uniform(2)));
            default:
                Y_UNREACHABLE();
        }
    }

    TUnversionedValue GetRandomNullableValue(EValueType type)
    {
        return Rng.Uniform(NullRatio) > 0
            ? GetRandomValue(type)
            : MakeUnversionedSentinelValue(EValueType::Null);
    }
};

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

    TFastRng64 Rng_{42};

    void TestRangeReader(
        TRange<TVersionedRow> initialRows,
        EOptimizeFor optimizeFor,
        TTableSchemaPtr writeSchema,
        TTableSchemaPtr readSchema,
        TSharedRange<TRowRange> ranges,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool testNewReader)
    {
        auto expected = CreateExpected(initialRows, writeSchema, readSchema, ranges, timestamp);

        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 1025;
        config->MaxSegmentValueCount = 128;
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        auto chunkWriter = CreateVersionedChunkWriter(
            config,
            options,
            writeSchema,
            memoryWriter,
            /*dataSink*/ std::nullopt);

        chunkWriter->Write(initialRows);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        auto memoryReader = CreateMemoryReader(
            memoryWriter->GetChunkMeta(),
            memoryWriter->GetBlocks());

        auto chunkMeta = memoryReader->GetMeta(/* chunkReadOptions */ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr))
            .Get()
            .ValueOrThrow();

        auto chunkState = New<TChunkState>(GetNullBlockCache());
        chunkState->TableSchema = readSchema;

        IVersionedReaderPtr chunkReader;
        if (testNewReader) {
            chunkReader = NNewTableClient::CreateVersionedChunkReader(
                ranges,
                timestamp,
                chunkMeta,
                readSchema,
                TColumnFilter(),
                nullptr,
                chunkState->BlockCache,
                TChunkReaderConfig::GetDefault(),
                memoryReader,
                chunkState->PerformanceCounters,
                /* chunkReadOptions */ {},
                produceAllVersions);
        } else {
            chunkReader = CreateVersionedChunkReader(
                TChunkReaderConfig::GetDefault(),
                memoryReader,
                std::move(chunkState),
                std::move(chunkMeta),
                /* chunkReadOptions */ {},
                ranges,
                TColumnFilter(),
                timestamp,
                produceAllVersions);
        }

        CheckResult(&expected, chunkReader);
    }

    void TestRangeReader(
        TRange<TVersionedRow> initialRows,
        EOptimizeFor optimizeFor,
        TTableSchemaPtr writeSchema,
        TTableSchemaPtr readSchema,
        TLegacyOwningKey lowerKey,
        TLegacyOwningKey upperKey,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool testNewReader)
    {
        TestRangeReader(
            initialRows,
            optimizeFor,
            writeSchema,
            readSchema,
            MakeSingletonRowRange(lowerKey, upperKey),
            timestamp,
            produceAllVersions,
            testNewReader);
    }

    void TestLookupReader(
        TRange<TVersionedRow> initialRows,
        EOptimizeFor optimizeFor,
        TTableSchemaPtr writeSchema,
        TTableSchemaPtr readSchema,
        TSharedRange<TUnversionedRow> lookupKeys,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool testNewReader)
    {
        auto expected = CreateExpected(initialRows, writeSchema, readSchema, lookupKeys, timestamp);

        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 1025;
        config->MaxSegmentValueCount = 128;
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        auto chunkWriter = CreateVersionedChunkWriter(
            config,
            options,
            writeSchema,
            memoryWriter,
            /*dataSink*/ std::nullopt);

        chunkWriter->Write(initialRows);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        auto memoryReader = CreateMemoryReader(
            memoryWriter->GetChunkMeta(),
            memoryWriter->GetBlocks());

        auto chunkMeta = memoryReader->GetMeta(/*chunkReadOptions*/ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr))
            .Get()
            .ValueOrThrow();

        auto chunkState = New<TChunkState>(GetNullBlockCache());
        chunkState->TableSchema = readSchema;

        IVersionedReaderPtr chunkReader;
        if (testNewReader) {
            chunkReader = NNewTableClient::CreateVersionedChunkReader(
                lookupKeys,
                timestamp,
                chunkMeta,
                readSchema,
                TColumnFilter(),
                nullptr,
                chunkState->BlockCache,
                TChunkReaderConfig::GetDefault(),
                memoryReader,
                chunkState->PerformanceCounters,
                /* chunkReadOptions */ {},
                produceAllVersions);
        } else {
            chunkReader = CreateVersionedChunkReader(
                TChunkReaderConfig::GetDefault(),
                memoryReader,
                std::move(chunkState),
                std::move(chunkMeta),
                /* chunkReadOptions */ {},
                lookupKeys,
                TColumnFilter(),
                timestamp,
                produceAllVersions);
        }

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
        YT_ABORT();
    }

    std::vector<TVersionedRow> CreateRows(int count = 10000)
    {
        std::vector<char> stringValue(100, 'a');
        srand(0);

        std::vector<TTimestamp> timestamps = {10, 20, 30, 40, 50, 60, 70, 80, 90};

        std::vector<TVersionedRow> rows;
        TVersionedRowBuilder builder(RowBuffer_);

        for (int rowIndex = 0; rowIndex < count; ++rowIndex) {
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
                builder.AddValue(MakeVersionedInt64Value(-10 + (rowIndex / 16) + i, writeTimestamps[i], 5, i % 2 == 0 ? EValueFlags::None : EValueFlags::Aggregate));
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

    static std::vector<TUnversionedValue> GenerateRandomValues(
        EValueType type,
        TRandomValueGenerator* valueGenerator,
        int distinctValueCount)
    {
        std::vector<TUnversionedValue> values;

        values.push_back(MakeUnversionedSentinelValue(EValueType::Null));

        for (int index = 0; index < distinctValueCount; ++index) {
            values.push_back(valueGenerator->GetRandomValue(type));
        }

        std::sort(values.begin(), values.end());
        values.erase(
            std::unique(values.begin(), values.end()),
            values.end());

        return values;
    }

    static TUnversionedValue MakeUnversionedValue(
        TUnversionedValue value,
        ui16 id)
    {
        value.Id = id;
        return value;
    }

    static TVersionedValue MakeVersionedValue(
        TUnversionedValue value,
        TTimestamp timestamp,
        ui16 id,
        EValueFlags flags = EValueFlags::None)
    {
        TVersionedValue result;
        static_cast<TUnversionedValue&>(result) = value;
        result.Timestamp = timestamp;
        result.Id = id;
        result.Flags = flags;
        return result;
    }

    struct TRadomKeyGenerator
    {
        const std::vector<EValueType> KeyTypes;

        std::vector<TUnversionedValue> IntValues;
        std::vector<TUnversionedValue> UintValues;
        std::vector<TUnversionedValue> StringValues;
        std::vector<TUnversionedValue> DoubleValues;
        std::vector<TUnversionedValue> BooleanValues;

        TRadomKeyGenerator(
            TRandomValueGenerator* valueGenerator,
            int distinctValueCount,
            std::vector<EValueType> keyTypes)
            : KeyTypes(keyTypes)
        {
            GetValues(EValueType::Int64) = GenerateRandomValues(EValueType::Int64, valueGenerator, distinctValueCount);
            GetValues(EValueType::Uint64) = GenerateRandomValues(EValueType::Uint64, valueGenerator, distinctValueCount);
            GetValues(EValueType::String) = GenerateRandomValues(EValueType::String, valueGenerator, distinctValueCount);
            GetValues(EValueType::Double) = GenerateRandomValues(EValueType::Double, valueGenerator, distinctValueCount);
            GetValues(EValueType::Boolean) = GenerateRandomValues(EValueType::Boolean, valueGenerator, distinctValueCount);
        }

        size_t GetDistinctKeyCount()
        {
            auto result = 1;
            for (auto type : KeyTypes) {
                result *= GetValues(type).size();
            }

            return result;
        }

        std::vector<TUnversionedValue>& GetValues(EValueType type)
        {
            switch (type) {
                case EValueType::Int64:
                    return IntValues;
                case EValueType::Uint64:
                    return UintValues;
                case EValueType::String:
                    return StringValues;
                case EValueType::Double:
                    return DoubleValues;
                case EValueType::Boolean:
                    return BooleanValues;
                default:
                    Y_UNREACHABLE();
            }
        }

        std::vector<TUnversionedValue> ValueStack;

        void Generate(TVersionedRowBuilder* builder, size_t index)
        {
            ValueStack.clear();
            for (int id = KeyTypes.size(); id > 0; --id) {
                const auto& values = GetValues(KeyTypes[id - 1]);

                auto value = values[index % values.size()];
                index /= values.size();
                value.Id = id - 1;

                ValueStack.push_back(value);
            }

            while (!ValueStack.empty()) {
                builder->AddKey(ValueStack.back());
                ValueStack.pop_back();
            }
        }

        void Generate(TUnversionedRowBuilder* builder, size_t index)
        {
            ValueStack.clear();
            for (int id = KeyTypes.size(); id > 0; --id) {
                const auto& values = GetValues(KeyTypes[id - 1]);

                auto value = values[index % values.size()];
                index /= values.size();
                value.Id = id - 1;

                ValueStack.push_back(value);
            }

            while (!ValueStack.empty()) {
                builder->AddValue(ValueStack.back());
                ValueStack.pop_back();
            }
        }

        void GenerateBound(TUnversionedRowBuilder* builder, TRandomValueGenerator* valueGenerator, TFastRng64* rng)
        {
            auto prefixSize = rng->Uniform(12);

            if (prefixSize > 8) {
                prefixSize = 5;
            } else if (prefixSize > 4) {
                prefixSize = 4;
            } else if (prefixSize > 2) {
                prefixSize = 3;
            } else if (prefixSize > 1) {
                prefixSize = 2;
            } else if (prefixSize > 0) {
                prefixSize = 1;
            } else {
                prefixSize = 0;
            }

            for (size_t id = 0; id < std::min(prefixSize, KeyTypes.size()); ++id) {
                builder->AddValue(MakeUnversionedValue(valueGenerator->GetRandomNullableValue(KeyTypes[id]), id));
            }

            auto sentinel = rng->Uniform(7);
            if (sentinel == 0) {
                builder->AddValue(MakeUnversionedSentinelValue(EValueType::Min));
            }

            if (sentinel == 1) {
                builder->AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            }
        }
    };

    // Greater value domain size for direct segment encoding.
    std::vector<TVersionedRow> GenerateRandomRows(
        TRandomValueGenerator* valueGenerator,
        TRadomKeyGenerator* keyGenerator)
    {
        auto distinctKeyCount = keyGenerator->GetDistinctKeyCount();

        auto randomUniqueSequence = GetRandomUniqueSequence(
            Rng_,
            Rng_.Uniform(distinctKeyCount / 7, distinctKeyCount / 3),
            0,
            distinctKeyCount);

        TVersionedRowBuilder builder(RowBuffer_);

        std::vector<TVersionedRow> rows;
        std::vector<TTimestamp> timestamps = {10, 20, 30, 40, 50, 60, 70, 80, 90};

        constexpr int SegmentModeSwitchLimit = 500;
        int rowIndex = 0;

        bool sparseColumns[6];
        auto getValueRadomCount = [&] (int index) {
            if (sparseColumns[index]) {
                auto count = Rng_.Uniform(12);
                if (count > 3) {
                    count = 0;
                }

                return count;
            } else {
                return Rng_.Uniform(4);
            }
        };

        for (auto index : randomUniqueSequence) {
            // Generate random rows selecting value by index from generated.
            keyGenerator->Generate(&builder, index);

            Shuffle(timestamps.begin(), timestamps.end());
            int deleteTimestampCount = Rng_.Uniform(4);
            std::vector<TTimestamp> deleteTimestamps(timestamps.begin(), timestamps.begin() + deleteTimestampCount);
            for (auto timestamp : deleteTimestamps) {
                builder.AddDeleteTimestamp(timestamp);
            }

            std::vector<TTimestamp> writeTimestamps(
                timestamps.begin() + deleteTimestampCount,
                timestamps.begin() + deleteTimestampCount + 4);

            if (rowIndex % SegmentModeSwitchLimit == 0) {
                for (int i = 0; i < 6; ++i) {
                    sparseColumns[i] = Rng_.Uniform(2);
                }
            }
            ++rowIndex;

            // v1
            auto count = getValueRadomCount(0);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::Int64),
                    writeTimestamps[i],
                    5,
                    Rng_.Uniform(2) == 0 ? EValueFlags::None : EValueFlags::Aggregate));
            }

            // v2
            count = getValueRadomCount(1);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::Int64),
                    writeTimestamps[i],
                    6));
            }

            // v3
            count = getValueRadomCount(2);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::Uint64),
                    writeTimestamps[i],
                    7));
            }

            // v4
            count = getValueRadomCount(3);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::String),
                    writeTimestamps[i],
                    8));
            }

            // v5
            count = getValueRadomCount(4);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::Double),
                    writeTimestamps[i],
                    9));
            }

            // v6
            count = getValueRadomCount(5);
            for (size_t i = 0; i < count; ++i) {
                builder.AddValue(MakeVersionedValue(
                    valueGenerator->GetRandomValue(EValueType::Boolean),
                    writeTimestamps[i],
                    10));
            }

            rows.push_back(builder.FinishRow());
        }

        return rows;
    }

    std::vector<int> BuildIdMapping(TTableSchemaPtr writeSchema, TTableSchemaPtr readSchema)
    {
        std::vector<int> idMapping(writeSchema->GetColumnCount(), -1);
        for (int i = 0; i < writeSchema->GetColumnCount(); ++i) {
            auto writeColumnSchema = writeSchema->Columns()[i];
            for (int j = 0; j < readSchema->GetColumnCount(); ++j) {
                auto readColumnSchema = readSchema->Columns()[j];
                if (writeColumnSchema.Name() == readColumnSchema.Name()) {
                    idMapping[i] = j;
                }
            }
        }
        return idMapping;
    }

    bool CreateExpectedRow(
        TVersionedRowBuilder* builder,
        TVersionedRow row,
        TTimestamp timestamp,
        TRange<int> idMapping,
        TRange<TColumnSchema> readSchemaColumns)
    {
        if (timestamp == AllCommittedTimestamp) {
            for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                builder->AddDeleteTimestamp(*deleteIt);
            }

            for (auto valueIt = row.BeginValues(); valueIt != row.EndValues(); ++valueIt) {
                if (idMapping[valueIt->Id] > 0) {
                    auto value = *valueIt;
                    value.Id = idMapping[valueIt->Id];
                    builder->AddValue(value);
                }
            }
        } else {
            // Find delete timestamp.
            TTimestamp deleteTimestamp = NullTimestamp;
            for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                if (*deleteIt <= timestamp) {
                    deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
                }
            }
            if (deleteTimestamp != NullTimestamp) {
                builder->AddDeleteTimestamp(deleteTimestamp);
            }

            TTimestamp writeTimestamp = NullTimestamp;
            for (auto writeIt = row.BeginWriteTimestamps(); writeIt != row.EndWriteTimestamps(); ++writeIt) {
                if (*writeIt <= timestamp && *writeIt > deleteTimestamp) {
                    writeTimestamp = std::max(*writeIt, writeTimestamp);
                    builder->AddWriteTimestamp(*writeIt);
                }
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
                return false;
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
                if (usedIds.insert(value.Id).second || readSchemaColumns[value.Id].Aggregate()) {
                    builder->AddValue(value);
                }
            }
        }

        return true;
    }

    std::vector<TVersionedRow> CreateExpected(
        TRange<TVersionedRow> rows,
        TTableSchemaPtr writeSchema,
        TTableSchemaPtr readSchema,
        TSharedRange<TRowRange> ranges,
        TTimestamp timestamp)
    {
        if (ranges.empty()) {
            return {};
        }

        std::vector<TVersionedRow> expected;

        auto idMapping = BuildIdMapping(writeSchema, readSchema);

        const auto* currentRange = ranges.begin();

        for (auto row : rows) {
            std::vector<TUnversionedValue> key;
            YT_VERIFY(row.GetKeyCount() <= readSchema->GetKeyColumnCount());
            for (int i = 0; i < row.GetKeyCount() && i < readSchema->GetKeyColumnCount(); ++i) {
                auto columnType = readSchema->Columns()[i].GetWireType();
                auto actualType = row.BeginKeys()[i].Type;
                YT_VERIFY(actualType == columnType || actualType == EValueType::Null);
                key.push_back(row.BeginKeys()[i]);
            }

            for (int i = row.GetKeyCount(); i < readSchema->GetKeyColumnCount(); ++i) {
                key.push_back(MakeUnversionedSentinelValue(EValueType::Null, i));
            }

            while (CompareRows(
                key.data(),
                key.data() + key.size(),
                currentRange->second.Begin(),
                currentRange->second.End()) >= 0)
            {
                ++currentRange;
                if (currentRange == ranges.end()) {
                    return expected;
                }
            }

            if (CompareRows(
                key.data(),
                key.data() + key.size(),
                currentRange->first.Begin(), currentRange->first.End()) < 0)
            {
                continue;
            }

            TVersionedRowBuilder builder(RowBuffer_, timestamp == AllCommittedTimestamp);
            for (const auto& value : key) {
                builder.AddKey(value);
            }

            if (CreateExpectedRow(&builder, row, timestamp, idMapping, readSchema->Columns())) {
                expected.push_back(builder.FinishRow());
            } else {
                expected.push_back(TVersionedRow());
            }
        }
        return expected;
    }

    std::vector<TVersionedRow> CreateExpected(
        TRange<TVersionedRow> rows,
        TTableSchemaPtr writeSchema,
        TTableSchemaPtr readSchema,
        TSharedRange<TUnversionedRow> keys,
        TTimestamp timestamp)
    {
        std::vector<TVersionedRow> expected;

        auto idMapping = BuildIdMapping(writeSchema, readSchema);

        auto rowsIt = rows.begin();

        for (auto key : keys) {
            rowsIt = ExponentialSearch(rowsIt, rows.end(), [&] (auto rowsIt) {
                return CompareRows(rowsIt->BeginKeys(), rowsIt->EndKeys(), key.Begin(), key.End()) < 0;
            });

            if (rowsIt != rows.end() &&
                CompareRows(rowsIt->BeginKeys(), rowsIt->EndKeys(), key.Begin(), key.End()) == 0)
            {
                TVersionedRowBuilder builder(RowBuffer_, timestamp == AllCommittedTimestamp);
                for (const auto& value : key) {
                    builder.AddKey(value);
                }

                if (CreateExpectedRow(&builder, *rowsIt, timestamp, idMapping, readSchema->Columns())) {
                    expected.push_back(builder.FinishRow());
                } else {
                    expected.push_back(TVersionedRow());
                }
            } else {
                expected.push_back(TVersionedRow());
            }
        }

        return expected;
    }

    void DoFullScanCompaction(EOptimizeFor optimizeFor, bool testNewReader = false)
    {
        auto writeSchema = New<TTableSchema>(ColumnSchemas_);
        auto readSchema = New<TTableSchema>(ColumnSchemas_);

        TestRangeReader(
            InitialRows_,
            optimizeFor,
            writeSchema,
            readSchema,
            MinKey(),
            MaxKey(),
            AllCommittedTimestamp,
            true,
            testNewReader);
    }

    void DoTimestampFullScanExtraKeyColumn(EOptimizeFor optimizeFor, TTimestamp timestamp, bool testNewReader = false)
    {
        auto writeSchema = New<TTableSchema>(ColumnSchemas_);

        auto columnSchemas = ColumnSchemas_;
        columnSchemas.insert(
            columnSchemas.begin() + 5,
            TColumnSchema("extraKey", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending));

        auto readSchema = New<TTableSchema>(columnSchemas);

        TestRangeReader(
            InitialRows_,
            optimizeFor,
            writeSchema,
            readSchema,
            MinKey(),
            MaxKey(),
            timestamp,
            false,
            testNewReader);
    }

    void DoEmptyReadWideSchema(EOptimizeFor optimizeFor, bool testNewReader = false)
    {
        auto writeSchema = New<TTableSchema>(ColumnSchemas_);

        auto columnSchemas = ColumnSchemas_;
        columnSchemas.insert(
            columnSchemas.begin() + 5,
            TColumnSchema("extraKey", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending));
        auto readSchema = New<TTableSchema>(columnSchemas);

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

        TestRangeReader(
            InitialRows_,
            optimizeFor,
            writeSchema,
            readSchema,
            lowerKey,
            upperKey,
            25,
            false,
            testNewReader);
    }

    void DoGroupsLimitsAndSchemaChange(bool testNewReader = false)
    {
        auto writeColumnSchemas = ColumnSchemas_;
        writeColumnSchemas[5].SetGroup(TString("G1"));
        writeColumnSchemas[9].SetGroup(TString("G1"));

        writeColumnSchemas[6].SetGroup(TString("G2"));
        writeColumnSchemas[7].SetGroup(TString("G2"));
        writeColumnSchemas[8].SetGroup(TString("G2"));
        auto writeSchema = New<TTableSchema>(writeColumnSchemas);

        auto readColumnSchemas = ColumnSchemas_;
        readColumnSchemas.erase(readColumnSchemas.begin() + 7, readColumnSchemas.end());
        readColumnSchemas.insert(readColumnSchemas.end(), TColumnSchema("extraValue", EValueType::Boolean));
        auto readSchema = New<TTableSchema>(readColumnSchemas);

        int lowerIndex = InitialRows_.size() / 3;

        auto lowerKey = TLegacyOwningKey(InitialRows_[lowerIndex].BeginKeys(), InitialRows_[lowerIndex].EndKeys());
        auto upperRowIndex = InitialRows_.size() - 1;
        auto upperKey = TLegacyOwningKey(
            InitialRows_[upperRowIndex].BeginKeys(),
            InitialRows_[upperRowIndex].EndKeys());

        TestRangeReader(
            InitialRows_,
            EOptimizeFor::Scan,
            writeSchema,
            readSchema,
            lowerKey,
            upperKey,
            SyncLastCommittedTimestamp,
            false,
            testNewReader);
    }

    void DoStressTest(EOptimizeFor optimizeFor, bool testNewReader = false)
    {
        // Good combination to test all types of segments.
        std::vector<EValueType> keyTypes = {
            EValueType::Int64,
            EValueType::Double,
            EValueType::Uint64,
            EValueType::String,
            EValueType::Boolean
        };

        std::vector<TColumnSchema> columnSchemas;
        for (int index = 0; index < std::ssize(keyTypes); ++index) {
            columnSchemas.push_back(TColumnSchema(Format("k%v", index), keyTypes[index])
                .SetSortOrder(ESortOrder::Ascending));
        }

        columnSchemas.push_back(TColumnSchema("v1", EValueType::Int64).SetAggregate(TString("min")));
        columnSchemas.push_back(TColumnSchema("v2", EValueType::Int64));
        columnSchemas.push_back(TColumnSchema("v3", EValueType::Uint64));
        columnSchemas.push_back(TColumnSchema("v4", EValueType::String));
        columnSchemas.push_back(TColumnSchema("v5", EValueType::Double));
        columnSchemas.push_back(TColumnSchema("v6", EValueType::Boolean));

        int distinctValueCount = 10;
        for (i64 valueDomainSize : {10, 20, 100, 100000000}) {
            // Generate random values for each type.
            TRandomValueGenerator valueGenerator{Rng_, RowBuffer_, valueDomainSize};
            TRadomKeyGenerator keyGenerator(&valueGenerator, distinctValueCount, keyTypes);
            auto rows = GenerateRandomRows(&valueGenerator, &keyGenerator);

            for (TTimestamp timestamp : {TTimestamp(50), AllCommittedTimestamp}) {
                // Test lookup keys.
                for (size_t keyCount : {1, 2, 3, 4, 5, 6, 7, 10, 20, 30, 40, 50, 60, 100, 200, 500, 1000, 5000}) {
                    TUnversionedRowBuilder builder;

                    auto randomUniqueSequence = GetRandomUniqueSequence(
                        Rng_,
                        std::min(keyCount, keyGenerator.GetDistinctKeyCount() * 2 / 3),
                        0,
                        keyGenerator.GetDistinctKeyCount());

                    std::vector<TUnversionedRow> lookupKeys;
                    for (auto index : randomUniqueSequence) {
                        builder.Reset();
                        keyGenerator.Generate(&builder, index);
                        lookupKeys.push_back(RowBuffer_->CaptureRow(builder.GetRow(), false));
                    }

                    auto writeSchema = New<TTableSchema>(columnSchemas);
                    auto readSchema = New<TTableSchema>(columnSchemas);

                    TestLookupReader(
                        rows,
                        optimizeFor,
                        writeSchema,
                        readSchema,
                        MakeSharedRange(lookupKeys, RowBuffer_),
                        timestamp,
                        timestamp == AllCommittedTimestamp,
                        testNewReader);
                }

                // Test ranges with common prefix.
                for (size_t keyCount : {2, 3, 4, 5, 6, 7, 10, 20, 30, 40, 50, 60, 100, 200, 500, 1000, 5000}) {
                    TUnversionedRowBuilder builder;

                    auto randomUniqueSequence = GetRandomUniqueSequence(
                        Rng_,
                        std::min(keyCount, keyGenerator.GetDistinctKeyCount() * 2 / 3),
                        0,
                        keyGenerator.GetDistinctKeyCount());

                    std::vector<TUnversionedRow> lookupKeys;
                    for (auto index : randomUniqueSequence) {
                        builder.Reset();
                        keyGenerator.Generate(&builder, index);

                        auto sentinel = Rng_.Uniform(7);
                        if (sentinel == 0) {
                            builder.AddValue(MakeUnversionedSentinelValue(EValueType::Min));
                        }

                        if (sentinel == 1) {
                            builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                        }

                        lookupKeys.push_back(RowBuffer_->CaptureRow(builder.GetRow(), false));
                    }

                    std::vector<TRowRange> readRanges;
                    for (int rangeIndex = 0; rangeIndex < std::ssize(lookupKeys) / 2; ++rangeIndex) {
                        readRanges.push_back(TRowRange(lookupKeys[2 * rangeIndex], lookupKeys[2 * rangeIndex + 1]));
                    }

                    if (readRanges.empty()) {
                        continue;
                    }

                    auto writeSchema = New<TTableSchema>(columnSchemas);
                    auto readSchema = New<TTableSchema>(columnSchemas);

                    TestRangeReader(
                        rows,
                        optimizeFor,
                        writeSchema,
                        readSchema,
                        MakeSharedRange(readRanges, RowBuffer_),
                        timestamp,
                        timestamp == AllCommittedTimestamp,
                        testNewReader);
                }

                // Test arbitrary ranges.
                for (i64 rangeCount : {1, 2, 3, 4, 5, 9, 13, 21, 25, 29, 30, 36, 37, 70, 83, 100, 297}) {
                    for (int iteration = 0; iteration < 3; ++iteration) {
                        TUnversionedRowBuilder builder;

                        std::vector<TUnversionedRow> bounds;
                        for (int boundIndex = 0; boundIndex < rangeCount * 2; ++boundIndex) {
                            builder.Reset();
                            keyGenerator.GenerateBound(&builder, &valueGenerator, &Rng_);
                            bounds.push_back(RowBuffer_->CaptureRow(builder.GetRow(), false));
                        }

                        std::sort(bounds.begin(), bounds.end());

                        YT_VERIFY(bounds.size() % 2 == 0);

                        std::vector<TRowRange> readRanges;
                        for (int rangeIndex = 0; rangeIndex < std::ssize(bounds) / 2; ++rangeIndex) {
                            readRanges.push_back(TRowRange(bounds[2 * rangeIndex], bounds[2 * rangeIndex + 1]));
                        }

                        auto writeSchema = New<TTableSchema>(columnSchemas);
                        auto readSchema = New<TTableSchema>(columnSchemas);

                        TestRangeReader(
                            rows,
                            optimizeFor,
                            writeSchema,
                            readSchema,
                            MakeSharedRange(readRanges, RowBuffer_),
                            timestamp,
                            timestamp == AllCommittedTimestamp,
                            testNewReader);
                    }
                }
            }

            RowBuffer_->Clear();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunksHeavyTestWithParametrizedBounds
    : public TVersionedChunksHeavyTest
    , public ::testing::WithParamInterface<std::pair<TUnversionedValue, TUnversionedValue>>
{
protected:
    void DoReadWideSchemaWithSpecifiedBounds(
        EOptimizeFor optimizeFor,
        const std::pair<TUnversionedValue, TUnversionedValue>& bounds,
        bool testNewReader = false)
    {
        auto writeSchema = New<TTableSchema>(ColumnSchemas_);

        auto columnSchemas = ColumnSchemas_;
        columnSchemas.insert(
            columnSchemas.begin() + 5,
            TColumnSchema("extraKey", EValueType::Int64).SetSortOrder(ESortOrder::Ascending));
        auto readSchema = New<TTableSchema>(columnSchemas);

        TUnversionedOwningRowBuilder lowerKeyBuilder;
        for (auto it = InitialRows_[1].BeginKeys(); it != InitialRows_[1].EndKeys(); ++it) {
            lowerKeyBuilder.AddValue(*it);
        }
        lowerKeyBuilder.AddValue(bounds.first);
        auto lowerKey = lowerKeyBuilder.FinishRow();

        TUnversionedOwningRowBuilder upperKeyBuilder;
        for (auto it = InitialRows_[1].BeginKeys(); it != InitialRows_[1].EndKeys(); ++it) {
            upperKeyBuilder.AddValue(*it);
        }
        upperKeyBuilder.AddValue(bounds.second);
        auto upperKey = upperKeyBuilder.FinishRow();

        TestRangeReader(
            InitialRows_,
            optimizeFor,
            writeSchema,
            readSchema,
            lowerKey,
            upperKey,
            25,
            false,
            testNewReader);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedChunksHeavyTest, FullScanCompactionScan)
{
    DoFullScanCompaction(EOptimizeFor::Scan);
}

TEST_F(TVersionedChunksHeavyTest, FullScanCompactionScanNew)
{
    DoFullScanCompaction(EOptimizeFor::Scan, true);
}

TEST_F(TVersionedChunksHeavyTest, FullScanCompactionLookup)
{
    DoFullScanCompaction(EOptimizeFor::Lookup);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScan)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, 50);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScanNew)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, 50, true);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnLookup)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Lookup, 50);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScanSyncLastCommitted)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, SyncLastCommittedTimestamp);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnScanSyncLastCommittedNew)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Scan, SyncLastCommittedTimestamp, true);
}

TEST_F(TVersionedChunksHeavyTest, TimestampFullScanExtraKeyColumnLookupSyncLastCommitted)
{
    DoTimestampFullScanExtraKeyColumn(EOptimizeFor::Lookup, SyncLastCommittedTimestamp);
}

TEST_F(TVersionedChunksHeavyTest, GroupsLimitsAndSchemaChange)
{
    DoGroupsLimitsAndSchemaChange();
}

TEST_F(TVersionedChunksHeavyTest, GroupsLimitsAndSchemaChangeNew)
{
    DoGroupsLimitsAndSchemaChange(true);
}

TEST_F(TVersionedChunksHeavyTest, EmptyReadWideSchemaScan)
{
    DoEmptyReadWideSchema(EOptimizeFor::Scan);
}

TEST_F(TVersionedChunksHeavyTest, EmptyReadWideSchemaScanNew)
{
    DoEmptyReadWideSchema(EOptimizeFor::Scan, true);
}

TEST_F(TVersionedChunksHeavyTest, EmptyReadWideSchemaLookup)
{
    DoEmptyReadWideSchema(EOptimizeFor::Lookup);
}

// TODO(lukyan): Fix and enable test.
#if 0
TEST_F(TVersionedChunksHeavyTest, StressTestScan)
{
    DoStressTest(EOptimizeFor::Scan);
}
#endif

TEST_F(TVersionedChunksHeavyTest, StressTestScanNew)
{
    DoStressTest(EOptimizeFor::Scan, true);
}

TEST_F(TVersionedChunksHeavyTest, StressTestLookup)
{
    DoStressTest(EOptimizeFor::Lookup);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TVersionedChunksHeavyTestWithParametrizedBounds, ReadWideSchemaWithNonscalarBoundsScan)
{
    auto bounds = GetParam();
    DoReadWideSchemaWithSpecifiedBounds(EOptimizeFor::Scan, bounds);
}

TEST_P(TVersionedChunksHeavyTestWithParametrizedBounds, ReadWideSchemaWithNonscalarBoundsScanNew)
{
    auto bounds = GetParam();
    DoReadWideSchemaWithSpecifiedBounds(EOptimizeFor::Scan, bounds, true);
}

TEST_P(TVersionedChunksHeavyTestWithParametrizedBounds, ReadWideSchemaWithNonscalarBoundsLookup)
{
    auto bounds = GetParam();
    DoReadWideSchemaWithSpecifiedBounds(EOptimizeFor::Lookup, bounds);
}

INSTANTIATE_TEST_SUITE_P(
    VersionedChunkWithNonscalarBoundsTest,
    TVersionedChunksHeavyTestWithParametrizedBounds,
    ::testing::Values(
        std::make_pair(DoMakeMinSentinel(), DoMakeMinSentinel()),
        std::make_pair(DoMakeMinSentinel(), DoMakeNullSentinel()),
        std::make_pair(DoMakeMinSentinel(), DoMakeMaxSentinel()),
        std::make_pair(DoMakeNullSentinel(), DoMakeNullSentinel()),
        std::make_pair(DoMakeNullSentinel(), DoMakeMaxSentinel()),
        std::make_pair(DoMakeMaxSentinel(), DoMakeMaxSentinel())));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
