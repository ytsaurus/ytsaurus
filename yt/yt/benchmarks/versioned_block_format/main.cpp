#include <benchmark/benchmark.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_reader.h>
#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_writer.h>

#include <yt/yt/ytlib/table_client/versioned_block_reader.h>
#include <yt/yt/ytlib/table_client/versioned_block_writer.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/yt/memory/new.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/fwd.h>

#include <util/random/random.h>
#include <util/random/mersenne.h>
#include <util/random/shuffle.h>

#include <random>

namespace NYT {
namespace {

using namespace NTableClient;
using namespace NProfiling;
using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Benchmark");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReaderMode,
    (Iterative)
    (SkipToKey)
);

struct TBenchmarkOptions
{
    bool Validate = false;

    TString SchemaName;
};

////////////////////////////////////////////////////////////////////////////////

struct TMiscOptions
{
    double DeleteToWriteTimestampRatio = 0.5;

    double NullKeyPercent = 1;
    double AggregateFlagPercent = 50;
    double NullValuePercent = 1;

    int RowCount = 1000;

    NCompression::ECodec CompressionCodec = NCompression::ECodec::None;

    TTimestamp ReadTimestamp = SyncLastCommittedTimestamp;
};

struct TRowGeneratorOptions
{
    int Seed = 42;

    int ValueCountPoissonMean = 3;
    int StringLengthPoissonMean = 100;

    TTimestamp MaxTimestamp = 100000;
};

struct TBlockInfo
{
    TSharedRef Data;
    NTableClient::NProto::TDataBlockMeta Meta;
    std::vector<TVersionedRow> Rows;
};

////////////////////////////////////////////////////////////////////////////////

class TRowGenerator
{
public:
    TRowGenerator(
        TRowGeneratorOptions options,
        TChunkedMemoryPool* memoryPool)
        : Options_(std::move(options))
        , MemoryPool_(memoryPool)
        , Generator_(Options_.Seed)
        , ValueCountDistr_(Options_.ValueCountPoissonMean)
        , StringLengthDistr_(Options_.StringLengthPoissonMean)
    { }

    TVersionedRow GenerateRow(
        const TTableSchemaPtr& schema,
        const TMiscOptions& options)
    {
        std::vector<std::vector<TTimestamp>> writeTimestamps;
        std::set<TTimestamp, std::greater<TTimestamp>> writeTimestampSet;
        std::set<TTimestamp, std::greater<TTimestamp>> deleteTimestampSet;

        int totalValueCount = 0;
        for (int i = 0; i < schema->GetColumnCount() - schema->GetKeyColumnCount(); ++i) {
            int valueCount = GenValueCount();
            totalValueCount += valueCount;

            std::set<TTimestamp, std::greater<TTimestamp>> columnWriteTimestampSet;
            while (std::ssize(columnWriteTimestampSet) < valueCount) {
                auto newTimestamp = GenTimestamp();
                if (!columnWriteTimestampSet.contains(newTimestamp)) {
                    columnWriteTimestampSet.insert(newTimestamp);
                    writeTimestampSet.insert(newTimestamp);
                }
            }

            writeTimestamps.push_back({columnWriteTimestampSet.begin(), columnWriteTimestampSet.end()});
        }

        int deleteTimestampCount = totalValueCount * options.DeleteToWriteTimestampRatio;
        while (std::ssize(deleteTimestampSet) < deleteTimestampCount) {
            auto newTimestamp = GenTimestamp();
            if (!writeTimestampSet.contains(newTimestamp) &&
                !deleteTimestampSet.contains(newTimestamp))
            {
                deleteTimestampSet.insert(newTimestamp);
            }
        }

        YT_LOG_DEBUG("Row generation statistics "
            "(TotalValueCount: %v, WriteTimestampCount: %v, DeleteTimestampCount: %v, ColumnValueCounts: %v)",
            totalValueCount,
            writeTimestampSet.size(),
            deleteTimestampSet.size(),
            MakeFormattableView(
                MakeRange(writeTimestamps.begin(), writeTimestamps.end()),
                [] (auto* builder, const auto& timestamps) {
                    builder->AppendFormat("%v", timestamps.size());
                }));

        auto row = TMutableVersionedRow::Allocate(
            MemoryPool_,
            schema->GetKeyColumnCount(),
            totalValueCount,
            writeTimestampSet.size(),
            deleteTimestampSet.size());

        for (int i = 0; i < schema->GetKeyColumnCount(); ++i) {
            row.Keys()[i] = GenUnversionedValue(i, schema, options);
        }

        int valueIndex = 0;
        for (int i = schema->GetKeyColumnCount(); i < schema->GetColumnCount(); ++i) {
            for (auto timestamp : writeTimestamps[i - schema->GetKeyColumnCount()]) {
                row.Values()[valueIndex++] = GenVersionedValue(
                    i,
                    schema,
                    options,
                    timestamp);
            }
        }

        int writeTimestampIndex = 0;
        for (auto writeTimestamp : writeTimestampSet) {
            row.WriteTimestamps()[writeTimestampIndex++] = writeTimestamp;
        }

        int deleteTimestampIndex = 0;
        for (auto deleteTimestamp : deleteTimestampSet) {
            row.DeleteTimestamps()[deleteTimestampIndex++] = deleteTimestamp;
        }

        return row;
    }

private:
    const TRowGeneratorOptions Options_;
    TChunkedMemoryPool* const MemoryPool_;

    TMersenne<ui64> Generator_;

    std::poisson_distribution<int> ValueCountDistr_;
    std::poisson_distribution<int> StringLengthDistr_;


    int GenValueCount()
    {
        return ValueCountDistr_(Generator_);
    }

    TTimestamp GenTimestamp()
    {
        return Generator_.Uniform(Options_.MaxTimestamp);
    }

    TStringBuf GenerateString()
    {
        auto length = StringLengthDistr_(Generator_);

        auto* buf = MemoryPool_->AllocateAligned(length);
        for (int i = 0; i < length; ++i) {
            buf[i] = 'a' + Generator_.Uniform(25);
        }

        return TStringBuf(buf, length);
    }

    TUnversionedValue GenUnversionedValue(
        int columnId,
        const TTableSchemaPtr& schema,
        const TMiscOptions& options)
    {
        if (Generator_.Uniform(100) < options.NullKeyPercent) {
            return MakeUnversionedSentinelValue(EValueType::Null, columnId);
        }

        switch (schema->Columns()[columnId].GetWireType()) {
            case EValueType::String:
                return MakeUnversionedStringValue(GenerateString(), columnId);
            case EValueType::Int64:
                return MakeUnversionedInt64Value(Generator_(), columnId);
            case EValueType::Uint64:
                return MakeUnversionedUint64Value(Generator_(), columnId);
            case EValueType::Boolean:
                return MakeUnversionedBooleanValue(static_cast<bool>(Generator_() & 1), columnId);
            case EValueType::Double:
                return MakeUnversionedDoubleValue(static_cast<double>(Generator_()), columnId);
            default:
                YT_ABORT();
        }
    }

    TVersionedValue GenVersionedValue(
        int columnId,
        const TTableSchemaPtr& schema,
        const TMiscOptions& options,
        TTimestamp timestamp)
    {
        EValueFlags aggregate = EValueFlags::None;
        if (schema->Columns()[columnId].Aggregate() &&
            Generator_.Uniform(100) < options.AggregateFlagPercent)
        {
            aggregate |= EValueFlags::Aggregate;
        }

        if (Generator_.Uniform(100) < options.NullValuePercent) {
            return MakeVersionedSentinelValue(EValueType::Null, timestamp, columnId, aggregate);
        }

        switch (schema->Columns()[columnId].GetWireType()) {
            case EValueType::String:
                return MakeVersionedStringValue(GenerateString(), timestamp, columnId, aggregate);
            case EValueType::Int64:
                return MakeVersionedInt64Value(Generator_(), timestamp, columnId, aggregate);
            case EValueType::Uint64:
                return MakeVersionedUint64Value(Generator_(), timestamp, columnId, aggregate);
            case EValueType::Boolean:
                return MakeVersionedBooleanValue(static_cast<bool>(Generator_() & 1), timestamp, columnId, aggregate);
            case EValueType::Double:
                return MakeVersionedDoubleValue(static_cast<double>(Generator_()), timestamp, columnId, aggregate);
            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

static const auto SmallSchema1 = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("v1", EValueType::Int64)
});

static const auto SmallSchema2 = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::Uint64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::String).SetSortOrder(ESortOrder::Ascending),

    TColumnSchema("v1", EValueType::Int64).SetAggregate(TString("max")),
    TColumnSchema("v2", EValueType::String),
    TColumnSchema("v3", EValueType::Boolean),
});

static const auto LargeSchema1 = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k4", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k5", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k6", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k7", EValueType::Uint64).SetSortOrder(ESortOrder::Ascending),

    TColumnSchema("v1", EValueType::Int64).SetAggregate(TString("max")),
    TColumnSchema("v2", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v3", EValueType::Int64),
    TColumnSchema("v4", EValueType::Double).SetGroup("group2").SetAggregate(TString("max")),
    TColumnSchema("v5", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v6", EValueType::Double).SetGroup("group3").SetAggregate(TString("max")),
    TColumnSchema("v7", EValueType::Int64),
    TColumnSchema("v8", EValueType::Uint64).SetGroup("group3"),
    TColumnSchema("v9", EValueType::Uint64).SetGroup("group3").SetAggregate(TString("max")),
    TColumnSchema("v10", EValueType::Int64),
    TColumnSchema("v11", EValueType::Double).SetAggregate(TString("max")),
    TColumnSchema("v12", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v13", EValueType::Int64).SetGroup("group2").SetAggregate(TString("max")),
    TColumnSchema("v14", EValueType::Boolean).SetGroup("group2"),
    TColumnSchema("v15", EValueType::Int64).SetGroup("group2")
});

static const auto LargeSchema2 = New<TTableSchema>(std::vector{
    TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k3", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k4", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k5", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k6", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    TColumnSchema("k7", EValueType::Uint64).SetSortOrder(ESortOrder::Ascending),

    TColumnSchema("v1", EValueType::String).SetAggregate(TString("max")),
    TColumnSchema("v2", EValueType::String).SetGroup("group1"),
    TColumnSchema("v3", EValueType::Int64),
    TColumnSchema("v4", EValueType::Double).SetGroup("group2").SetAggregate(TString("max")),
    TColumnSchema("v5", EValueType::String).SetGroup("group1"),
    TColumnSchema("v6", EValueType::Double).SetGroup("group3").SetAggregate(TString("max")),
    TColumnSchema("v7", EValueType::Int64),
    TColumnSchema("v8", EValueType::Uint64).SetGroup("group3"),
    TColumnSchema("v9", EValueType::Uint64).SetGroup("group3").SetAggregate(TString("max")),
    TColumnSchema("v10", EValueType::String),
    TColumnSchema("v11", EValueType::Double).SetAggregate(TString("max")),
    TColumnSchema("v12", EValueType::Int64).SetGroup("group1"),
    TColumnSchema("v13", EValueType::String).SetGroup("group2").SetAggregate(TString("max")),
    TColumnSchema("v14", EValueType::Boolean).SetGroup("group2"),
    TColumnSchema("v15", EValueType::String).SetGroup("group2")
});

static THashMap<TString, TTableSchemaPtr> Schemas = {
    {"SmallSchema1", SmallSchema1},
    {"SmallSchema2", SmallSchema2},
    {"LargeSchema1", LargeSchema1},
    {"LargeSchema2", LargeSchema2}
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TVersionedRow> GenerateRows(
    const TMiscOptions& options,
    const TTableSchemaPtr& schema,
    TChunkedMemoryPool* memoryPool)
{
    std::vector<TVersionedRow> rows;
    rows.reserve(options.RowCount);

    TRowGenerator generator(TRowGeneratorOptions{}, memoryPool);

    for (int i = 0; i < options.RowCount; ++i) {
        auto row = generator.GenerateRow(
            schema,
            options);
        rows.push_back(row);
    }

    Sort(rows, [&] (const TVersionedRow& lhs, const TVersionedRow& rhs) {
        return CompareValueRanges(lhs.Keys(), rhs.Keys()) < 0;
    });
    rows.erase(
        std::unique(rows.begin(), rows.end(), [&] (const TVersionedRow& lhs, const TVersionedRow& rhs) {
            return CompareValueRanges(lhs.Keys(), rhs.Keys()) == 0;
        }),
        rows.end());

    return rows;
}

////////////////////////////////////////////////////////////////////////////////

struct TVersionedSimpleBlockFormatAdapter
{
    using TBlockReader = TSimpleVersionedBlockReader;

    static TSimpleVersionedBlockWriter CreateBlockWriter(TTableSchemaPtr schema)
    {
        return TSimpleVersionedBlockWriter(std::move(schema));
    }
};

struct TVersionedSlimBlockFormatAdapter
{
    using TBlockReader = TSlimVersionedBlockReader;

    static TSlimVersionedBlockWriter CreateBlockWriter(TTableSchemaPtr schema)
    {
        return TSlimVersionedBlockWriter(
            New<TSlimVersionedWriterConfig>(),
            std::move(schema));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockFormatAdapter>
class TBenchmarkBase
{
protected:
    const std::vector<TVersionedRow>& GetRows(const TTableSchemaPtr& schema)
    {
        if (!Rows_) {
            Rows_ = GenerateRows(Options_, schema, &MemoryPool_);
        }

        return *Rows_;
    }

    const TBlockInfo& GetBlockInfo(const TTableSchemaPtr& schema)
    {
        if (!BlockInfo_) {
            auto block = WriteBlock(schema);
            auto data = GetCodec(Options_.CompressionCodec)->Compress(block.Data);
            BlockInfo_ = TBlockInfo{
                .Data = data,
                .Meta = block.Meta,
                .Rows = GetRows(schema),
            };
        }

        return *BlockInfo_;
    }

    TBlock WriteBlock(TTableSchemaPtr schema)
    {
        const auto& rows = GetRows(schema);
        auto blockWriter = TBlockFormatAdapter::CreateBlockWriter(std::move(schema));
        for (const auto& row : rows) {
            blockWriter.WriteRow(row);
        }
        return blockWriter.FlushBlock();
    }

    std::vector<TMutableVersionedRow> ReadRows(
        const TTableSchemaPtr& schema,
        bool versioned,
        EReaderMode mode)
    {
        const auto& blockInfo = GetBlockInfo(schema);

        std::vector<TColumnIdMapping> schemaIdMapping;
        schemaIdMapping.reserve(schema->GetColumnCount() - schema->GetKeyColumnCount());
        for (int i = schema->GetKeyColumnCount(); i < schema->GetColumnCount(); ++i) {
            schemaIdMapping.push_back({i, i});
        }

        typename TBlockFormatAdapter::TBlockReader blockReader(
            blockInfo.Data,
            blockInfo.Meta,
            /*blockFormatVersion*/ 1,
            schema,
            schema->GetKeyColumnCount(),
            schemaIdMapping,
            KeyComparer_,
            // all committed ts ?
            Options_.ReadTimestamp,
            versioned);
        blockReader.SkipToRowIndex(0);

        std::vector<TMutableVersionedRow> rows;
        rows.reserve(blockInfo.Rows.size());

        switch (mode) {
            case EReaderMode::Iterative:
                do {
                    rows.push_back(blockReader.GetRow(&MemoryPool_));
                } while (blockReader.NextRow());
                break;

            case EReaderMode::SkipToKey:
                for (auto key : GetKeys(schema)) {
                    YT_VERIFY(blockReader.SkipToKey(key));
                    rows.push_back(blockReader.GetRow(&MemoryPool_));
                }
                break;

            default:
                YT_ABORT();
        }

        return rows;
    }

private:
    const TMiscOptions Options_ = {};
    const TKeyComparer KeyComparer_;

    TChunkedMemoryPool MemoryPool_;
    std::optional<std::vector<TVersionedRow>> Rows_;
    std::optional<TBlockInfo> BlockInfo_;

    std::optional<std::vector<TMutableUnversionedRow>> Keys_;


    const std::vector<TMutableUnversionedRow>& GetKeys(const TTableSchemaPtr& schema)
    {
        if (!Keys_) {
            const auto& blockInfo = GetBlockInfo(schema);
            std::vector<TMutableUnversionedRow> keys;
            keys.reserve(blockInfo.Rows.size());
            for (auto row : blockInfo.Rows) {
                keys.push_back(TMutableUnversionedRow::Allocate(
                    &MemoryPool_,
                    row.GetKeyCount()));
                for (int i = 0; i < row.GetKeyCount(); ++i) {
                    keys.back().Elements()[i] = row.Keys()[i];
                }
            }
            Keys_ = keys;
        }

        return *Keys_;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockFormatAdapter>
class TBlockWriterFixture
    : public benchmark::Fixture
    , public TBenchmarkBase<TBlockFormatAdapter>
{
protected:
    using TBenchmarkBase<TBlockFormatAdapter>::GetRows;
    using TBenchmarkBase<TBlockFormatAdapter>::WriteBlock;


    void RunBenchmark(
        benchmark::State& state,
        const TTableSchemaPtr& schema)
    {
        const auto& rows = GetRows(schema);
        i64 dataWeight = 0;
        for (auto row : rows) {
            dataWeight += GetDataWeight(row);
        }

        state.counters["DataWeight"] = benchmark::Counter(0, benchmark::Counter::kIsRate);
        state.counters["BlockSize"] = benchmark::Counter(0, benchmark::Counter::kIsRate);
        for (auto _ : state) {
            auto block = WriteBlock(schema);
            state.counters["DataWeight"] += dataWeight;
            state.counters["BlockSize"] += GetByteSize(block.Data);
            DoNotOptimizeAway(block);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA_AND_FORMAT(Schema, ChunkFormat) \
    BENCHMARK_TEMPLATE_DEFINE_F(TBlockWriterFixture, Schema##_##ChunkFormat, T##ChunkFormat##BlockFormatAdapter)(benchmark::State& state) \
    { \
        RunBenchmark(state, Schema); \
    } \
    BENCHMARK_REGISTER_F(TBlockWriterFixture, Schema##_##ChunkFormat)->Unit(benchmark::kMicrosecond);

#define REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA(Schema) \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA_AND_FORMAT(Schema, VersionedSimple) \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA_AND_FORMAT(Schema, VersionedSlim)

#define REGISTER_BLOCK_WRITER_BENCHMARKS() \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA(SmallSchema1) \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA(SmallSchema2) \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA(LargeSchema1) \
    REGISTER_BLOCK_WRITER_BENCHMARKS_FOR_SCHEMA(LargeSchema2)

REGISTER_BLOCK_WRITER_BENCHMARKS()

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockFormatAdapter>
class TBlockReaderFixture
    : public benchmark::Fixture
    , public TBenchmarkBase<TBlockFormatAdapter>
{
protected:
    using TBenchmarkBase<TBlockFormatAdapter>::GetBlockInfo;
    using TBenchmarkBase<TBlockFormatAdapter>::ReadRows;


    void RunBenchmark(
        benchmark::State& state,
        const TTableSchemaPtr& schema,
        bool versioned,
        EReaderMode mode)
    {
        const auto& blockInfo = GetBlockInfo(schema);
        auto blockSize = blockInfo.Data.Size();
        std::optional<i64> dataWeight;

        state.counters["DataWeight"] = benchmark::Counter(0, benchmark::Counter::kIsRate);
        state.counters["BlockSize"] = benchmark::Counter(0, benchmark::Counter::kIsRate);
        for (auto _ : state) {
            auto rows = ReadRows(schema, versioned, mode);
            if (!dataWeight) {
                dataWeight = 0;
                for (auto row : rows) {
                    *dataWeight += GetDataWeight(row);
                }
            }
            DoNotOptimizeAway(rows);
            state.counters["DataWeight"] += *dataWeight;
            state.counters["BlockSize"] += blockSize;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE_AND_FORMAT(Schema, Mode, ChunkFormat) \
    BENCHMARK_TEMPLATE_DEFINE_F(TBlockReaderFixture, Schema##_##Mode##_Versioned##_##ChunkFormat, T##ChunkFormat##BlockFormatAdapter)(benchmark::State& state) \
    { \
        RunBenchmark(state, Schema, true, EReaderMode::Mode); \
    } \
    BENCHMARK_REGISTER_F(TBlockReaderFixture, Schema##_##Mode##_Versioned##_##ChunkFormat)->Unit(benchmark::kMicrosecond); \
    BENCHMARK_TEMPLATE_DEFINE_F(TBlockReaderFixture, Schema##_##Mode##_Unversioned##_##ChunkFormat, T##ChunkFormat##BlockFormatAdapter)(benchmark::State& state) \
    { \
        RunBenchmark(state, Schema, false, EReaderMode::Mode); \
    } \
    BENCHMARK_REGISTER_F(TBlockReaderFixture, Schema##_##Mode##_Unversioned##_##ChunkFormat)->Unit(benchmark::kMicrosecond);

#define REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE(Schema, Mode) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE_AND_FORMAT(Schema, Mode, VersionedSimple) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE_AND_FORMAT(Schema, Mode, VersionedSlim)

#define REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA(Schema) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE(Schema, Iterative) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA_AND_MODE(Schema, SkipToKey)

#define REGISTER_BLOCK_READER_BENCHMARKS() \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA(SmallSchema1) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA(SmallSchema2) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA(LargeSchema1) \
    REGISTER_BLOCK_READER_BENCHMARKS_FOR_SCHEMA(LargeSchema2)

REGISTER_BLOCK_READER_BENCHMARKS()

////////////////////////////////////////////////////////////////////////////////

}  // namespace
}  // namespace NYT

////////////////////////////////////////////////////////////////////////////////

BENCHMARK_MAIN();
