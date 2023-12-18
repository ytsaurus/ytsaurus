#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/sorted_merging_reader.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <benchmark/benchmark.h>
#include <library/cpp/testing/gbenchmark/benchmark.h> // XXX

#include <random>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetTestTableSchema()
{
    TColumnSchema columnSchema(
        /*name*/ "k",
        /*type*/ EValueType::Int64,
        /*sortOrder*/ ESortOrder::Ascending);
    return New<TTableSchema>(std::vector{columnSchema});
}

TNameTablePtr GetTestNameTable()
{
    auto nameTable = New<TNameTable>();
    nameTable->RegisterName("k");
    nameTable->RegisterName(TableIndexColumnName);
    return nameTable;
}

////////////////////////////////////////////////////////////////////////////////

struct TStreamDescription
{
    const i64 Index;
    const i64 From;
    const i64 To;
    const i64 Step;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamReader
    : public ISchemalessMultiChunkReader
{
public:
    explicit TStreamReader(TStreamDescription description)
        : CurrentKey_(description.From)
        , StreamIndex_(description.Index)
        , From_(description.From)
        , To_(description.To)
        , Step_(description.Step)
        , Schema_(GetTestTableSchema())
        , NameTable_(GetTestNameTable())
    { }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YT_ABORT();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_UNIMPLEMENTED();
    }

    NTableClient::TTimingStatistics GetTimingStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return true;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YT_ABORT();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (CurrentKey_ >= To_) {
            return nullptr;
        }

        Rows_.resize(options.MaxRowsPerRead);
        std::vector<TUnversionedRow> rows(options.MaxRowsPerRead);

        for (i64 index = 0; index < options.MaxRowsPerRead; ++index) {
            if (CurrentKey_ >= To_) {
                break;
            }
            auto key = CurrentKey_;
            CurrentKey_ += Step_;

            auto& row = Rows_[index];
            if (!row) {
                row = TMutableUnversionedRow::Allocate(&Pool_, /*valueCount*/ 2);
            }
            row[0] = MakeUnversionedInt64Value(key);
            row[1] = MakeUnversionedInt64Value(StreamIndex_);
            rows[index] = row;
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        return CurrentRowIndex_;
    }

    TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

    i64 GetSessionRowIndex() const override
    {
        return CurrentRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return (To_ - From_) / Step_;
    }

    void Interrupt() override
    {
        YT_ABORT();
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    i64 CurrentRowIndex_ = 0;

    i64 CurrentKey_;

    const i64 StreamIndex_;
    const i64 From_;
    const i64 To_;
    const i64 Step_;

    TChunkedMemoryPool Pool_;

    std::vector<TMutableUnversionedRow> Rows_;

    const TTableSchemaPtr Schema_;
    const TNameTablePtr NameTable_;
};

////////////////////////////////////////////////////////////////////////////////

void ReadAll(
    const ISchemalessMultiChunkReaderPtr& reader,
    benchmark::State& state)
{
    TRowBatchReadOptions options{
        .MaxRowsPerRead = 100,
    };

    while (auto batch = reader->Read(options)) {
        if (!batch) {
            break;
        }

        if (batch->IsEmpty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        for (auto row : batch->MaterializeRows()) {
            Y_UNUSED(row);
            if (!state.KeepRunning()) {
                return;
            }
        }
    }
}

void RunMerge(
    const std::vector<TStreamDescription>& streamDescriptions,
    benchmark::State& state)
{
    std::vector<ISchemalessMultiChunkReaderPtr> streams;
    streams.reserve(streamDescriptions.size());
    for (const auto& streamDescription : streamDescriptions) {
        streams.push_back(New<TStreamReader>(streamDescription));
    }

    auto schema = GetTestTableSchema();
    auto comparator = schema->ToComparator();
    auto reader = CreateSortedMergingReader(
        streams,
        comparator,
        comparator,
        /*interruptAtKeyEdge*/ true);
    ReadAll(reader, state);
}

void BM_Merge(benchmark::State& state)
{
    std::mt19937 rng(12345);

    auto streamCount = state.range(0);

    std::vector<TStreamDescription> streamDescriptions;
    streamDescriptions.reserve(streamCount);
    for (int index = 0; index < streamCount; ++index) {
        TStreamDescription description{
            .Index = index,
            .From = i64(rng() % 20),
            .To = 1'000'000'000,
            .Step = i64(rng() % 20 + 1),
        };
        streamDescriptions.push_back(description);
    }

    RunMerge(streamDescriptions, state);
}

void BM_MergeSparse(benchmark::State& state)
{
    RunMerge(
        {
            {.Index = 0, .From = 0, .To = 1'000'000'000, .Step = 1},
            {.Index = 0, .From = 0, .To = 1'000'000'000, .Step = 1000},
        },
        state);
}

void RunJoin(
    const std::vector<TStreamDescription>& primaryStreamDescriptions,
    const std::vector<TStreamDescription>& foreignStreamDescriptions,
    benchmark::State& state)
{
    std::vector<ISchemalessMultiChunkReaderPtr> primaryStreams;
    primaryStreams.reserve(primaryStreamDescriptions.size());
    for (const auto& streamDescription : primaryStreamDescriptions) {
        primaryStreams.push_back(New<TStreamReader>(streamDescription));
    }

    std::vector<ISchemalessMultiChunkReaderPtr> foreignStreams;
    foreignStreams.reserve(foreignStreamDescriptions.size());
    for (const auto& streamDescription : foreignStreamDescriptions) {
        foreignStreams.push_back(New<TStreamReader>(streamDescription));
    }

    auto schema = GetTestTableSchema();
    auto comparator = schema->ToComparator();
    auto reader = CreateSortedJoiningReader(
        primaryStreams,
        schema->ToComparator(),
        schema->ToComparator(),
        foreignStreams,
        schema->ToComparator(),
        /*interruptAtKeyEdge*/ true);
    ReadAll(reader, state);
}

[[maybe_unused]] void BM_Join(benchmark::State& state)
{
    std::mt19937 rng(12345);

    auto primaryStreamCount = state.range(0);
    auto foreignStreamCount = state.range(1);

    std::vector<TStreamDescription> primaryStreamDescriptions;
    primaryStreamDescriptions.reserve(primaryStreamCount);
    for (int index = 0; index < primaryStreamCount; ++index) {
        TStreamDescription description{
            .Index = index,
            .From = i64(rng() % 20),
            .To = 1'000'000'000,
            .Step = i64(rng() % 20 + 1),
        };
        primaryStreamDescriptions.push_back(description);
    }

    std::vector<TStreamDescription> foreignStreamDescriptions;
    foreignStreamDescriptions.reserve(foreignStreamCount);
    for (int index = 0; index < foreignStreamCount; ++index) {
        TStreamDescription description{
            .Index = index,
            .From = i64(rng() % 20),
            .To = 1'000'000'000,
            .Step = i64(rng() % 2 + 1),
        };
        foreignStreamDescriptions.push_back(description);
    }

    RunJoin(
        primaryStreamDescriptions,
        foreignStreamDescriptions,
        state);
}

void BM_JoinSparse(benchmark::State& state)
{
    RunJoin(
        {
            {.Index = 0, .From = 0, .To = 1'000'000'000, .Step = 1000},
        },
        {
            {.Index = 0, .From = 0, .To = 1'000'000'000, .Step = 1},
        },
        state);
}

////////////////////////////////////////////////////////////////////////////////

BENCHMARK(BM_Merge)
    ->Arg(2)
    ->Arg(3)
    ->Arg(5)
    ->Arg(10)
    ->Arg(50)
    ->Arg(200);

BENCHMARK(BM_MergeSparse);

BENCHMARK(BM_Join)
    ->Args({2, 1})
    ->Args({3, 1})
    ->Args({5, 2})
    ->Args({10, 10});

BENCHMARK(BM_JoinSparse);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient

BENCHMARK_MAIN();
