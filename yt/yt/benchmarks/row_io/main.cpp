#include "pipeline.h"

#include <yt/yt/benchmarks/row_io/dataset/dataset.h>
#include <yt/yt/benchmarks/row_io/dataset/input_stream.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/formats/arrow_writer.h>
#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <util/system/env.h>

#include <functional>
#include <string>
#include <utility>

namespace NYT::NRowIOBenchmark {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBenchmarkCase
{
    int BatchRowCount = 0;
    std::function<TDataset()> DatasetFactory;
    std::string DatasetLabel;
};

////////////////////////////////////////////////////////////////////////////////

void DeconfigureConcurrency()
{
    auto rpcConfig = New<NRpc::TDispatcherConfig>();
    rpcConfig->CompressionPoolSize = 1;
    rpcConfig->HeavyPoolSize = 1;
    NRpc::TDispatcher::Get()->Configure(rpcConfig);
    auto chunkConfig = New<NChunkClient::TDispatcherConfig>();
    chunkConfig->ChunkReaderPoolSize = 1;
    NChunkClient::TDispatcher::Get()->Configure(chunkConfig);
}

////////////////////////////////////////////////////////////////////////////////

void ReportStatistics(
    TNonNullPtr<benchmark::State> state,
    const TDataset& dataset,
    const TSharedRef& input,
    const TArrowWriterStatisticsPtr& arrowStatistics,
    const TRunResult& totals)
{
    state->SetItemsProcessed(state->iterations() * dataset.RowCount);
    state->SetBytesProcessed(state->iterations() * dataset.LogicalBytes);
    state->counters["output_bytes"] = benchmark::Counter(
        totals.OutputBytes,
        benchmark::Counter::kAvgIterations);
    state->counters["output_blocks"] = benchmark::Counter(
        totals.OutputBlockCount,
        benchmark::Counter::kAvgIterations);
    state->counters["input_bytes"] = input.Size();
    if (arrowStatistics) {
        for (auto encoding : TEnumTraits<EArrowEncoding>::GetDomainValues()) {
            state->counters[Format("arrow_%lv_batches", encoding)] =
                arrowStatistics->ColumnBatchCounts[encoding];
        }
        state->counters["arrow_record_batches"] = arrowStatistics->RecordBatchCount;
    }
    state->counters["source_chunk_bytes"] = GetEncodedSize(dataset.Source);
    state->counters["rows"] = dataset.RowCount;
}

////////////////////////////////////////////////////////////////////////////////

void RunBenchmark(
    TNonNullPtr<benchmark::State> state,
    const TFormat& format,
    const TBenchmarkCase& config)
{
    DeconfigureConcurrency();
    // Chunk readers reject GetSyncInvoker(), so preparation also needs the action queue.
    auto coordinator = New<TActionQueue>("RowIO");
    auto dataset = BIND([&] {
        return config.DatasetFactory();
    }).AsyncVia(coordinator->GetInvoker()).Run().BlockingGet()
        .ValueOrThrow();
    state->SetLabel(config.DatasetLabel);

    auto arrowStatistics = format.GetType() == EFormatType::Arrow
        ? New<TArrowWriterStatistics>()
        : nullptr;
    auto input = BIND([&] {
        return PrepareInputStream(format, dataset, config.BatchRowCount, arrowStatistics);
    }).AsyncVia(coordinator->GetInvoker()).Run().BlockingGet()
        .ValueOrThrow();
    TRunResult totals;
    bool validated = false;
    for ([[maybe_unused]] auto iteration : *state) {
        auto result = BIND([&] {
            return RunIteration(
                format,
                dataset,
                input,
                /*validate*/ !validated);
        }).AsyncVia(coordinator->GetInvoker()).Run().BlockingGet();
        const auto& statistics = result
            .ValueOrThrow();
        state->SetIterationTime(statistics.Seconds);
        validated = true;
        totals.OutputBytes += statistics.OutputBytes;
        totals.OutputBlockCount += statistics.OutputBlockCount;
    }
    ReportStatistics(
        state,
        dataset,
        input,
        arrowStatistics,
        totals);
}

////////////////////////////////////////////////////////////////////////////////

void RegisterCase(
    const std::string& name,
    const std::string& formatYson,
    TBenchmarkCase config)
{
    benchmark::RegisterBenchmark(
        name,
        [formatYson, config = std::move(config)] (benchmark::State& state) {
            auto format = ConvertTo<TFormat>(TYsonString(TStringBuf(formatYson)));
            RunBenchmark(&state, format, config);
        })
        ->UseManualTime();
}

////////////////////////////////////////////////////////////////////////////////

void RegisterRandomCases(const std::string& formatName, const std::string& formatYson)
{
    for (const auto& [columnCount, rowCount] : {std::pair{8, 32768}, std::pair{32, 8192}}) {
        for (int batchRowCount : {128, 1024}) {
            auto name = Format(
                "%v/columns:%v/rows:%v/batch_rows:%v",
                formatName,
                columnCount,
                rowCount,
                batchRowCount);
            RegisterCase(name, formatYson, {
                .BatchRowCount = batchRowCount,
                .DatasetFactory = [columnCount, rowCount] {
                    return GenerateRandomDataset(columnCount, rowCount);
                },
            });
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void RegisterCases(const std::string& formatName, const std::string& formatYson)
{
    auto datasetPath = GetEnv("ROW_IO_DATASET");
    if (datasetPath.empty()) {
        RegisterRandomCases(formatName, formatYson);
        return;
    }

    for (int batchRowCount : {128, 1024, 4096}) {
        auto name = Format("%v/batch_rows:%v", formatName, batchRowCount);
        RegisterCase(name, formatYson, {
            .BatchRowCount = batchRowCount,
            .DatasetFactory = [datasetPath] {
                return ReadLocalDataset(datasetPath);
            },
            .DatasetLabel = datasetPath,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] const bool BenchmarksRegistered = [] {
    RegisterCases("Yson", "<format=binary>yson");
    RegisterCases("YsonText", "<format=text>yson");
    RegisterCases("Arrow", "arrow");
    RegisterCases("Json", "<annotate_with_types=%true>json");
    return true;
}();

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
