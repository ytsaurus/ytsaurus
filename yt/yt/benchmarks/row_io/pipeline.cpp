#include "pipeline.h"

#include <yt/yt/benchmarks/row_io/dataset/dataset.h>
#include <yt/yt/benchmarks/row_io/dataset/multi_chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>

namespace NYT::NRowIOBenchmark {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NProfiling;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 InputBlockSize = 64_KB;

////////////////////////////////////////////////////////////////////////////////

class TOutputPipeline
{
public:
    TOutputPipeline(
        const TFormat& format,
        const TTableSchemaPtr& schema)
        : Collector_(New<TMemoryChunkCollector>())
        , Writer_(CreateMemoryMultiChunkWriter(
            schema,
            TNameTable::FromSchema(*schema),
            Collector_,
            OutputDesiredChunkSize))
        , Consumer_(
            Writer_,
            ConvertTo<TTypeConversionConfigPtr>(format.Attributes()))
        , Output_(CreateParserForFormat(format, &Consumer_))
    { }

    TRunResult Run(TRef input)
    {
        TWallTimer timer;
        auto* current = input.Begin();
        while (current != input.End()) {
            auto size = std::min<i64>(input.End() - current, InputBlockSize);
            Output_.Write(current, size);
            current += size;
        }
        Output_.Finish();
        WaitFor(Consumer_.Flush())
            .ThrowOnError();
        WaitFor(Writer_->Close())
            .ThrowOnError();
        double seconds = timer.GetElapsedTime().SecondsFloat();
        Chunks_ = Collector_->GetCompletedChunks(Writer_);
        TRunResult result{
            .Seconds = seconds,
            .OutputChunkCount = static_cast<int>(Chunks_.size()),
        };
        for (const auto& chunk : Chunks_) {
            result.OutputBytes += GetEncodedSize(chunk);
            result.OutputBlockCount += static_cast<int>(chunk->GetBlocks().size());
        }
        return result;
    }

    void Validate(const TDataset& dataset)
    {
        ValidateOutput(dataset.Schema, dataset.Source, Chunks_, dataset.RowCount);
    }

private:
    const TMemoryChunkCollectorPtr Collector_;
    const ISchemalessMultiChunkWriterPtr Writer_;
    std::vector<TMemoryWriterPtr> Chunks_;

    TWritingValueConsumer Consumer_;
    TTableOutput Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRunResult RunIteration(
    const TFormat& format,
    const TDataset& dataset,
    TRef input,
    bool validate)
{
    TOutputPipeline pipeline(format, dataset.Schema);
    auto result = pipeline.Run(input);
    if (validate) {
        pipeline.Validate(dataset);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
