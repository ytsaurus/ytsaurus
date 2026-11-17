#include "multi_chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NRowIOBenchmark {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TUnderlyingChunkWriterFactory TMemoryChunkCollector::GetFactory()
{
    return BIND(&TMemoryChunkCollector::CreateSink, MakeStrong(this));
}

IChunkWriterPtr TMemoryChunkCollector::CreateSink()
{
    auto guard = Guard(SpinLock_);
    auto chunkId = MakeRandomId(EObjectType::Chunk, TCellTag(1));
    while (Sinks_.contains(chunkId)) {
        chunkId = MakeRandomId(EObjectType::Chunk, TCellTag(1));
    }
    auto sink = New<TMemoryWriter>(chunkId);
    Sinks_.emplace(chunkId, sink);
    return sink;
}

std::vector<TMemoryWriterPtr> TMemoryChunkCollector::Finish(
    const ISchemalessMultiChunkWriterPtr& writer)
{
    WaitFor(writer->Close())
        .ThrowOnError();
    return GetCompletedChunks(writer);
}

std::vector<TMemoryWriterPtr> TMemoryChunkCollector::GetCompletedChunks(
    const ISchemalessMultiChunkWriterPtr& writer)
{
    auto guard = Guard(SpinLock_);
    std::vector<TMemoryWriterPtr> result;
    for (const auto& spec : writer->GetWrittenChunkSpecs()) {
        auto sinkIt = Sinks_.find(FromProto<TChunkId>(spec.chunk_id()));
        YT_VERIFY(sinkIt != Sinks_.end());
        result.push_back(sinkIt->second);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateMemoryMultiChunkWriter(
    const TTableSchemaPtr& schema,
    const TNameTablePtr& nameTable,
    const TMemoryChunkCollectorPtr& collector,
    i64 desiredChunkSize)
{
    for (const auto& column : schema->Columns()) {
        if (column.Expression()) {
            THROW_ERROR_EXCEPTION("Offline writer does not support computed columns");
        }
    }
    auto config = New<TTableWriterConfig>();
    config->BlockSize = 256_KB;
    config->CompressionConcurrency = 1;
    config->VerifyCompression = false;
    config->DesiredChunkSize = desiredChunkSize;
    config->Postprocess();
    auto options = New<TTableWriterOptions>();
    options->TableSchema = schema;
    options->EvaluateComputedColumns = false;
    options->EnableSkynetSharing = false;
    options->ComputeDigest = false;
    options->OptimizeFor = EOptimizeFor::Scan;
    options->CompressionCodec = NCompression::ECodec::None;
    options->ErasureCodec = NErasure::ECodec::None;
    options->Postprocess();
    return CreateSchemalessMultiChunkWriter(
        config,
        options,
        nameTable,
        schema,
        /*lastKey*/ {},
        /*client*/ nullptr,
        /*localHostName*/ {},
        TCellTag(1),
        /*transactionId*/ {},
        /*schemaId*/ {},
        /*dataSink*/ {},
        /*writeBlocksOptions*/ {},
        NullChunkListId,
        TChunkTimestamps(),
        /*trafficMeter*/ nullptr,
        GetUnlimitedThrottler(),
        GetNullBlockCache(),
        /*boundaryKeysProcessor*/ {},
        collector->GetFactory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
