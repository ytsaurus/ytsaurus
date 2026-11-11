#include "writer.h"

#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NRowIOBenchmark {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateWriter(
    const TTableSchemaPtr& schema,
    const TMemoryWriterPtr& sink,
    EOptimizeFor optimizeFor)
{
    auto config = New<TChunkWriterConfig>();
    config->BlockSize = 256_KB;
    config->CompressionConcurrency = 1;
    config->VerifyCompression = false;
    config->Postprocess();
    auto options = New<TChunkWriterOptions>();
    options->OptimizeFor = optimizeFor;
    options->CompressionCodec = NCompression::ECodec::None;
    options->Postprocess();
    return CreateSchemalessChunkWriter(
        config,
        options,
        schema,
        TNameTable::FromSchema(*schema),
        /*chunkWriter*/ sink,
        /*writeBlocksOptions*/ {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
