#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessChunkWriterPtr CreateWriter(
    const NTableClient::TTableSchemaPtr& schema,
    const NChunkClient::TMemoryWriterPtr& sink,
    NTableClient::EOptimizeFor optimizeFor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
