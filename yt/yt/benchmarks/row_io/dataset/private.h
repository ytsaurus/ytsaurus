#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessChunkReaderPtr CreateReader(
    const NTableClient::TTableSchemaPtr& schema,
    const NChunkClient::TMemoryWriterPtr& source);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
