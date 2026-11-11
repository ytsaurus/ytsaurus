#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <string>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

struct TDataset
{
    NTableClient::TTableSchemaPtr Schema;
    NChunkClient::TMemoryWriterPtr Source;
    i64 RowCount = 0;
    i64 LogicalBytes = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateOutput(
    const NTableClient::TTableSchemaPtr& schema,
    const NChunkClient::TMemoryWriterPtr& expected,
    const NChunkClient::TMemoryWriterPtr& actual,
    i64 expectedRowCount);

i64 GetEncodedSize(const NChunkClient::TMemoryWriterPtr& sink);

TDataset GenerateRandomDataset(int columnCount, int rowCount);

TDataset ReadLocalDataset(const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
