#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <library/cpp/yt/cpu_clock/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IProfilingMultiChunkReader
    : public NTableClient::ISchemalessMultiChunkReader
{
    virtual std::optional<TDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingMultiChunkReader)

IProfilingMultiChunkReaderPtr CreateProfilingMultiChunkReader(
    NTableClient::ISchemalessMultiChunkReaderPtr underlying,
    TCpuInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
