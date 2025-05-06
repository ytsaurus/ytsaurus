#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class IProfilingMultiChunkReader
    : public NTableClient::ISchemalessMultiChunkReader
{
public:
    virtual std::optional<TCpuDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingMultiChunkReader)

IProfilingMultiChunkReaderPtr CreateProfilingMultiChunkReader(
    NTableClient::ISchemalessMultiChunkReaderPtr underlying, TCpuInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
