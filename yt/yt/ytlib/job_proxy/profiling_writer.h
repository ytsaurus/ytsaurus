#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/library/formats/format.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class IProfilingMultiChunkWriter
    : public NTableClient::ISchemalessMultiChunkWriter
{
public:
    virtual std::optional<TCpuDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////


class IProfilingSchemalessFormatWriter
    : public NFormats::ISchemalessFormatWriter
{
public:
    virtual std::optional<TCpuDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingSchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

IProfilingMultiChunkWriterPtr CreateProfilingMultiChunkWriter(
    NTableClient::ISchemalessMultiChunkWriterPtr underlying, TCpuInstant start);

IProfilingSchemalessFormatWriterPtr CreateProfilingSchemalessFormatWriter(
    NFormats::ISchemalessFormatWriterPtr underlying, TCpuInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
