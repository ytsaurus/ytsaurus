#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/library/formats/format.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IProfilingMultiChunkWriter
    : public NTableClient::ISchemalessMultiChunkWriter
{
    virtual std::optional<TDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

struct IProfilingSchemalessFormatWriter
    : public NFormats::ISchemalessFormatWriter
{
    virtual std::optional<TDuration> GetTimeToFirstBatch() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfilingSchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

IProfilingMultiChunkWriterPtr CreateProfilingMultiChunkWriter(
    NTableClient::ISchemalessMultiChunkWriterPtr underlying,
    TInstant start);

IProfilingSchemalessFormatWriterPtr CreateProfilingSchemalessFormatWriter(
    NFormats::ISchemalessFormatWriterPtr underlying,
    TInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
