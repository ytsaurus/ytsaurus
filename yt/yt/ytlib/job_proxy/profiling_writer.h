#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/library/formats/format.h>

#include <library/cpp/yt/cpu_clock/public.h>

namespace NYT::NJobProxy {

struct TWriterTimingStatistics
{
    TDuration WriteTime = TDuration::Zero();
    TDuration WaitTime = TDuration::Zero();
    TDuration IdleTime = TDuration::Zero();
    TDuration CloseTime = TDuration::Zero();
};

////////////////////////////////////////////////////////////////////////////////

struct IProfilingMultiChunkWriter
    : public NTableClient::ISchemalessMultiChunkWriter
{
    virtual std::optional<TDuration> GetTimeToFirstBatch() const = 0;
    virtual TWriterTimingStatistics GetTimingStatistics() const = 0;
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
    TCpuInstant start);

IProfilingSchemalessFormatWriterPtr CreateProfilingSchemalessFormatWriter(
    NFormats::ISchemalessFormatWriterPtr underlying,
    TCpuInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
