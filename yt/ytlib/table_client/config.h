#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/client/table_client/config.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableWriterOptions
    : public TChunkWriterOptions
    , public NChunkClient::TMultiChunkWriterOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TBlobTableWriterConfig
    : public NTableClient::TTableWriterConfig
{
public:
    i64 MaxPartSize;

    TBlobTableWriterConfig()
    {
        RegisterParameter("max_part_size", MaxPartSize)
            .Default(4 * 1024 * 1024)
            .GreaterThanOrEqual(1 * 1024 * 1024)
            .LessThanOrEqual(MaxRowWeightLimit);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlobTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TBufferedTableWriterConfig
    : public TTableWriterConfig
{
public:
    TDuration RetryBackoffTime;
    TDuration FlushPeriod;
    i64 RowBufferChunkSize;

    TBufferedTableWriterConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::Seconds(60));
        RegisterParameter("row_buffer_chunk_size", RowBufferChunkSize)
            .Default(64 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TBufferedTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderOptions
    : public TChunkReaderOptions
    , public NChunkClient::TMultiChunkReaderOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTableReaderOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
