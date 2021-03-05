#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/client/chunk_client/config.h>

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

class TTableColumnarStatisticsCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    // Two fields below are for the chunk spec fetcher.
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    NChunkClient::TFetcherConfigPtr Fetcher;

    EColumnarStatisticsFetcherMode ColumnarStatisticsFetcherMode;

    TTableColumnarStatisticsCacheConfig()
    {
        RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
            .Default(100'000);
        RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
            .Default(10'000);
        RegisterParameter("fetcher", Fetcher)
            .DefaultNew();
        RegisterParameter("columnar_statistics_fetcher_mode", ColumnarStatisticsFetcherMode)
            .Default(EColumnarStatisticsFetcherMode::Fallback);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableColumnarStatisticsCacheConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
