#include "config.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void TBlobTableWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_part_size", &TThis::MaxPartSize)
        .Default(4_MB)
        .GreaterThanOrEqual(1_MB)
        .LessThanOrEqual(MaxRowWeightLimit);
}

////////////////////////////////////////////////////////////////////////////////

void TBufferedTableWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retry_backoff_time", &TThis::RetryBackoffTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("row_buffer_chunk_size", &TThis::RowBufferChunkSize)
        .Default(64_KB);
}

////////////////////////////////////////////////////////////////////////////////

void TTableColumnarStatisticsCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .Default(100'000);
    registrar.Parameter("max_chunks_per_locate_request", &TThis::MaxChunksPerLocateRequest)
        .Default(10'000);
    registrar.Parameter("fetcher", &TThis::Fetcher)
        .DefaultNew();
    registrar.Parameter("columnar_statistics_fetcher_mode", &TThis::ColumnarStatisticsFetcherMode)
        .Default(EColumnarStatisticsFetcherMode::Fallback);
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkPayloadWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_block_size", &TThis::DesiredBlockSize)
        .GreaterThan(0)
        .Default(16_MBs);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
