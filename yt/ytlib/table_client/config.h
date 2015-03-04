#pragma once

#include "public.h"
#include <ytlib/chunk_client/schema.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/schema.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxRowWeight;

    //! Fraction of rows data size that samples are allowed to occupy.
    double SampleRate;

    //! Fraction of rows data size that chunk index is allowed to occupy.
    double IndexRate;

    double EstimatedCompressionRatio;

    bool AllowDuplicateColumnNames;

    i64 MaxBufferSize;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("max_row_weight", MaxRowWeight)
            .GreaterThan((i64) 0)
            .LessThanOrEqual(MaxRowWeightLimit)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);
        RegisterParameter("index_rate", IndexRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);
        RegisterParameter("estimated_compression_ratio", EstimatedCompressionRatio)
            .GreaterThan(0)
            .LessThan(1)
            .Default(0.2);
        RegisterParameter("allow_duplicate_column_names", AllowDuplicateColumnNames)
            .Default(true);
        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThanOrEqual(1024 * 1024)
            .Default(16 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

////////////////////////////////////////////////////////////////////////////////

class TBufferedTableWriterConfig
    : public TTableWriterConfig
{
public:
    TDuration RetryBackoffTime;
    TDuration FlushPeriod;

    TBufferedTableWriterConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::Seconds(60));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
public:
    TNullable<TKeyColumns> KeyColumns;
    NChunkClient::TChannels Channels;

    TChunkWriterOptions()
    {
        RegisterParameter("key_columns", KeyColumns)
            .Default(Null);
        RegisterParameter("channels", Channels)
            .Default(NChunkClient::TChannels());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTableWriterOptions
    : public NChunkClient::TMultiChunkWriterOptions
    , public TChunkWriterOptions
{
public:
    TTableWriterOptions()
    {
        CompressionCodec = NCompression::ECodec::Lz4;
    }
};

*/

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    bool ReadKey;

    // If set, reader keeps all memory buffers valid until destruction.
    bool KeepBlocks;

    TChunkReaderOptions()
    {
        RegisterParameter("read_key", ReadKey)
            .Default(false);
        RegisterParameter("keep_blocks", KeepBlocks)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

/*
class TTableReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{
public:
    bool SuppressAccessTracking;
    bool EnableTableIndex;

    TTableReaderConfig()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(true);
    }
};
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
