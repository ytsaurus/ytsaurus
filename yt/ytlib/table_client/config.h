#pragma once

#include "public.h"
#include "schema.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
    i64 BlockSize;

    //! Fraction of rows data size samples are allowed to occupy.
    double SampleRate;

    //! Fraction of rows data size chunk index allowed to occupy.
    double IndexRate;

    double EstimatedCompressionRatio;

    bool AllowDuplicateColumnNames;

    i64 MaxBufferSize;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        Register("block_size", BlockSize)
            .GreaterThanOrEqual(1024 * 1024)
            .Default(16 * 1024 * 1024);
        Register("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);
        Register("index_rate", IndexRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);
        Register("estimated_compression_ratio", EstimatedCompressionRatio)
            .GreaterThan(0)
            .LessThan(1)
            .Default(0.2);
        Register("allow_duplicate_column_names", AllowDuplicateColumnNames)
            .Default(true);
        Register("max_buffer_size", MaxBufferSize)
            .GreaterThanOrEqual(1024 * 1024)
            .Default(32 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
    TNullable<TKeyColumns> KeyColumns;
    TChannels Channels;

    TChunkWriterOptions()
    {
        Register("key_columns", KeyColumns)
            .Default(Null);
        Register("channels", Channels)
            .Default(TChannels());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableWriterOptions
    : public NChunkClient::TMultiChunkWriterOptions
    , public TChunkWriterOptions
{
    TTableWriterOptions()
    {
        Codec = NCompression::ECodec::Lz4;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableReaderConfig
    : public NChunkClient::TRemoteReaderConfig
    , public NChunkClient::TSequentialReaderConfig
{
    int PrefetchWindow;

    TTableReaderConfig()
    {
        Register("prefetch_window", PrefetchWindow)
            .GreaterThan(0)
            .LessThanOrEqual(1000)
            .Default(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
