#pragma once

#include "public.h"

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

        CodecId = ECodecId::Lz4;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TRemoteWriterConfig
{
    i64 DesiredChunkSize;
    i64 MaxMetaSize;

    int ReplicationFactor;
    int UploadReplicationFactor;

    bool ChunksMovable;
    bool PreferLocalHost;

    TTableWriterConfig()
    {
        Register("desired_chunk_size", DesiredChunkSize)
            .GreaterThan(0)
            .Default(1024 * 1024 * 1024);
        Register("max_meta_size", MaxMetaSize)
            .GreaterThan(0)
            .LessThanOrEqual(64 * 1024 * 1024)
            .Default(30 * 1024 * 1024);
        Register("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(3);
        Register("upload_replication_factor", UploadReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(2);
        Register("chunks_movable", ChunksMovable)
            .Default(true);
        Register("prefer_local_host", PreferLocalHost)
            .Default(true);
    }

    virtual void DoValidate() const
    {
        if (ReplicationFactor < UploadReplicationFactor) {
            THROW_ERROR_EXCEPTION("\"replication_factor\" cannot be less than \"upload_replication_factor\"");
        }
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
