#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
    : public TYsonSerializable
{
    i64 BlockSize;

    //! Fraction of rows data size samples are allowed to occupy.
    double SampleRate;

    //! Fraction of rows data size chunk index allowed to occupy.
    double IndexRate;

    double EstimatedCompressionRatio;

    NChunkClient::TEncodingWriterConfigPtr EncodingWriter;

    bool AllowDuplicateColumnNames;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        Register("block_size", BlockSize)
            .GreaterThan(1024)
            .Default(16 * 1024 * 1024);
        Register("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThan(0.001)
            .Default(0.0001);
        Register("index_rate", IndexRate)
            .GreaterThan(0)
            .LessThan(0.001)
            .Default(0.0001);
        Register("estimated_compression_ratio", EstimatedCompressionRatio)
            .GreaterThan(0)
            .LessThan(1)
            .Default(0.2);
        Register("encoding_writer", EncodingWriter)
            .DefaultNew();
        EncodingWriter->CodecId = ECodecId::Snappy;
        Register("allow_duplicate_column_names", AllowDuplicateColumnNames)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkSequenceWriterConfig
    : public TYsonSerializable
{
    i64 DesiredChunkSize;
    i64 MaxMetaSize;

    int ReplicationFactor;
    int UploadReplicationFactor;

    TChunkWriterConfigPtr ChunkWriter;
    NChunkClient::TRemoteWriterConfigPtr RemoteWriter;

    TChunkSequenceWriterConfig()
    {
        Register("desired_chunk_size", DesiredChunkSize)
            .GreaterThan(0)
            .Default(1024 * 1024 * 1024);
        Register("max_meta_size", MaxMetaSize)
            .GreaterThan(0)
            .LessThan(64 * 1024 * 1024)
            .Default(30 * 1024 * 1024);
        Register("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(3);
        Register("upload_replication_factor", UploadReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(2);
        Register("chunk_writer", ChunkWriter)
            .DefaultNew();
        Register("remote_writer", RemoteWriter)
            .DefaultNew();
    }

    virtual void DoValidate() const
    {
        if (ReplicationFactor < UploadReplicationFactor) {
            ythrow yexception() << "\"replication_factor\" cannot be less than \"upload_replication_factor\"";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkSequenceReaderConfig
    : public TYsonSerializable
{
    NChunkClient::TRemoteReaderConfigPtr RemoteReader;
    NChunkClient::TSequentialReaderConfigPtr SequentialReader;
    int PrefetchWindow;

    TChunkSequenceReaderConfig()
    {
        Register("remote_reader", RemoteReader)
            .DefaultNew();
        Register("sequential_reader", SequentialReader)
            .DefaultNew();
        Register("prefetch_window", PrefetchWindow)
            .GreaterThan(0)
            .LessThanOrEqual(1000)
            .Default(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
