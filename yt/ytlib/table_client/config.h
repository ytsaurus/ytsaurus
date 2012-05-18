#pragma once

#include "public.h"
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>

#include <ytlib/misc/codec.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableConsumerConfig
    : public TConfigurable
{
    i64 MaxColumnNameSize;
    i64 MaxRowSize;
    i64 MaxKeySize;

    TTableConsumerConfig() 
    {
        Register("max_column_name_size", MaxColumnNameSize)
            .LessThanOrEqual(256)
            .Default(256);
        Register("max_row_size", MaxRowSize)
            .LessThanOrEqual(16 * 1024 * 1024)
            .Default(16 * 1024 * 1024);
        Register("max_key_size", MaxKeySize)
            .LessThanOrEqual(4 * 1024)
            .Default(4 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
    : public TConfigurable
{
    i64 BlockSize;
    ECodecId CodecId;

    //! Fraction of rows data size samples are allowed to occupy.
    double SampleRate;

    //! Fraction of rows data size samples are allowed to occupy.
    double IndexRate;

    double EstimatedCompressionRatio;

    TChunkWriterConfig()
    {
        // Block less than 1Kb is nonsense.
        Register("block_size", BlockSize)
            .GreaterThan(1024)
            .Default(1024 * 1024);
        Register("codec_id", CodecId)
            .Default(ECodecId::Snappy);
        Register("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThan(0.001)
            .Default(0.0001);
        Register("index_rate", IndexRate)
            .GreaterThan(0)
            .LessThan(0.1)
            .Default(0.01);
        Register("estimated_compression_ratio", EstimatedCompressionRatio)
            .GreaterThan(0)
            .LessThan(1)
            .Default(0.2);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkSequenceWriterConfig
    : public TConfigurable
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
            ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkSequenceReaderConfig
    : public TConfigurable
{
    NChunkClient::TRemoteReaderConfigPtr RemoteReader;
    NChunkClient::TSequentialReaderConfigPtr SequentialReader;

    TChunkSequenceReaderConfig()
    {
        Register("remote_reader", RemoteReader).DefaultNew();
        Register("sequential_reader", SequentialReader).DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
