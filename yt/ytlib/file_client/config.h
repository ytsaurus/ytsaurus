#pragma once

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/misc/codec.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileWriterConfig
    : public TConfigurable
{
    i64 BlockSize;
    ECodecId CodecId;
    int TotalReplicaCount;
    int UploadReplicaCount;
    NChunkClient::TRemoteWriterConfigPtr RemoteWriter;

    TFileWriterConfig()
    {
        Register("block_size", BlockSize)
            .Default(1024 * 1024)
            .GreaterThan(0);
        Register("codec_id", CodecId)
            .Default(ECodecId::None);
        Register("total_replica_count", TotalReplicaCount)
            .Default(3)
            .GreaterThanOrEqual(1);
        Register("upload_replica_count", UploadReplicaCount)
            .Default(2)
            .GreaterThanOrEqual(1);
        Register("remote_writer", RemoteWriter)
            .DefaultNew();
    }

    virtual void DoValidate()
    {
        if (TotalReplicaCount < UploadReplicaCount) {
            ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderConfig
    : public TConfigurable
{
    NChunkClient::TSequentialReaderConfigPtr SequentialReader;
    NChunkClient::TRemoteReaderConfigPtr RemoteReader;

    TFileReaderConfig()
    {
        Register("sequential_reader", SequentialReader)
            .DefaultNew();
        Register("remote_reader", RemoteReader)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
