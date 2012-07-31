#pragma once

#include "public.h"

//#include <ytlib/chunk_client/public.h>
//#include <ytlib/chunk_client/config.h>
#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/misc/codec.h>
#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileWriterConfig
    : public NChunkClient::TRemoteWriterConfig
{
    i64 BlockSize;
    ECodecId CodecId;
    int ReplicationFactor;
    int UploadReplicationFactor;
    bool ChunkMovable;

    TFileWriterConfig()
    {
        Register("block_size", BlockSize)
            .Default(1024 * 1024)
            .GreaterThan(0);
        Register("codec_id", CodecId)
            .Default(ECodecId::None);
        Register("replication_factor", ReplicationFactor)
            .Default(3)
            .GreaterThanOrEqual(1);
        Register("upload_replication_factor", UploadReplicationFactor)
            .Default(2)
            .GreaterThanOrEqual(1);
        Register("chunk_movable", ChunkMovable)
            .Default(true);
    }

    virtual void DoValidate()
    {
        if (ReplicationFactor < UploadReplicationFactor) {
            ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderConfig
    : public NChunkClient::TSequentialReaderConfig
    , public NChunkClient::TRemoteReaderConfig
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
