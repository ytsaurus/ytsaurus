#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/compression/public.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileWriterConfig
    : public NChunkClient::TReplicationWriterConfig
{
public:
    i64 BlockSize;
    NCompression::ECodec Codec;

    int ReplicationFactor;
    int UploadReplicationFactor;

    bool ChunkMovable;
    bool ChunkVital;

    TFileWriterConfig()
    {
        RegisterParameter("block_size", BlockSize)
            .Default(1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("compression_codec", Codec)
            .Default(NCompression::ECodec::None);
        RegisterParameter("replication_factor", ReplicationFactor)
            .Default(3)
            .GreaterThanOrEqual(1);
        RegisterParameter("upload_replication_factor", UploadReplicationFactor)
            .Default(2)
            .GreaterThanOrEqual(1);
        RegisterParameter("chunk_movable", ChunkMovable)
            .Default(true);
        RegisterParameter("chunk_vital", ChunkVital)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public NChunkClient::TSequentialReaderConfig
    , public NChunkClient::TRemoteReaderConfig
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
