#pragma once

#include "public.h"

#include <ytlib/rpc/client.h>

#include <ytlib/chunk_client/data_node_service.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "ChunkHolder";
    }

    DECLARE_ENUM(EErrorCode,
        ((PutBlocksFailed)(101))
        ((NoSuchSession)(102))
        ((SessionAlreadyExists)(103))
        ((ChunkAlreadyExists)(104))
        ((WindowError)(105))
        ((BlockContentMismatch)(106))
        ((NoSuchBlock)(107))
        ((NoSuchChunk)(108))
        ((ChunkPrecachingFailed)(109))
        ((OutOfSpace)(110))
    );

    explicit TDataNodeServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, StartChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, FinishChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, PutBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, SendBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, FlushBlock);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkMeta);
    DEFINE_RPC_PROXY_METHOD(NProto, PrecacheChunk);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, UpdatePeer);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTableSamples);
    DEFINE_RPC_PROXY_METHOD(NChunkClient::NProto, GetChunkSplits);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
