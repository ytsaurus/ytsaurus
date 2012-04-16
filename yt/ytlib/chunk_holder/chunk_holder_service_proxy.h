#pragma once

#include "public.h"
#include <ytlib/chunk_holder/chunk_holder_service.pb.h>

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "ChunkHolder";
    }

    DECLARE_ENUM(EErrorCode,
        ((PutBlocksFailed)(1))
        ((NoSuchSession)(2))
        ((SessionAlreadyExists)(3))
        ((ChunkAlreadyExists)(4))
        ((WindowError)(5))
        ((BlockContentMismatch)(6))
        ((NoSuchBlock)(7))
        ((NoSuchChunk)(8))
        ((ChunkPrecachingFailed)(9))
        ((OutOfSpace)(10))
    );

    TChunkHolderServiceProxy(NRpc::IChannel* channel)
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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
