#pragma once

#include "chunk_holder_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TChunkHolderServiceProxy> TPtr;

    static Stroka GetServiceName()
    {
        return "ChunkHolder";
    }

    DECLARE_ENUM(EErrorCode,
        ((RemoteCallFailed)(1))
        ((NoSuchSession)(2))
        ((SessionAlreadyExists)(3))
        ((ChunkAlreadyExists)(4))
        ((WindowError)(5))
        ((UnmatchedBlockContent)(6))
        ((NoSuchBlock)(7))
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
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, PrecacheChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
