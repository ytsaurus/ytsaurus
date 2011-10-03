#pragma once

#include "chunk_holder.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TChunkHolderProxy> TPtr;

    DECLARE_POLY_ENUM2(EErrorCode, NRpc::EErrorCode,
        ((RemoteCallFailed)(1))
        ((NoSuchSession)(2))
        ((SessionAlreadyExists)(3))
        ((ChunkAlreadyExists)(4))
        ((WindowError)(5))
        ((UnmatchedBlockContent)(6))
    );

    static Stroka GetServiceName() 
    {
        return "ChunkHolder";
    }

    TChunkHolderProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, StartChunk);
    RPC_PROXY_METHOD(NProto, FinishChunk);
    RPC_PROXY_METHOD(NProto, PutBlocks);
    RPC_PROXY_METHOD(NProto, SendBlocks);
    RPC_PROXY_METHOD(NProto, FlushBlock);
    RPC_PROXY_METHOD(NProto, GetBlocks);
    RPC_PROXY_METHOD(NProto, PingSession);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
