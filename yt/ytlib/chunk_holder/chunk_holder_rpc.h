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

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode, 
        ((RemoteCallFailed)(1))
        (NoSuchSession)
        (SessionAlreadyExists)
        (ChunkAlreadyExists)
        (WindowError)
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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
