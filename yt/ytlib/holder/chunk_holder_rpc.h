#pragma once

#include "../rpc/service.h"
#include "../rpc/client.h"
#include "chunk_holder.pb.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TChunkHolderProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode, 
        ((RemoteCallFailed)(1))
    );

    static Stroka GetServiceName() 
    {
        return "ChunkHolder";
    }

    TChunkHolderProxy(NRpc::TChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NRpcChunkHolder, StartChunk);
    RPC_PROXY_METHOD(NRpcChunkHolder, FinishChunk);
    RPC_PROXY_METHOD(NRpcChunkHolder, PutBlocks);
    RPC_PROXY_METHOD(NRpcChunkHolder, SendBlocks);
    RPC_PROXY_METHOD(NRpcChunkHolder, GetBlocks);
};

////////////////////////////////////////////////////////////////////////////////

}
