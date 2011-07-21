#pragma once

#include "common.h"
#include "chunk_manager_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TChunkManagerProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode,
        ((NoSuchTransaction)(1))
        ((NoSuchHolder)(2))
    );

    static Stroka GetServiceName()
    {
        return "ChunkManager";
    }

    TChunkManagerProxy(NRpc::TChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, RegisterHolder);
    RPC_PROXY_METHOD(NProto, HolderHeartbeat);
    RPC_PROXY_METHOD(NProto, AddChunk);
    RPC_PROXY_METHOD(NProto, FindChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
