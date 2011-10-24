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

    RPC_DECLARE_PROXY(ChunkManager,
        ((NoSuchTransaction)(1))
        ((NoSuchHolder)(2))
        ((NoSuchChunk)(3))
    );

    TChunkManagerProxy(NRpc::IChannel::TPtr channel)
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
