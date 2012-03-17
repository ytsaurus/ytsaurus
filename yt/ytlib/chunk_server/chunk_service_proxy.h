#pragma once

#include "common.h"
#include "chunk_service.pb.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "ChunkService";
    }

    DECLARE_ENUM(EErrorCode,
        ((NoSuchTransaction)(1))
        ((NoSuchHolder)(2))
        ((NotEnoughHolders)(3))
        ((InvalidState)(4))
    );

    TChunkServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, RegisterHolder);
    DEFINE_RPC_PROXY_METHOD(NProto, FullHeartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, IncrementalHeartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateChunks);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
