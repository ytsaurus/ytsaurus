#pragma once

#include "common.h"

#include <ytlib/rpc/client.h>
#include <ytlib/chunk_server/chunk_service.pb.h>

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
        ((NotAuthorized)(5))
    );

    TChunkServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, RegisterNode);
    DEFINE_RPC_PROXY_METHOD(NProto, FullHeartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, IncrementalHeartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
