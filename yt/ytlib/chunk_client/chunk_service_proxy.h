#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/chunk_client/chunk_service.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "ChunkService";
    }

    static int GetProtocolVersion()
    {
        return 1;
    }

    explicit TChunkServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, LocateChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, AllocateWriteTargets);
    DEFINE_RPC_PROXY_METHOD(NProto, ExportChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, ImportChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkOwningNodes);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
