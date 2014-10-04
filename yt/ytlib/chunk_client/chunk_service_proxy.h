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

    explicit TChunkServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, LocateChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, AllocateWriteTargets);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
