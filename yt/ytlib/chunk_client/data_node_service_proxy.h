#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/chunk_client/data_node_service.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "DataNode";
    }

    explicit TDataNodeServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, StartChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, FinishChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, CancelChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, PutBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, SendBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, FlushBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockSet);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockRange);
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkMeta);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, UpdatePeer);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTableSamples);
    DEFINE_RPC_PROXY_METHOD(NChunkClient::NProto, GetChunkSlices);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
