#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_node_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDataNodeServiceProxy, DataNodeService,
        .SetProtocolVersion(6));

    DEFINE_RPC_PROXY_METHOD(NProto, StartChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, FinishChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, CancelChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, PutBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, SendBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, PopulateCache);
    DEFINE_RPC_PROXY_METHOD(NProto, FlushBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockSet);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockRange);
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkMeta);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdatePeer);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTableSamples);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkSlices);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
