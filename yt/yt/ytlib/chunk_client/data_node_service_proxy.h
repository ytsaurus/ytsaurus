#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Proxy to both DataNodeService and OffshoreNodeService; it was decided to include one into
//! the other as OffshoreNodeService currently implements a subset of DataNodeService's
//! API methods and even uses identical signatures. This allows us to work with them
//! in a more convenient and clean way. For instance, the code does not need to think
//! where a given chunk/meta is read from, it's handled on the channel level.
class TDataNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDataNodeServiceProxy, DataNodeService,
        .SetProtocolVersion(6)
        .SetFeaturesType<EChunkClientFeature>());

    DEFINE_RPC_PROXY_METHOD(NProto, StartChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, FinishChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, CancelChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, PutBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, SendBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateP2PBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, FlushBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, ProbeChunkSet);
    DEFINE_RPC_PROXY_METHOD(NProto, ProbeBlockSet);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockSet);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockRange);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkFragmentSet);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupRows);
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkMeta);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkSliceDataWeights);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdatePeer);
    DEFINE_RPC_PROXY_METHOD(NProto, GetTableSamples);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkSlices);
    DEFINE_RPC_PROXY_METHOD(NProto, GetColumnarStatistics);
    DEFINE_RPC_PROXY_METHOD(NProto, DisableChunkLocations);
    DEFINE_RPC_PROXY_METHOD(NProto, DestroyChunkLocations);
    DEFINE_RPC_PROXY_METHOD(NProto, ResurrectChunkLocations);
    DEFINE_RPC_PROXY_METHOD(NProto, AnnounceChunkReplicas);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
