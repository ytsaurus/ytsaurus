#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): With how our RPC service definitions work, we can either reuse
// the exact proto definitions from DataNodeService, or isolate ourselves into a different
// proto namespace and defining similar messages. It depends on what we want to do with
// client-points in replication reader — do we want to fill protos using exactly the same
// code? or will we only fill the fields necessary for each service?
// The latter is probably more reasonable, but we need to think about how to share common
// parts (or just copy them) and write filler-code for each service.
//
// Let's start with using the same protos, for the very beginning, and later we can decide
// whether we want to share them or not.

class TOffshoreNodeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOffshoreNodeServiceProxy, OffshoreNodeService,
        .SetProtocolVersion(0)
        .SetFeaturesType<EChunkClientFeature>());
        
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockSet);
    DEFINE_RPC_PROXY_METHOD(NProto, GetBlockRange);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkMeta);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
