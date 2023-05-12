#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TChunkServiceProxy, ChunkService,
        .SetProtocolVersion(6)
        .SetAcceptsBaggage(false)
        .SetFeaturesType<NObjectClient::EMasterFeature>());

    DEFINE_RPC_PROXY_METHOD(NProto, LocateChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, LocateDynamicStores);
    DEFINE_RPC_PROXY_METHOD(NProto, TouchChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, AllocateWriteTargets);
    DEFINE_RPC_PROXY_METHOD(NProto, ExportChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, ImportChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChunkOwningNodes);
    // COMPAT(aleksandra-zh): ExecuteBatch will probably be deleted in the future.
    DEFINE_RPC_PROXY_METHOD(NProto, ExecuteBatch);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, ConfirmChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, SealChunk);
    DEFINE_RPC_PROXY_METHOD(NProto, CreateChunkLists);
    DEFINE_RPC_PROXY_METHOD(NProto, UnstageChunkTree);
    DEFINE_RPC_PROXY_METHOD(NProto, AttachChunkTrees);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
