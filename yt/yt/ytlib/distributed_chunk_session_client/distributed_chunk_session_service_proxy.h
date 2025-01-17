#pragma once

#include <yt/yt/ytlib/distributed_chunk_session_client/proto/distributed_chunk_session_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDistributedChunkSessionServiceProxy, DistributedChunkSessionService);

    DEFINE_RPC_PROXY_METHOD(NProto, StartSession);
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession);
    DEFINE_RPC_PROXY_METHOD(NProto, SendBlocks);
    DEFINE_RPC_PROXY_METHOD(NProto, FinishSession);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
