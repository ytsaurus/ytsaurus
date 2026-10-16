#pragma once

#include <yt/yt/ytlib/shuffle_client/proto/shuffle_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NShuffleClient {

////////////////////////////////////////////////////////////////////////////////

class TShuffleServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TShuffleServiceProxy, ShuffleService);

    DEFINE_RPC_PROXY_METHOD(NProto, StartShuffle);
    DEFINE_RPC_PROXY_METHOD(NProto, RegisterChunks);
    DEFINE_RPC_PROXY_METHOD(NProto, FetchChunks);
    // COMPAT(apollo1321): Remove RegisterMapper after the 26.2 branch is created.
    DEFINE_RPC_PROXY_METHOD_GENERIC(
        RegisterMapper,
        NProto::TReqRegisterWriter,
        NProto::TRspRegisterWriter);
    DEFINE_RPC_PROXY_METHOD(NProto, RegisterWriter);
    DEFINE_RPC_PROXY_METHOD(NProto, GetPartitionWriteSession);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleClient
