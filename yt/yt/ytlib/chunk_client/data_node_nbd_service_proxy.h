#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_nbd_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeNbdServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDataNodeNbdServiceProxy, DataNodeNbdService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NNbd::NProto, OpenSession);
    DEFINE_RPC_PROXY_METHOD(NNbd::NProto, CloseSession);
    DEFINE_RPC_PROXY_METHOD(NNbd::NProto, Read);
    DEFINE_RPC_PROXY_METHOD(NNbd::NProto, Write);
    DEFINE_RPC_PROXY_METHOD(NNbd::NProto, KeepSessionAlive);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
