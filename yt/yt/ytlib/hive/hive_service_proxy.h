#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class THiveServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(THiveServiceProxy, HiveService,
        .SetProtocolVersion(1)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, Ping,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, SyncCells);
    DEFINE_RPC_PROXY_METHOD(NProto, PostMessages);
    DEFINE_RPC_PROXY_METHOD(NProto, SendMessages);
    DEFINE_RPC_PROXY_METHOD(NProto, SyncWithOthers);
    DEFINE_RPC_PROXY_METHOD(NProto, GetConsistentState);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
