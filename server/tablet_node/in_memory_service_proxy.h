#pragma once

#include "public.h"

#include <yt/server/tablet_node/in_memory_service.pb.h>

namespace NYT {
namespace NTabletNode {

using TInMemorySessionId = TGuid;

////////////////////////////////////////////////////////////////////////////////

class TInMemoryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TInMemoryServiceProxy, InMemoryService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, StartSession,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, PingSession,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, FinishSession,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy));
    DEFINE_RPC_PROXY_METHOD(NProto, PutBlocks,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
