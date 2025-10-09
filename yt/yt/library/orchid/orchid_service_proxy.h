#pragma once

#include <yt/yt/library/orchid/proto/orchid_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOrchidServiceProxy, OrchidService);

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
