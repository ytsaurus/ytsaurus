#pragma once

#include <yt/ytlib/orchid/orchid_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOrchidServiceProxy, OrchidService);

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
