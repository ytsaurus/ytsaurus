#pragma once

#include <yt/yt/ytlib/lease_client/proto/lease_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NLeaseClient {

////////////////////////////////////////////////////////////////////////////////

class TLeaseServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TLeaseServiceProxy, LeaseService);

    DEFINE_RPC_PROXY_METHOD(NProto, IssueLease);
    DEFINE_RPC_PROXY_METHOD(NProto, RevokeLease);
    DEFINE_RPC_PROXY_METHOD(NProto, ReferenceLease);
    DEFINE_RPC_PROXY_METHOD(NProto, UnreferenceLease);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseClient
