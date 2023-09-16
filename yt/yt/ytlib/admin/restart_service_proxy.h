#pragma once

#include <yt/yt/ytlib/admin/proto/restart_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

class TRestartServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TRestartServiceProxy, RestartService);

    DEFINE_RPC_PROXY_METHOD(NProto, RequestRestart);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
