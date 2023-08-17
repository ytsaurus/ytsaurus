#pragma once

#include <yt/yt/ytlib/admin/proto/reboot_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

class TRebootServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TRebootServiceProxy, RebootService);

    DEFINE_RPC_PROXY_METHOD(NProto, RequestReboot);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
