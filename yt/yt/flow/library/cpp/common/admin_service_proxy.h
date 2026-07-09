#pragma once

#include <yt/yt/flow/library/cpp/common/proto/admin_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TAdminServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TAdminServiceProxy, AdminService);

    DEFINE_RPC_PROXY_METHOD(NProto, Die);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
