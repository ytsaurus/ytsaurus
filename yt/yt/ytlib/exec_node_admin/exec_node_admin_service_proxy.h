#pragma once

#include <yt/yt/ytlib/exec_node_admin/proto/exec_node_admin_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TExecNodeAdminServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TExecNodeAdminServiceProxy, ExecNodeAdminService);

    DEFINE_RPC_PROXY_METHOD(NProto, HealNode);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
