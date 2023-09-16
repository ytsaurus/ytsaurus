#include "restart_service.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/admin/restart_service_proxy.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NClusterNode {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRestartService
    : public TServiceBase
{
public:
    explicit TRestartService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            NAdmin::TRestartServiceProxy::GetDescriptor(),
            ClusterNodeLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestRestart));
    }

private:
    IBootstrap* const Bootstrap_;

    // Endpoint is necessary for manual configuration regeneration, disk partitioning and node restart.
    // Important part of Hot Swap mechanic.
    DECLARE_RPC_SERVICE_METHOD(NAdmin::NProto, RequestRestart)
    {
        auto manager = Bootstrap_->GetRestartManager();

        if (manager) {
            manager->RequestRestart();
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateRestartService(IBootstrap* bootstrap)
{
    return New<TRestartService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
