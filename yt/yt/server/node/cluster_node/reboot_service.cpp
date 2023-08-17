#include "reboot_service.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/admin/reboot_service_proxy.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NClusterNode {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRebootService
    : public TServiceBase
{
public:
    explicit TRebootService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            NAdmin::TRebootServiceProxy::GetDescriptor(),
            ClusterNodeLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestReboot));
    }

private:
    IBootstrap* const Bootstrap_;

    // Endpoint is necessary for manual configuration regeneration, disk partitioning and node restart.
    // Important part of Hot Swap mechanic.
    DECLARE_RPC_SERVICE_METHOD(NAdmin::NProto, RequestReboot)
    {
        auto manager = Bootstrap_->GetRebootManager();

        if (manager) {
            manager->RequestReboot();
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateRebootService(IBootstrap* bootstrap)
{
    return New<TRebootService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
