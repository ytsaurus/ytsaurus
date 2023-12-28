#include "restart_service.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/admin/restart_service_proxy.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NAdmin {

using namespace NRpc;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TRestartService
    : public TServiceBase
{
public:
    TRestartService(
        TRestartManagerPtr restartManager,
        IInvokerPtr invoker,
        TLogger logger,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            invoker,
            NAdmin::TRestartServiceProxy::GetDescriptor(),
            logger,
            NullRealmId,
            authenticator)
        , RestartManager_(std::move(restartManager))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestRestart));
    }

private:
    const TRestartManagerPtr RestartManager_;

    // Endpoint is necessary for manual configuration regeneration, disk partitioning and node restart.
    // Important part of Hot Swap mechanic.
    DECLARE_RPC_SERVICE_METHOD(NProto, RequestRestart)
    {
        if (RestartManager_) {
            RestartManager_->RequestRestart();
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateRestartService(
    TRestartManagerPtr restartManager,
    IInvokerPtr invoker,
    TLogger logger,
    IAuthenticatorPtr authenticator)
{
    return New<TRestartService>(
        restartManager,
        invoker,
        logger,
        authenticator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
