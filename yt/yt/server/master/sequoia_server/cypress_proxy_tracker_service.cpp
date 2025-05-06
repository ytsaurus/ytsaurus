#include "cypress_proxy_tracker_service.h"

#include "private.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/lib/sequoia/cypress_proxy_tracker_service_proxy.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NRpc;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TCypressProxyTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCypressProxyTrackerServiceProxy::GetDescriptor(),
            TMasterHydraServiceBase::TRpcHeavyDefaultInvoker{},
            SequoiaServerLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        ValidateClusterInitialized();

        // NB: leader is not required here since in most cases Cypress proxy is
        // already registered and mutation is not needed.

        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();

        context->SetRequestInfo("Address: %v, SequoiaReign: %v",
            request->address(),
            static_cast<ESequoiaReign>(request->sequoia_reign()));

        // NB: currently, heartbeat is not used to deliver dynamic config to
        // Cypress proxy to allow adding new Cypress proxies during master's
        // read-only. It can be done via heartbeats instead, but with special
        // handling in read-only mode.
        // TODO(kvk1920): think about it again and probably do it.
        if (!cypressProxyTracker->TryProcessCypressProxyHeartbeatWithoutMutation(context)) {
            ValidatePeer(EPeerKind::Leader);
            cypressProxyTracker->ProcessCypressProxyHeartbeat(context);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCypressProxyTrackerService(TBootstrap* bootstrap)
{
    return New<TCypressProxyTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
