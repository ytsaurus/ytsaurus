#include "cypress_proxy_tracker_service.h"

#include "private.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/lib/sequoia/cypress_proxy_tracker_service_proxy.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/ytlib/sequoia_client/sequoia_reign.h>

namespace NYT::NSequoiaServer {

using namespace NConcurrency;
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

        // NB: static_cast is intented. Cypress proxy reign is used for two
        // purposes only: to compare with master reign and to store as
        // registered Cypress proxy info. Both of use cases works fine with
        // unknown values.
        auto sequoiaReign = static_cast<ESequoiaReign>(request->sequoia_reign());
        context->SetRequestInfo("Address: %v, SequoiaReign: %v",
            request->address(),
            sequoiaReign);

        if (!Bootstrap_->IsPrimaryMaster()) {
            YT_LOG_ALERT(
                "Attempt to register Cypress proxy at secondary master "
                "(Address: %v, SequoiaReign: %v)",
                request->address(),
                sequoiaReign);
            context->Reply(TError("Cypress proxy cannot be registered at secondary master"));
            return;
        }

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        WaitFor(hydraManager->SyncWithLeader())
            .ThrowOnError();

        // NB: currently, heartbeat is not used to deliver dynamic config to
        // Cypress proxy to allow adding new Cypress proxies during master's
        // read-only. It can be done via heartbeats instead, but with special
        // handling in read-only mode.
        // TODO(kvk1920): think about it again and probably do it.
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        cypressProxyTracker->ProcessCypressProxyHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCypressProxyTrackerService(TBootstrap* bootstrap)
{
    return New<TCypressProxyTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
