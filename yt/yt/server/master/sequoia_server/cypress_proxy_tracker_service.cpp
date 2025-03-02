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
        ValidatePeer(EPeerKind::Leader);

        auto sequoiaReign = FromProto<ESequoiaReign>(request->sequoia_reign());

        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();

        context->SetRequestInfo("Address: %v, SequoiaReign: %v",
            request->address(),
            sequoiaReign);

        // We could avoid reign validation in automaton thread in most cases. If
        // reign in heartbeat is already persisted in corresponding Cypress
        // proxy object we don't actually need mutation to update its reign: it
        // is enough to check reign here. But to achieve this some way to read
        // persisted Cypress proxy reign from non-automaton thread is needed.
        // Somewhat similar is done in exec node tracker service.
        // TODO(kvk1920): do it.

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
