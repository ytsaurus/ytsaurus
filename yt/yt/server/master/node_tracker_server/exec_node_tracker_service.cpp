#include "exec_node_tracker_service.h"

#include "private.h"
#include "exec_node_tracker.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/persistent_state_transient_cache.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NExecNodeTrackerClient;
using namespace NHydra;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTrackerService
    : public NRpc::TServiceBase
{
public:
    explicit TExecNodeTrackerService(TBootstrap* bootstrap)
        : TServiceBase(
            TDispatcher::Get()->GetHeavyInvoker(),
            TExecNodeTrackerServiceProxy::GetDescriptor(),
            NodeTrackerServerLogger(),
            NRpc::TServiceOptions{
                .RealmId = bootstrap->GetCellId(),
                .Authenticator =bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NExecNodeTrackerClient::NProto, Heartbeat)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& transientCache = Bootstrap_->GetPersistentStateTransientCache();
        transientCache->ValidateWorldInitialized();

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->ValidatePeer(EPeerKind::Leader);

        auto nodeId = FromProto<TNodeId>(request->node_id());

        context->SetRequestInfo("NodeId: %v, Address: %v",
            nodeId,
            transientCache->GetNodeDefaultAddress(nodeId));

        const auto& execNodeTracker = Bootstrap_->GetExecNodeTracker();
        execNodeTracker->ProcessHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateExecNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TExecNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
