#include "node_tracker_service.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectServer;
using namespace NSecurityServer;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::NodeTrackerService,
            NodeTrackerServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            THROW_ERROR_EXCEPTION("Cannot register nodes at secondary master");
        }

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (worldInitializer->HasProvisionLock()) {
            THROW_ERROR_EXCEPTION(
                "Provision lock is found, which indicates a fresh instance of masters being run. "
                "If this is not intended then please check snapshot/changelog directories location. "
                "Ignoring this warning and removing the lock may cause UNRECOVERABLE DATA LOSS! "
                "If you are sure and wish to continue then run 'yt remove //sys/@provision_lock'");
        }

        auto nodeAddresses = FromProto<TNodeAddressMap>(request->node_addresses());
        const auto& addresses = GetAddressesOrThrow(nodeAddresses, EAddressType::InternalRpc);
        const auto& address = GetDefaultAddress(addresses);
        auto leaseTransactionId = FromProto<TTransactionId>(request->lease_transaction_id());

        context->SetRequestInfo("Address: %v, LeaseTransactionId: %v",
            address,
            leaseTransactionId);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(address);
        if (IsObjectAlive(node)) {
            node->ValidateNotBanned();
        }

        nodeTracker->ProcessRegisterNode(address, context);
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, Heartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetDefaultAddress(),
            statistics);

        nodeTracker->ProcessHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
