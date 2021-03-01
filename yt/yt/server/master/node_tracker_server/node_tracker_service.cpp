#include "node_tracker_service.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/master_hydra_service.h>
#include <yt/server/master/cell_master/world_initializer.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

namespace NYT::NNodeTrackerServer {

using namespace NHydra;
using namespace NCellMaster;
using namespace NNodeTrackerClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NChunkClient::NProto;

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));

        // Legacy heartbeats.
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetQueueSizeLimit(10000)
            .SetConcurrencyLimit(10000)
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

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, FullHeartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetDefaultAddress(),
            statistics);

        nodeTracker->ProcessFullHeartbeat(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, IncrementalHeartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetDefaultAddress(),
            statistics);

        nodeTracker->ProcessIncrementalHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
