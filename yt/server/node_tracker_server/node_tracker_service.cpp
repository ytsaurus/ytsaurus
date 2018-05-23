#include "node_tracker_service.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/master_hydra_service.h>
#include <yt/server/cell_master/world_initializer.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NHydra;
using namespace NCellMaster;
using namespace NNodeTrackerClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TNodeTrackerService(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::NodeTrackerService,
            NodeTrackerServerLogger)
        , Config_(config)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetMaxQueueSize(10000)
            .SetMaxConcurrency(10000)
            .SetHeavy(true));
    }

private:
    const TNodeTrackerConfigPtr Config_;

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        if (!Bootstrap_->IsPrimaryMaster()) {
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
        const auto& statistics = request->statistics();
        auto leaseTransactionId = FromProto<TTransactionId>(request->lease_transaction_id());

        context->SetRequestInfo("Address: %v, LeaseTransactionId: %v, %v",
            address,
            leaseTransactionId,
            statistics);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(address);
        if (IsObjectAlive(node)) {
            node->ValidateNotBanned();
        }

        nodeTracker->ProcessRegisterNode(address, context);
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

NRpc::IServicePtr CreateNodeTrackerService(
    TNodeTrackerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
{
    return New<TNodeTrackerService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
