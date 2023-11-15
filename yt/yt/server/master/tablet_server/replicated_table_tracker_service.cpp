#include "replicated_table_tracker_service.h"

#include "private.h"
#include "replicated_table_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/ytlib/replicated_table_tracker_client/replicated_table_tracker_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NReplicatedTableTrackerClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerService
    : public TServiceBase
{
public:
    TReplicatedTableTrackerService(TBootstrap* bootstrap, IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TReplicatedTableTrackerServiceProxy::GetDescriptor(),
            TabletServerLogger,
            bootstrap->GetMulticellManager()->GetCellId(),
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTrackerStateUpdates));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyChangeReplicaModeCommands));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ComputeReplicaLagTimes));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NReplicatedTableTrackerClient::NProto, GetTrackerStateUpdates)
    {
        Bootstrap_->GetHydraFacade()->GetHydraManager()->ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo("Revision: %v, SnapshotRequested: %v",
            request->revision(),
            request->snapshot_requested());

        Bootstrap_->GetReplicatedTableTrackerStateProvider()->DrainUpdateQueue(
            response,
            request->revision(),
            request->snapshot_requested());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NReplicatedTableTrackerClient::NProto, ApplyChangeReplicaModeCommands)
    {
        Bootstrap_->GetHydraFacade()->GetHydraManager()->ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo("ChangeReplicaModeCommandCount: %v",
            request->commands_size());

        auto commands = FromProto<std::vector<TChangeReplicaModeCommand>>(request->commands());

        auto results = WaitFor(Bootstrap_->GetReplicatedTableTrackerStateProvider()->ApplyChangeReplicaModeCommands(
            std::move(commands)))
            .ValueOrThrow();

        for (const auto& result : results) {
            ToProto(response->add_inner_errors(), result);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NReplicatedTableTrackerClient::NProto, ComputeReplicaLagTimes)
    {
        Bootstrap_->GetHydraFacade()->GetHydraManager()->ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo("ReplicaCount: %v",
            request->replica_ids_size());

        auto replicaIds = FromProto<std::vector<TTableReplicaId>>(request->replica_ids());

        auto replicaLagTimes = WaitFor(
            Bootstrap_->GetReplicatedTableTrackerStateProvider()->ComputeReplicaLagTimes(std::move(replicaIds)))
            .ValueOrThrow();

        for (const auto& replicaLagTime : replicaLagTimes) {
            ToProto(response->add_replica_ids(), replicaLagTime.first);
            YT_VERIFY(replicaLagTime.second);
            response->add_lag_times(replicaLagTime.second->GetValue());
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateReplicatedTableTrackerService(TBootstrap* bootstrap, IInvokerPtr invoker)
{
    return New<TReplicatedTableTrackerService>(bootstrap, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
