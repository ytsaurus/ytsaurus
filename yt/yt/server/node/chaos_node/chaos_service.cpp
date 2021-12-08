#include "chaos_service.h"

#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "replication_card.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_service.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosNode {

using namespace NRpc;
using namespace NChaosClient;
using namespace NHydra;
using namespace NClusterNode;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosService
    : public THydraServiceBase
{
public:
    explicit TChaosService(IChaosSlotPtr slot)
        : THydraServiceBase(
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Default),
            TChaosServiceProxy::GetDescriptor(),
            ChaosNodeLogger,
            slot->GetCellId())
        , Slot_(std::move(slot))
    {
        YT_VERIFY(Slot_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateReplicationProgress));
    }

private:
    const IChaosSlotPtr Slot_;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateReplicationCard)
    {
        context->SetRequestInfo();

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateReplicationCard(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCard)
    {
        SyncWithUpstream();

        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        bool includeCoordinators = request->request_coordinators();
        bool includeProgress = request->request_replication_progress();
        bool includeHistory = request->request_history();

        context->SetRequestInfo("ReplicationCardToken: %v, IncludeCoordinators: %v, IncludeProgress: %v, IncludeHistory: %v",
            replicationCardToken,
            includeCoordinators,
            includeProgress,
            includeHistory);

        const auto& chaosManager = Slot_->GetChaosManager();
        auto replicationCard = chaosManager->GetReplicationCard(replicationCardToken.ReplicationCardId);
        auto* protoReplicationCard = response->mutable_replication_card();
        protoReplicationCard->set_era(replicationCard->GetEra());

        std::vector<TCellId> coordinators;
        if (includeCoordinators) {
            for (const auto& [cellId, info] : replicationCard->Coordinators()) {
                if (!chaosManager->IsCoordinatorSuspended(cellId)) {
                    coordinators.push_back(cellId);
                }
            }
            ToProto(protoReplicationCard->mutable_coordinator_cell_ids(), coordinators);
        }

        for (const auto& replica : replicationCard->Replicas()) {
            auto *protoReplica = protoReplicationCard->add_replicas();
            ToProto(protoReplica, replica, includeProgress, includeHistory);
        }

        context->SetResponseInfo("ReplicationCardToken: %v, CoordinatorCellIds: %v, ReplicationCard: %v",
            replicationCardToken,
            coordinators,
            *replicationCard);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateTableReplica)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        auto replica = FromProto<TReplicaInfo>(request->replica_info());

        context->SetRequestInfo("ReplicationCardToken: %v, Replica: %v",
            replicationCardToken,
            replica);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, RemoveTableReplica)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        auto replicaId = FromProto<TReplicaId>(request->replica_id());

        context->SetRequestInfo("ReplicationCardToken: %v, ReplicaId: %v",
            replicationCardToken,
            replicaId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->RemoveTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, AlterTableReplica)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto mode = request->has_mode()
            ? std::make_optional(FromProto<EReplicaMode>(request->mode()))
            : std::nullopt;
        auto state = request->has_state()
            ? std::make_optional(FromProto<EReplicaState>(request->state()))
            : std::nullopt;

        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qv", *mode);
        }

        if (state && !IsStableReplicaState(*state)) {
            THROW_ERROR_EXCEPTION("Invalid replica state %Qv", *state);
        }

        context->SetRequestInfo("ReplicationCardToken: %v, ReplicaId: %v, Mode: %v, State: %v",
            replicationCardToken,
            replicaId,
            mode,
            state);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->AlterTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateReplicationProgress)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto progress = FromProto<TReplicationProgress>(request->replication_progress());

        context->SetRequestInfo("ReplicationCardToken: %v, ReplicaId: %v, ReplicationProgress: %v",
            replicationCardToken,
            replicaId,
            progress);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->UpdateReplicationProgress(std::move(context));
    }

    // THydraServiceBase overrides.
    IHydraManagerPtr GetHydraManager() override
    {
        return Slot_->GetHydraManager();
    }
};

IServicePtr CreateChaosService(IChaosSlotPtr slot)
{
    return New<TChaosService>(std::move(slot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
