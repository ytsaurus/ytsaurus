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

        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto fetchOptions = FromProto<TReplicationCardFetchOptions>(request->fetch_options());

        context->SetRequestInfo("ReplicationCardId: %v, FetchOptions: %v",
            replicationCardId,
            fetchOptions);

        const auto& chaosManager = Slot_->GetChaosManager();
        auto* replicationCard = chaosManager->GetReplicationCardOrThrow(replicationCardId);

        auto* protoReplicationCard = response->mutable_replication_card();
        protoReplicationCard->set_era(replicationCard->GetEra());

        std::vector<TCellId> coordinatorCellIds;
        if (fetchOptions.IncludeCoordinators) {
            for (const auto& [cellId, info] : replicationCard->Coordinators()) {
                if (!chaosManager->IsCoordinatorSuspended(cellId)) {
                    coordinatorCellIds.push_back(cellId);
                }
            }
            ToProto(protoReplicationCard->mutable_coordinator_cell_ids(), coordinatorCellIds);
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            auto* protoEntry = protoReplicationCard->add_replicas();
            ToProto(protoEntry->mutable_id(), replicaId);
            ToProto(protoEntry->mutable_info(), replicaInfo, fetchOptions);
        }

        context->SetResponseInfo("ReplicationCardId: %v, CoordinatorCellIds: %v, ReplicationCard: %v",
            replicationCardId,
            coordinatorCellIds,
            *replicationCard);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateTableReplica)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replica = FromProto<TReplicaInfo>(request->replica_info());

        context->SetRequestInfo("ReplicationCardId: %v, Replica: %v",
            replicationCardId,
            replica);

        if (!IsStableReplicaMode(replica.Mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", replica.Mode);
        }

        if (!IsStableReplicaState(replica.State)) {
            THROW_ERROR_EXCEPTION("Invalid replica state %Qlv", replica.State);
        }

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, RemoveTableReplica)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TReplicaId>(request->replica_id());

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v",
            replicationCardId,
            replicaId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->RemoveTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, AlterTableReplica)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto mode = request->has_mode()
            ? std::make_optional(FromProto<EReplicaMode>(request->mode()))
            : std::nullopt;
        auto state = request->has_state()
            ? std::make_optional(FromProto<EReplicaState>(request->state()))
            : std::nullopt;

        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
        }

        if (state && !IsStableReplicaState(*state)) {
            THROW_ERROR_EXCEPTION("Invalid replica state %Qlv", *state);
        }

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v, Mode: %v, State: %v",
            replicationCardId,
            replicaId,
            mode,
            state);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->AlterTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateReplicationProgress)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto progress = FromProto<TReplicationProgress>(request->replication_progress());

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v, ReplicationProgress: %v",
            replicationCardId,
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
