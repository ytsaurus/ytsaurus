#include "chaos_node_service.h"

#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "replication_card.h"
#include "replication_card_collocation.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_service.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosNode {

using namespace NRpc;
using namespace NChaosClient;
using namespace NHydra;
using namespace NClusterNode;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosNodeService
    : public THydraServiceBase
{
public:
    TChaosNodeService(
        IChaosSlotPtr slot,
        IAuthenticatorPtr authenticator)
        : THydraServiceBase(
            slot->GetHydraManager(),
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Default),
            TChaosNodeServiceProxy::GetDescriptor(),
            ChaosNodeLogger,
            slot->GetCellId(),
            CreateHydraManagerUpstreamSynchronizer(slot->GetHydraManager()),
            std::move(authenticator))
        , Slot_(std::move(slot))
    {
        YT_VERIFY(Slot_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateReplicationCardId));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FindReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateTableReplicaProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MigrateReplicationCards));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeChaosCell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateReplicationCardCollocation));
    }

private:
    const IChaosSlotPtr Slot_;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GenerateReplicationCardId)
    {
        context->SetRequestInfo();

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->GenerateReplicationCardId(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateReplicationCard)
    {
        context->SetRequestInfo();

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateReplicationCard(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, RemoveReplicationCard)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

        context->SetRequestInfo("ReplicationCardId: %v",
            replicationCardId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->RemoveReplicationCard(std::move(context));
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
        ToProto(protoReplicationCard->mutable_table_id(), replicationCard->GetTableId());
        protoReplicationCard->set_table_path(replicationCard->GetTablePath());
        protoReplicationCard->set_table_cluster_name(replicationCard->GetTableClusterName());
        protoReplicationCard->set_current_timestamp(replicationCard->GetCurrentTimestamp());

        if (auto* collocation = replicationCard->GetCollocation()) {
            ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), collocation->GetId());
        }

        std::vector<TCellId> coordinators;
        if (fetchOptions.IncludeCoordinators) {
            for (const auto& [cellId, info] : replicationCard->Coordinators()) {
                if (info.State == EShortcutState::Granted) {
                    coordinators.push_back(cellId);
                }
            }
            ToProto(protoReplicationCard->mutable_coordinator_cell_ids(), coordinators);
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            auto* protoEntry = protoReplicationCard->add_replicas();
            ToProto(protoEntry->mutable_id(), replicaId);
            ToProto(protoEntry->mutable_info(), replicaInfo, fetchOptions);
        }

        if (fetchOptions.IncludeReplicatedTableOptions) {
            protoReplicationCard->set_replicated_table_options(ConvertToYsonString(replicationCard->GetReplicatedTableOptions()).ToString());
        }

        context->SetResponseInfo("ReplicationCardId: %v, CoordinatorCellIds: %v, ReplicationCard: %v",
            replicationCardId,
            coordinators,
            *replicationCard);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, FindReplicationCard)
    {
        SyncWithUpstream();

        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

        context->SetRequestInfo("ReplicationCardId: %v",
            replicationCardId);

        const auto& chaosManager = Slot_->GetChaosManager();
        auto* replicationCard = chaosManager->GetReplicationCardOrThrow(replicationCardId);
        Y_UNUSED(replicationCard);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateTableReplica)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        const auto& clusterName = request->cluster_name();
        const auto& replicaPath = request->replica_path();

        context->SetRequestInfo("ReplicationCardId: %v, ClusterName: %v, ReplicaPath: %v",
            replicationCardId,
            clusterName,
            replicaPath);

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
            ? std::make_optional(FromProto<ETableReplicaMode>(request->mode()))
            : std::nullopt;
        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
        }

        auto enabled = request->has_enabled()
            ? std::make_optional(request->enabled())
            : std::nullopt;

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v, Mode: %v, Enabled: %v",
            replicationCardId,
            replicaId,
            mode,
            enabled);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->AlterTableReplica(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateTableReplicaProgress)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto progress = FromProto<TReplicationProgress>(request->replication_progress());

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v, ReplicationProgress: %v",
            replicationCardId,
            replicaId,
            progress);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->UpdateTableReplicaProgress(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, AlterReplicationCard)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

        context->SetRequestInfo("ReplicationCardId: %v",
            replicationCardId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->AlterReplicationCard(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, MigrateReplicationCards)
    {
        auto replicationCardIds = FromProto<std::vector<TReplicationCardId>>(request->replication_card_ids());
        bool migrateAllReplicationCards = request->migrate_all_replication_cards();
        bool suspendChaosCell = request->suspend_chaos_cell();

        context->SetRequestInfo("ReplicationCardIds: %v, MigrateAllReplicationCards: %v, SuspendChaosCell: %v",
            replicationCardIds,
            migrateAllReplicationCards,
            suspendChaosCell);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->MigrateReplicationCards(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, ResumeChaosCell)
    {
        context->SetRequestInfo();

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->ResumeChaosCell(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateReplicationCardCollocation)
    {
        auto replicationCardIds = FromProto<std::vector<TReplicationCardId>>(request->replication_card_ids());

        context->SetRequestInfo("ReplicationCardIds: %v", replicationCardIds);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateReplicationCardCollocation(std::move(context));
    }
};

IServicePtr CreateChaosNodeService(
    IChaosSlotPtr slot,
    IAuthenticatorPtr authenticator)
{
    return New<TChaosNodeService>(std::move(slot), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
