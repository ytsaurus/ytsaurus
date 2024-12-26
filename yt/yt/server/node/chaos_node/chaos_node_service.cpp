#include "chaos_node_service.h"

#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "replication_card.h"
#include "replication_card_collocation.h"

#include <yt/yt/server/lib/chaos_node/replication_card_watcher_service_callbacks.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NClusterNode;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
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
            ChaosNodeLogger(),
            CreateHydraManagerUpstreamSynchronizer(slot->GetHydraManager()),
            TServiceOptions{
                .RealmId = slot->GetCellId(),
                .Authenticator = std::move(authenticator),
            })
        , Slot_(std::move(slot))
    {
        YT_VERIFY(Slot_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateReplicationCardId));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FindReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateTableReplicaProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MigrateReplicationCards));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeChaosCell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateReplicationCardCollocation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCardCollocation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ForsakeCoordinator));
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

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, ForsakeCoordinator)
    {
        auto coordinatorCellId = FromProto<TReplicationCardId>(request->coordinator_cell_id());

        context->SetRequestInfo("CoordinatorCellId: %v",
            coordinatorCellId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->ForsakeCoordinator(std::move(context));
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
        } else if (replicationCard->GetAwaitingCollocationId()) {
            ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), replicationCard->GetAwaitingCollocationId());
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

        auto objectId = FromProto<TObjectId>(request->replication_card_id());

        context->SetRequestInfo("ObjectId: %v, Type: %v",
            objectId,
            TypeFromId(objectId));

        const auto& chaosManager = Slot_->GetChaosManager();

        switch (TypeFromId(objectId)) {
            case EObjectType::ReplicationCard: {
                auto* replicationCard = chaosManager->GetReplicationCardOrThrow(objectId);
                Y_UNUSED(replicationCard);
                break;
            }
            case EObjectType::ReplicationCardCollocation: {
                auto* collocation = chaosManager->GetReplicationCardCollocationOrThrow(objectId);
                Y_UNUSED(collocation);
                break;
            }
            default:
                THROW_ERROR_EXCEPTION("Unsupported object type: %v",
                    TypeFromId(objectId));
        }

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

        auto replicaPath = request->has_replica_path()
            ? std::make_optional(request->replica_path())
            : std::nullopt;

        context->SetRequestInfo("ReplicationCardId: %v, ReplicaId: %v, Mode: %v, Enabled: %v, ReplicaPath: %v",
            replicationCardId,
            replicaId,
            mode,
            enabled,
            replicaPath);

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

        context->SetRequestInfo("ReplicationCardIds: %v, Options: %v",
            replicationCardIds,
            request->options());

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateReplicationCardCollocation(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCardCollocation)
    {
        SyncWithUpstream();

        auto replicationCardCollocationId = FromProto<TReplicationCardCollocationId>(
            request->replication_card_collocation_id());
        context->SetRequestInfo("ReplicationCardCollocationId: %v", replicationCardCollocationId);

        const auto& chaosManager = Slot_->GetChaosManager();
        auto* collocation = chaosManager->GetReplicationCardCollocationOrThrow(replicationCardCollocationId);
        auto replicationCardIds = collocation->GetReplicationCardIds();

        context->SetResponseInfo("ReplicationCardCollocationId: %v, ReplicationCardIds: %v",
            replicationCardCollocationId,
            replicationCardIds);
        ToProto(response->mutable_replication_card_ids(), replicationCardIds);
        response->set_options(ConvertToYsonString(collocation->Options()).ToString());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, WatchReplicationCard)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto cacheTimestamp = FromProto<TTimestamp>(request->replication_card_cache_timestamp());

        context->SetRequestInfo("ReplicationCardId %v, Timestamp: %v",
            replicationCardId,
            cacheTimestamp);

        const auto& replicationCardWatcher = Slot_->GetReplicationCardsWatcher();
        replicationCardWatcher->WatchReplicationCard(
            replicationCardId,
            cacheTimestamp,
            CreateReplicationCardWatcherCallbacks(std::move(context)));
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
