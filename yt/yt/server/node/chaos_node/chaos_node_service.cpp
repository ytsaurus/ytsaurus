#include "chaos_node_service.h"

#include "chaos_manager.h"
#include "chaos_slot.h"
#include "chaos_lease.h"
#include "private.h"
#include "replication_card.h"
#include "replication_card_collocation.h"
#include "replication_card_serialization.h"

#include <yt/yt/server/lib/chaos_node/config.h>
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

#include <yt/yt/core/yson/protobuf_helpers.h>

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
        // COMPAT(gryzlov-ad)
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FindChaosObject));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateTableProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateMultipleTableProgresses));
    }

private:
    const IChaosSlotPtr Slot_;

    struct TCachedCard
    {
        TCpuInstant Deadline;
        NChaosClient::TReplicationCardPtr CachedCard;
    };
    THashMap<TReplicationCardId, TCachedCard> ReplicationCardCache_;

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
        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());

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
        if (!fetchOptions.IncludeProgress) {
            // Replication card is small without replication progress,
            // so do not try to copy or validate the progress if progress was not requested.
            ToProto(response->mutable_replication_card(), *replicationCard, fetchOptions);
            context->SetResponseInfo("ReplicationCardId: %v",
                replicationCardId);
            context->Reply();
            return;
        }

        auto awaitingCollocationId = replicationCard->GetAwaitingCollocationId();

        auto isSame = [] (const auto& cachedCard, const auto& replicationCard) {
            if (cachedCard->Era != replicationCard->GetEra()) {
                return false;
            }

            auto collocationId = replicationCard->GetCollocation()
                ? replicationCard->GetCollocation()->GetId()
                : TReplicationCardCollocationId();

            if (cachedCard->ReplicationCardCollocationId != collocationId) {
                return false;
            }

            int grantedCoordinatorsCount = 0;
            for (const auto& [_, info] : replicationCard->Coordinators()) {
                if (info.State == EShortcutState::Granted) {
                    ++grantedCoordinatorsCount;
                }
            }

            if (std::ssize(cachedCard->CoordinatorCellIds) != grantedCoordinatorsCount) {
                return false;
            }

            if (cachedCard->ReplicatedTableOptions != replicationCard->GetReplicatedTableOptions()) {
                return false;
            }

            for (const auto& [replicaId, cachedInfo] : cachedCard->Replicas) {
                auto it = replicationCard->Replicas().find(replicaId);
                if (!it ||
                    cachedInfo.State != it->second.State ||
                    cachedInfo.Mode != it->second.Mode ||
                    cachedInfo.EnableReplicatedTableTracker != it->second.EnableReplicatedTableTracker)
                {
                    return false;
                }

                // TODO(savrus): This is computationally-intensive, remove or cache.
                if (GetReplicationProgressMinTimestamp(cachedInfo.ReplicationProgress) < GetReplicationProgressMinTimestamp(it->second.ReplicationProgress)) {
                    return false;
                }
            }

            return true;
        };

        NChaosClient::TReplicationCardPtr replicationCardCopy;
        auto cached = ReplicationCardCache_.find(replicationCardId);
        if (!cached || cached->second.Deadline < GetCpuInstant() || !isSame(cached->second.CachedCard, replicationCard))
        {
            auto options = TReplicationCardFetchOptions{
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true
            };

            replicationCardCopy = replicationCard->ConvertToClientCard(options);
            ReplicationCardCache_[replicationCardId] = TCachedCard{
                .Deadline = GetCpuInstant() +
                    DurationToCpuDuration(Slot_->GetDynamicConfig()->ReplicationCardAutomatonCacheExpirationTime),
                .CachedCard = replicationCardCopy
            };
        } else {
            replicationCardCopy = cached->second.CachedCard;
        }

        const auto& invoker = Slot_->GetSnapshotStoreReadPoolInvoker();
        auto callback = BIND([context, response, replicationCard = std::move(replicationCardCopy), fetchOptions, awaitingCollocationId] {
            auto* protoReplicationCard = response->mutable_replication_card();
            ToProto(protoReplicationCard, *replicationCard, fetchOptions);
            if (!replicationCard->ReplicationCardCollocationId && awaitingCollocationId) {
                ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), awaitingCollocationId);
            }
        }).AsyncVia(invoker);

        context->ReplyFrom(callback());
    }

    void DoFindChaosObject(TChaosObjectId chaosObjectId)
    {
        const auto& chaosManager = Slot_->GetChaosManager();

        switch (TypeFromId(chaosObjectId)) {
            case EObjectType::ReplicationCard: {
                auto* replicationCard = chaosManager->GetReplicationCardOrThrow(chaosObjectId);
                Y_UNUSED(replicationCard);
                break;
            }
            case EObjectType::ReplicationCardCollocation: {
                auto* collocation = chaosManager->GetReplicationCardCollocationOrThrow(chaosObjectId);
                Y_UNUSED(collocation);
                break;
            }
            case EObjectType::ChaosLease: {
                auto* chaosLease = chaosManager->GetChaosLeaseOrThrow(chaosObjectId);
                Y_UNUSED(chaosLease);
                break;
            }
            default:
                THROW_ERROR_EXCEPTION("Unsupported object type: %v",
                    TypeFromId(chaosObjectId));
        }
    }

    // COMPAT(gryzlov-ad)
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, FindReplicationCard)
    {
        SyncWithUpstream();

        auto objectId = FromProto<TObjectId>(request->replication_card_id());

        context->SetRequestInfo("ObjectId: %v, Type: %v",
            objectId,
            TypeFromId(objectId));

        DoFindChaosObject(objectId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, FindChaosObject)
    {
        SyncWithUpstream();

        auto objectId = FromProto<TChaosObjectId>(request->chaos_object_id());

        context->SetRequestInfo("ChaosObjectId: %v, Type: %v",
            objectId,
            TypeFromId(objectId));

        DoFindChaosObject(objectId);

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

        bool force = request->force();

        context->SetRequestInfo(
            "ReplicationCardId: %v, ReplicaId: %v, Mode: %v, Enabled: %v, ReplicaPath: %v, Force: %v",
            replicationCardId,
            replicaId,
            mode,
            enabled,
            replicaPath,
            force);

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

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateTableProgress)
    {
        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->UpdateTableProgress(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateMultipleTableProgresses)
    {
        int replicationCardUpdatesSize = request->replication_card_progress_updates().size();
        context->SetRequestInfo("ReplicationCardCount: %v",
            replicationCardUpdatesSize);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->UpdateMultipleTableProgresses(std::move(context));
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
        response->set_options(ToProto(ConvertToYsonString(collocation->Options())));
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

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, CreateChaosLease)
    {
        context->SetRequestInfo();

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->CreateChaosLease(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetChaosLease)
    {
        auto chaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());

        context->SetRequestInfo("ChaosLeaseId: %v",
            chaosLeaseId);

        const auto& chaosManager = Slot_->GetChaosManager();
        auto* chaosLease = chaosManager->GetChaosLeaseOrThrow(chaosLeaseId);
        response->set_timeout(ToProto(chaosLease->GetTimeout()));

        auto futureLastPingTime = chaosManager->GetChaosLeaseTracker()->GetLastPingTime(chaosLeaseId)
            .Apply(BIND([=] (TInstant lastPingTime) {
                response->set_last_ping_time(ToProto(lastPingTime));
            }));

        context->ReplyFrom(futureLastPingTime);
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, RemoveChaosLease)
    {
        auto chaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());

        context->SetRequestInfo("ChaosLeaseId: %v",
            chaosLeaseId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->RemoveChaosLease(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, PingChaosLease)
    {
        auto chaosLeaseId = FromProto<TChaosLeaseId>(context->Request().chaos_lease_id());
        bool pingAncestors = context->Request().ping_ancestors();

        context->SetRequestInfo("ChaosLeaseId: %v, PingAncestors: %v",
            chaosLeaseId,
            pingAncestors);


        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->PingChaosLease(std::move(context));
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
