#include "chaos_helpers.h"
#include "config.h"
#include "connection.h"
#include "tablet_helpers.h"

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>
#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NLogging;
using namespace NQueryClient;
using namespace NTabletClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TReplicationCardPtr GetSyncReplicationCard(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableInfo)
{
    const auto& Logger = connection->GetLogger();

    const auto& mountCacheConfig = connection->GetStaticConfig()->TableMountCache;
    const auto& replicationCardCache = connection->GetReplicationCardCache();

    TReplicationCardPtr replicationCard;
    auto fetchOptions = TReplicationCardFetchOptions{
        .IncludeCoordinators = true,
        .IncludeProgress = true,
        .IncludeHistory = true,
    };
    auto key = TReplicationCardCacheKey{
        .CardId = tableInfo->ReplicationCardId,
        .FetchOptions = fetchOptions,
    };

    for (int retryCount = 0; retryCount < mountCacheConfig->OnErrorRetryCount; ++retryCount) {
        YT_LOG_DEBUG("Synchronizing replication card (ReplicationCardId: %v, Attempt: %v)",
            tableInfo->ReplicationCardId,
            retryCount);

        if (retryCount > 0) {
            if (replicationCard) {
                key.RefreshEra = replicationCard->Era;
                replicationCardCache->ForceRefresh(key, replicationCard);
            }

            TDelayedExecutor::WaitForDuration(mountCacheConfig->OnErrorSlackPeriod);
        }

        auto futureReplicationCard = replicationCardCache->GetReplicationCard(key);
        auto replicationCardOrError = WaitForFast(futureReplicationCard);

        if (!replicationCardOrError.IsOK()) {
            YT_LOG_DEBUG(replicationCardOrError, "Failed to get replication card from cache (ReplicationCardId: %v)",
                tableInfo->ReplicationCardId);
            continue;
        }

        replicationCard = replicationCardOrError.Value();

        if (replicationCard->CoordinatorCellIds.empty()) {
            YT_LOG_DEBUG("Replication card contains no coordinators (ReplicationCard: %v)",
                *replicationCard);
            continue;
        }

        auto coordinator = replicationCard->CoordinatorCellIds[RandomNumber<size_t>() % replicationCard->CoordinatorCellIds.size()];
        auto channel = connection->GetChaosChannelByCellId(coordinator, EPeerKind::Leader);
        auto proxy = TCoordinatorServiceProxy(channel);
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosNodeServiceTimeout);
        auto req = proxy.GetReplicationCardEra();

        ToProto(req->mutable_replication_card_id(), tableInfo->ReplicationCardId);

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to get replication card from coordinator (ReplicationCardId: %v)",
                tableInfo->ReplicationCardId);
            continue;
        }

        auto rsp = rspOrError.Value();
        auto coordinatorEra = rsp->replication_era();

        YT_LOG_DEBUG("Got replication card era from coordinator (Era: %v)",
            coordinatorEra);

        if (replicationCard->Era == coordinatorEra) {
            return replicationCard;
        }

        YT_VERIFY(replicationCard->Era < coordinatorEra);

        YT_LOG_DEBUG("Replication card era mismatch coordinator era (ReplicationCardEra: %v, CoordinatorEra: %v)",
            replicationCard->Era,
            coordinatorEra);
    }

    THROW_ERROR_EXCEPTION("Unable to synchronize replication card")
        << TErrorAttribute("replication_card_id", tableInfo->ReplicationCardId);
}

std::vector<TTableReplicaId> GetChaosTableInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TReplicationCardPtr& replicationCard,
    const TNameTablePtr& nameTable,
    const TColumnEvaluatorPtr& columnEvaluator,
    const TSharedRange<TLegacyKey>& keys,
    bool allKeys,
    TTimestamp userTimestamp)
{
    auto evaluatedKeys = PermuteAndEvaluateKeys(tableInfo, nameTable, keys, columnEvaluator);
    std::vector<TTableReplicaId> replicaIds;

    auto isReplicationProgressGood = [&] (const auto& replica, auto replicationTimestamp) {
        return (IsReplicaReallySync(replica.Mode, replica.State, replica.History) &&
            replicationTimestamp >= replica.History.back().Timestamp) ||
            IsTimestampInSync(userTimestamp, replicationTimestamp);
    };

    auto isReplicaInSync = [&] (const auto& replica) {
        if (allKeys) {
            auto timestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
            return isReplicationProgressGood(replica, timestamp);
        } else {
            for (auto key : evaluatedKeys) {
                auto timestamp = GetReplicationProgressTimestampForKeyOrThrow(replica.ReplicationProgress, key);
                if (!isReplicationProgressGood(replica, timestamp)) {
                    return false;
                }
            }
        }
        return true;
    };

    for (const auto& [replicaId, replica] : replicationCard->Replicas) {
        if (tableInfo->IsSorted() && replica.ContentType != ETableReplicaContentType::Data) {
            continue;
        } else if (!tableInfo->IsSorted() && replica.ContentType != ETableReplicaContentType::Queue) {
            continue;
        }
        if (isReplicaInSync(replica)) {
            replicaIds.push_back(replicaId);
        }
    }

    return replicaIds;
}

TTableReplicaInfoPtrList PickInSyncChaosReplicas(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options)
{
    const auto& Logger = connection->GetLogger();

    YT_ASSERT(tableInfo->ReplicationCardId);

    auto replicationCard = GetSyncReplicationCard(connection, tableInfo);
    auto replicaIds = GetChaosTableInSyncReplicas(
        tableInfo,
        replicationCard,
        /*nameTable*/ nullptr,
        tableInfo->NeedKeyEvaluation
            ? connection->GetColumnEvaluatorCache()->Find(tableInfo->Schemas[ETableSchemaKind::Primary])
            : nullptr,
        /*keys*/ {},
        /*allKeys*/ true,
        options.Timestamp);

    auto bannedReplicaTracker = connection->GetBannedReplicaTrackerCache()->GetTracker(tableInfo->TableId);
    bannedReplicaTracker->SyncReplicas(replicationCard);

    YT_LOG_DEBUG("Picked in-sync replicas for table (TablePath: %v, ReplicaIds: %v, Timestamp: %v, ReplicationCard: %v)",
        tableInfo->Path,
        replicaIds,
        options.Timestamp,
        *replicationCard);

    TTableReplicaInfoPtrList inSyncReplicas;
    inSyncReplicas.reserve(replicaIds.size());
    for (auto replicaId : replicaIds) {
        const auto& replica = GetOrCrash(replicationCard->Replicas, replicaId);
        auto replicaInfo = New<TTableReplicaInfo>();
        replicaInfo->ReplicaId = replicaId;
        replicaInfo->ClusterName = replica.ClusterName;
        replicaInfo->ReplicaPath = replica.ReplicaPath;
        replicaInfo->Mode = replica.Mode;
        inSyncReplicas.push_back(std::move(replicaInfo));
    }

    return inSyncReplicas;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
