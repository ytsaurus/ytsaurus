#include "client_impl.h"
#include "connection.h"
#include "tablet_helpers.h"
#include "config.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <util/random/random.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool TClient::IsReplicaInSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo,
    TTimestamp timestamp)
{
    if (IsReplicaSync(replicaInfo, tabletInfo)) {
        return true;
    }
    if (timestamp >= MinTimestamp &&
        timestamp <= MaxTimestamp &&
        replicaInfo.last_replication_timestamp() >= timestamp)
    {
        return true;
    }
    return false;
}

std::vector<TTableReplicaId> TClient::DoGetInSyncReplicasWithKeys(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    return DoGetInSyncReplicas(
        path,
        false,
        nameTable,
        keys,
        options);
}

std::vector<TTableReplicaId> TClient::DoGetInSyncReplicasWithoutKeys(
    const TYPath& path,
    const TGetInSyncReplicasOptions& options)
{
    return DoGetInSyncReplicas(
        path,
        true,
        nullptr,
        {},
        options);
}

TSharedRange<TUnversionedRow> TClient::PermuteAndEvaluateKeys(
    const TTableMountInfoPtr& tableInfo,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys)
{
    if (keys.empty()) {
        return {};
    }

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto idMapping = BuildColumnIdMapping(*schema, nameTable);

    auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
    auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

    struct TGetInSyncReplicasTag
    { };
    auto rowBuffer = New<TRowBuffer>(TGetInSyncReplicasTag());
    std::vector<TUnversionedRow> evaluatedKeys;

    for (auto key : keys) {
        ValidateClientKey(key, *schema, idMapping, nameTable);
        auto capturedKey = rowBuffer->CaptureAndPermuteRow(
            key,
            *schema,
            schema->GetKeyColumnCount(),
            idMapping,
            nullptr);

        if (evaluator) {
            evaluator->EvaluateKeys(capturedKey, rowBuffer);
        }

        evaluatedKeys.push_back(capturedKey);
    }

    return MakeSharedRange(evaluatedKeys, std::move(rowBuffer));
}

std::vector<TTableReplicaId> TClient::GetReplicatedTableInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    bool allKeys,
    const TGetInSyncReplicasOptions& options)
{
    THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
    THashSet<TTabletId> tabletIds;
    auto registerTablet = [&] (const TTabletInfoPtr& tabletInfo) {
        if (tabletIds.insert(tabletInfo->TabletId).second) {
            ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);
            cellToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
        }
    };

    if (allKeys) {
        for (const auto& tabletInfo : tableInfo->Tablets) {
            registerTablet(tabletInfo);
        }
    } else {
        auto evaluatedKeys = PermuteAndEvaluateKeys(tableInfo, nameTable, keys);
        for (auto capturedKey : evaluatedKeys) {
            registerTablet(tableInfo->GetTabletForRow(capturedKey));
        }
    }

    if (options.CachedSyncReplicasTimeout) {
        TTabletReadOptions tabletReadOptions;
        tabletReadOptions.CachedSyncReplicasTimeout = options.CachedSyncReplicasTimeout;
        tabletReadOptions.Timeout = options.Timeout;

        auto future = PickInSyncReplicas(
            Connection_,
            tableInfo,
            tabletReadOptions,
            std::move(cellToTabletIds));

        auto replicaInfoList = WaitFor(future)
            .ValueOrThrow();

        std::vector<TTableReplicaId> replicaIds;
        replicaIds.reserve(replicaInfoList.size());

        for (auto& replicaInfo : replicaInfoList) {
            replicaIds.push_back(replicaInfo->ReplicaId);
        }

        return replicaIds;
    }

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
    for (const auto& [cellId, perCellTabletIds] : cellToTabletIds) {
        auto channel = GetReadCellChannelOrThrow(cellId);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        req->SetResponseHeavy(true);
        ToProto(req->mutable_tablet_ids(), perCellTabletIds);
        for (int index = 0; index < std::ssize(perCellTabletIds); ++index) {
            ToProto(req->add_cell_ids(), cellId);
        }

        futures.push_back(req->Invoke());
    }

    auto responsesResult = WaitFor(AllSucceeded(futures));
    auto responses = responsesResult.ValueOrThrow();

    THashMap<TTableReplicaId, int> replicaIdToCount;
    for (const auto& response : responses) {
        for (const auto& protoTabletInfo : response->tablets()) {
            for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo, options.Timestamp)) {
                    ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                }
            }
        }
    }

    std::vector<TTableReplicaId> replicaIds;
    for (auto [replicaId, count] : replicaIdToCount) {
        if (count == std::ssize(tabletIds)) {
            replicaIds.push_back(replicaId);
        }
    }

    return replicaIds;
}

std::vector<TTableReplicaId> TClient::GetChaosTableInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TReplicationCardPtr& replicationCard,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    bool allKeys,
    TTimestamp userTimestamp)
{
    auto evaluatedKeys = PermuteAndEvaluateKeys(tableInfo, nameTable, keys);
    std::vector<TTableReplicaId> replicaIds;

    auto isTimestampInSync = [&] (const auto& replica, auto timestamp) {
        if (const auto& item = replica.History.back();
            IsReplicaReallySync(item.Mode, item.State) && timestamp >= item.Timestamp)
        {
            return true;
        }
        if (userTimestamp >= MinTimestamp &&
            userTimestamp <= MaxTimestamp &&
            timestamp >= userTimestamp)
        {
            return true;
        }
        return false;
    };

    auto isReplicaInSync = [&] (const auto& replica) {
        if (allKeys) {
            auto timestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
            if (!isTimestampInSync(replica, timestamp)) {
                return false;
            }
        } else {
            for (auto key : evaluatedKeys) {
                auto timestamp = GetReplicationProgressTimestampForKeyOrThrow(replica.ReplicationProgress, key);
                if (!isTimestampInSync(replica, timestamp)) {
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

std::vector<TTableReplicaId> TClient::DoGetInSyncReplicas(
    const TYPath& path,
    bool allKeys,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    ValidateGetInSyncReplicasTimestamp(options.Timestamp);

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();
    bool isChaos = static_cast<bool>(tableInfo->ReplicationCardId);

    auto replicationCard = isChaos
        ? GetSyncReplicationCard(tableInfo)
        : nullptr;

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();
    if (!isChaos) {
        tableInfo->ValidateReplicated();
    }

    auto getAllReplicaIds = [&] {
        std::vector<TTableReplicaId> replicaIds;
        if (isChaos) {
            for (const auto& [replicaId, replica] : replicationCard->Replicas) {
                if (replica.ContentType == ETableReplicaContentType::Data) {
                    replicaIds.push_back(replicaId);
                }
            }
        } else {
            for (const auto& replica : tableInfo->Replicas) {
                replicaIds.push_back(replica->ReplicaId);
            }
        }
        return replicaIds;
    };

    std::vector<TTableReplicaId> replicaIds;

    if (!allKeys && keys.Empty()) {
        replicaIds = getAllReplicaIds();
    } else {
        replicaIds = isChaos
            ? GetChaosTableInSyncReplicas(tableInfo, replicationCard, nameTable, keys, allKeys, options.Timestamp)
            : GetReplicatedTableInSyncReplicas(tableInfo, nameTable, keys, allKeys, options);
    }

    YT_LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %v)",
        tableInfo->TableId,
        replicaIds,
        options.Timestamp);

    return replicaIds;
}

TTableReplicaInfoPtr TClient::PickRandomReplica(
    const TTableReplicaInfoPtrList& replicas)
{
    YT_VERIFY(!replicas.empty());
    return replicas[RandomNumber<size_t>() % replicas.size()];
}

TClient::TReplicaFallbackInfo TClient::GetReplicaFallbackInfo(
    const TTableReplicaInfoPtrList& replicas)
{
    auto replicaInfo = PickRandomReplica(replicas);
    return {
        .Client = GetOrCreateReplicaClient(replicaInfo->ClusterName),
        .Path = replicaInfo->ReplicaPath,
        .ReplicaId = replicaInfo->ReplicaId
    };
}

TString TClient::PickRandomCluster(
    const std::vector<TString>& clusterNames)
{
    YT_VERIFY(!clusterNames.empty());
    return clusterNames[RandomNumber<size_t>() % clusterNames.size()];
}

std::pair<std::vector<TTableMountInfoPtr>, std::vector<TTableReplicaInfoPtrList>> TClient::PrepareInSyncReplicaCandidates(
    const TTabletReadOptions& options,
    NAst::TQuery* query)
{
    std::vector<TYPath> paths{query->Table.Path};
    for (const auto& join : query->Joins) {
        paths.push_back(join.Table.Path);
    }

    const auto& tableMountCache = Connection_->GetTableMountCache();
    std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
    for (const auto& path : paths) {
        asyncTableInfos.push_back(tableMountCache->GetTableInfo(path));
    }

    auto tableInfos = WaitFor(AllSucceeded(asyncTableInfos))
        .ValueOrThrow();

    bool someReplicated = false;
    bool someNotReplicated = false;
    for (const auto& tableInfo : tableInfos) {
        if (tableInfo->IsReplicationLog()) {
            THROW_ERROR_EXCEPTION("Replication log table is not supported for this type of query");
        }
        if (tableInfo->IsReplicated() ||
            tableInfo->IsChaosReplicated() ||
            (tableInfo->ReplicationCardId && options.ReplicaConsistency == EReplicaConsistency::Sync))
        {
            someReplicated = true;
        } else {
            someNotReplicated = true;
        }
    }

    if (someReplicated && someNotReplicated) {
        THROW_ERROR_EXCEPTION("Query involves both replicated and non-replicated tables");
    }

    if (!someReplicated) {
        return {};
    }

    auto pickInSyncReplicas = [&] (const TTableMountInfoPtr& tableInfo) {
        if (!tableInfo->ReplicationCardId) {
            return PickInSyncReplicas(Connection_, tableInfo, options);
        }

        TTableReplicaInfoPtrList inSyncReplicas;
        auto replicationCard = GetSyncReplicationCard(tableInfo);
        auto replicaIds = GetChaosTableInSyncReplicas(
            tableInfo,
            replicationCard,
            /*nameTable*/ nullptr,
            /*keys*/ {},
            /*allKeys*/ true,
            options.Timestamp);

        auto bannedReplicaTracker = Connection_->GetBannedReplicaTrackerCache()->GetTracker(tableInfo->TableId);
        bannedReplicaTracker->SyncReplicas(replicationCard);

        YT_LOG_DEBUG("Picked in-sync replicas for select (ReplicaIds: %v, Timestamp: %v, ReplicationCard: %v)",
            replicaIds,
            options.Timestamp,
            *replicationCard);

        for (auto replicaId : replicaIds) {
            const auto& replica = GetOrCrash(replicationCard->Replicas, replicaId);
            auto replicaInfo = New<TTableReplicaInfo>();
            replicaInfo->ReplicaId = replicaId;
            replicaInfo->ClusterName = replica.ClusterName;
            replicaInfo->ReplicaPath = replica.ReplicaPath;
            replicaInfo->Mode = replica.Mode;
            inSyncReplicas.push_back(std::move(replicaInfo));
        }

        return MakeFuture(std::move(inSyncReplicas));
    };

    std::vector<TFuture<TTableReplicaInfoPtrList>> asyncCandidates;
    for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
        asyncCandidates.push_back(pickInSyncReplicas(tableInfos[tableIndex]));
    }

    auto candidates = WaitFor(AllSucceeded(asyncCandidates))
        .ValueOrThrow();

    return {tableInfos, candidates};
}

std::pair<TString, TSelectRowsOptions::TExpectedTableSchemas> TClient::PickInSyncClusterAndPatchQuery(
    const std::vector<TTableMountInfoPtr>& tableInfos,
    const std::vector<TTableReplicaInfoPtrList>& candidates,
    NAst::TQuery* query)
{
    YT_VERIFY(tableInfos.size() == candidates.size());

    std::vector<TYPath> paths;
    for (const auto& tableInfo : tableInfos) {
        paths.push_back(tableInfo->Path);
    }

    const auto& bannedReplicaTrackerCache = Connection_->GetBannedReplicaTrackerCache();
    auto emptyBannedReplicaTracker = CreateEmptyBannedReplicaTracker();

    std::vector<IBannedReplicaTrackerPtr> bannedReplicaTrackers;
    for (const auto& tableInfo : tableInfos) {
        const auto& bannedReplicaTracker = tableInfo->ReplicationCardId
            ? bannedReplicaTrackerCache->GetTracker(tableInfo->TableId)
            : emptyBannedReplicaTracker;
        bannedReplicaTrackers.push_back(bannedReplicaTracker);
    }

    std::vector<std::vector<TTableReplicaId>> bannedSyncReplicaIds(candidates.size());

    THashMap<TString, int> clusterNameToCount;
    for (int index = 0; index < std::ssize(tableInfos); ++index) {
        const auto& replicaInfos = candidates[index];
        const auto& bannedReplicaTracker = bannedReplicaTrackers[index];
        auto& bannedIds = bannedSyncReplicaIds[index];

        THashSet<TString> clusterNames;
        for (const auto& replicaInfo : replicaInfos) {
            if (bannedReplicaTracker->IsReplicaBanned(replicaInfo->ReplicaId)) {
                bannedIds.push_back(replicaInfo->ReplicaId);
            } else {
                clusterNames.insert(replicaInfo->ClusterName);
            }
        }
        for (const auto& clusterName : clusterNames) {
            ++clusterNameToCount[clusterName];
        }
    }

    YT_LOG_DEBUG("Gathered banned replicas while selecting in-sync cluster (Tables: %v, BannedReplicaIds: %v)",
        paths,
        bannedSyncReplicaIds);

    std::vector<TString> inSyncClusterNames;
    for (const auto& [name, count] : clusterNameToCount) {
        if (count == std::ssize(paths)) {
            inSyncClusterNames.push_back(name);
        }
    }

    if (inSyncClusterNames.empty()) {
        std::vector<TError> replicaErrors;
        std::vector<TTableReplicaId> flatBannedReplicaIds;

        for (int index = 0; index < std::ssize(bannedSyncReplicaIds); ++index) {
            const auto& bannedReplicaTracker = bannedReplicaTrackers[index];
            for (auto bannedReplicaId : bannedSyncReplicaIds[index]) {
                if (auto error = bannedReplicaTracker->GetReplicaError(bannedReplicaId); !error.IsOK()) {
                    replicaErrors.push_back(std::move(error));
                    flatBannedReplicaIds.push_back(bannedReplicaId);
                }
            }
        }

        auto error = TError(
            NTabletClient::EErrorCode::NoInSyncReplicas,
            "No single cluster contains in-sync replicas for all involved tables %v",
            paths)
            << TErrorAttribute("banned_replicas", flatBannedReplicaIds);
        *error.MutableInnerErrors() = std::move(replicaErrors);
        THROW_ERROR error;
    }

    auto inSyncClusterName = PickRandomCluster(inSyncClusterNames);

    auto chaosTableSchemas = std::vector<TTableSchemaPtr>(tableInfos.size());
    for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
        const auto& tableInfo = tableInfos[tableIndex];
        if (tableInfo->ReplicationCardId) {
            chaosTableSchemas[tableIndex] = tableInfo->Schemas[ETableSchemaKind::Primary];
        }
    }

    TSelectRowsOptions::TExpectedTableSchemas expectedTableSchemas;
    std::vector<TTableReplicaId> pickedReplicaIds;

    auto patchTableDescriptor = [&] (
        NAst::TTableDescriptor* descriptor,
        const TTableReplicaInfoPtrList& replicaInfos,
        TTableSchemaPtr tableSchema,
        const IBannedReplicaTrackerPtr& bannedReplicaTracker)
    {
        for (const auto& replicaInfo : replicaInfos) {
            if (replicaInfo->ClusterName == inSyncClusterName && !bannedReplicaTracker->IsReplicaBanned(replicaInfo->ReplicaId)) {
                pickedReplicaIds.push_back(replicaInfo->ReplicaId);
                descriptor->Path = replicaInfo->ReplicaPath;
                if (tableSchema) {
                    InsertOrCrash(expectedTableSchemas, std::make_pair(replicaInfo->ReplicaPath, std::move(tableSchema)));
                }
                return;
            }
        }
        YT_ABORT();
    };

    patchTableDescriptor(
        &query->Table,
        candidates[0],
        chaosTableSchemas[0],
        bannedReplicaTrackers[0]);

    for (size_t index = 0; index < query->Joins.size(); ++index) {
        patchTableDescriptor(
            &query->Joins[index].Table,
            candidates[index + 1],
            chaosTableSchemas[index + 1],
            bannedReplicaTrackers[index + 1]);
    }

    YT_LOG_DEBUG("In-sync cluster selected (Paths: %v, ClusterName: %v, ReplicaIds: %v)",
        paths,
        inSyncClusterName,
        pickedReplicaIds);

    return {inSyncClusterName, expectedTableSchemas};
}

NApi::IConnectionPtr TClient::GetReplicaConnectionOrThrow(const TString& clusterName)
{
    const auto& clusterDirectory = Connection_->GetClusterDirectory();
    auto replicaConnection = clusterDirectory->FindConnection(clusterName);
    if (replicaConnection) {
        return replicaConnection;
    }

    WaitFor(Connection_->GetClusterDirectorySynchronizer()->Sync())
        .ThrowOnError();

    return clusterDirectory->GetConnectionOrThrow(clusterName);
}

NApi::IClientPtr TClient::GetOrCreateReplicaClient(const TString& clusterName)
{
    TIntrusivePtr<TReplicaClient> replicaClient;

    {
        auto guard = ReaderGuard(ReplicaClientsLock_);
        if (auto it = ReplicaClients_.find(clusterName); it != ReplicaClients_.end()) {
            replicaClient = it->second;
        } else {
            guard.Release();

            auto guard = WriterGuard(ReplicaClientsLock_);
            if (auto it = ReplicaClients_.find(clusterName); it != ReplicaClients_.end()) {
                replicaClient = it->second;
            } else {
                replicaClient = New<TReplicaClient>();
                ReplicaClients_.emplace(clusterName, replicaClient);
            }
        }
    }

    auto guard = ReaderGuard(replicaClient->Lock);
    if (replicaClient->Client) {
        return replicaClient->Client;
    }
    guard.Release();

    auto writerGuard = WriterGuard(replicaClient->Lock);
    if (replicaClient->Client) {
        return replicaClient->Client;
    }

    if (auto asyncClient = replicaClient->AsyncClient) {
        writerGuard.Release();

        return WaitFor(asyncClient)
            .ValueOrThrow();
    }

    auto asyncClient = BIND([this, replicaClient, clusterName, this_=MakeStrong(this)] () {
        try {
            auto connection = GetReplicaConnectionOrThrow(clusterName);

            auto guard = WriterGuard(replicaClient->Lock);
            replicaClient->Client = connection->CreateClient(Options_);
            return replicaClient->Client;
        } catch (const std::exception& ex) {
            auto guard = WriterGuard(replicaClient->Lock);
            replicaClient->AsyncClient = {};

            throw;
        }
    })
        .AsyncVia(Connection_->GetInvoker())
        .Run();

    replicaClient->AsyncClient = asyncClient;
    writerGuard.Release();

    return WaitFor(asyncClient)
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
