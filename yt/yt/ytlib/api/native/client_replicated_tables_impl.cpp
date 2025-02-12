#include "chaos_helpers.h"
#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "private.h"
#include "tablet_helpers.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/range_formatters.h>

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

bool IsReplicaInSync(
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

std::vector<TTableReplicaId> TClient::GetReplicatedTableInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    bool allKeys,
    const TGetInSyncReplicasOptions& options)
{
    THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
    THashSet<TTabletId> tabletIds;
    std::vector<TCellId> cellIds;

    auto registerTablet = [&] (const TTabletInfoPtr& tabletInfo) {
        if (tabletIds.insert(tabletInfo->TabletId).second) {
            ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);
            auto [it, emplaced] = cellToTabletIds.try_emplace(tabletInfo->CellId);
            if (emplaced) {
                cellIds.push_back(it->first);
            }
            it->second.push_back(tabletInfo->TabletId);
        }
    };

    if (allKeys) {
        for (const auto& tabletInfo : tableInfo->Tablets) {
            registerTablet(tabletInfo);
        }
    } else {
        auto evaluator = tableInfo->NeedKeyEvaluation
            ? Connection_->GetColumnEvaluatorCache()->Find(tableInfo->Schemas[ETableSchemaKind::Primary])
            : nullptr;
        auto evaluatedKeys = PermuteAndEvaluateKeys(tableInfo, nameTable, keys, evaluator);
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

    auto cellDescriptorsByPeer = GroupCellDescriptorsByPeer(Connection_, cellIds);

    int requestedTabletCount = 0;
    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
    futures.reserve(cellDescriptorsByPeer.size());
    for (const auto& cellDescriptors : cellDescriptorsByPeer) {
        auto channel = GetReadCellChannelOrThrow(cellDescriptors[0]);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        req->SetResponseHeavy(true);
        for (const auto& cellDescriptor : cellDescriptors) {
            auto cellId = cellDescriptor->CellId;
            for (auto tabletId : cellToTabletIds[cellId]) {
                ToProto(req->add_tablet_ids(), tabletId);
                ToProto(req->add_cell_ids(), cellId);
            }
        }

        requestedTabletCount += req->tablet_ids_size();

        futures.push_back(req->Invoke());
    }

    YT_VERIFY(requestedTabletCount == std::ssize(tabletIds));

    auto responses = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

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
    auto isChaos = static_cast<bool>(tableInfo->ReplicationCardId);

    auto replicationCard = isChaos
        ? GetSyncReplicationCard(Connection_, tableInfo)
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
        auto evaluator = tableInfo->NeedKeyEvaluation && isChaos
            ? GetNativeConnection()
                ->GetColumnEvaluatorCache()
                ->Find(tableInfo->Schemas[ETableSchemaKind::Primary])
            : nullptr;

        replicaIds = isChaos
            ? GetChaosTableInSyncReplicas(
                tableInfo,
                replicationCard,
                nameTable,
                evaluator,
                keys,
                allKeys,
                options.Timestamp)
            : GetReplicatedTableInSyncReplicas(
                tableInfo,
                nameTable,
                keys,
                allKeys,
                options);
    }

    YT_LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %v)",
        tableInfo->TableId,
        replicaIds,
        options.Timestamp);

    return replicaIds;
}

TTableReplicaInfoPtr PickRandomReplica(
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

NApi::NNative::IConnectionPtr TClient::GetReplicaConnectionOrThrow(const std::string& clusterName)
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

bool TClient::TReplicaClient::IsTerminated() const
{
    return Client->GetNativeConnection()->IsTerminated();
}

NApi::IClientPtr TClient::GetOrCreateReplicaClient(const std::string& clusterName)
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
    if (replicaClient->Client && !replicaClient->IsTerminated()) {
        return replicaClient->Client;
    }
    guard.Release();

    auto writerGuard = WriterGuard(replicaClient->Lock);
    if (replicaClient->Client && !replicaClient->IsTerminated()) {
        return replicaClient->Client;
    } else if (replicaClient->Client) {
        replicaClient->Client = nullptr;
        replicaClient->AsyncClient = {};
    }

    if (auto asyncClient = replicaClient->AsyncClient) {
        if (asyncClient.IsSet() && !asyncClient.Get().IsOK()) {
            replicaClient->AsyncClient = {};
        }

        writerGuard.Release();

        return WaitFor(asyncClient)
            .ValueOrThrow();
    }

    auto asyncClient = BIND([this, replicaClient, clusterName, this_ = MakeStrong(this)] {
        auto connection = GetReplicaConnectionOrThrow(clusterName);

        auto guard = WriterGuard(replicaClient->Lock);
        replicaClient->Client = connection->CreateNativeClient(Options_);
        return replicaClient->Client;
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
