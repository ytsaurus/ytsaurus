#include "client_impl.h"
#include "connection.h"
#include "tablet_helpers.h"
#include "config.h"

#include <yt/yt/ytlib/query_client/query_service_proxy.h>
#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <util/random/random.h>

namespace NYT::NApi::NNative {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NHiveClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TClient::IsReplicaSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo)
{
    return
        FromProto<ETableReplicaMode>(replicaInfo.mode()) == ETableReplicaMode::Sync &&
        replicaInfo.current_replication_row_index() >= tabletInfo.total_row_count();
}

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

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();
    tableInfo->ValidateReplicated();

    std::vector<TTableReplicaId> replicaIds;

    if (!allKeys && keys.Empty()) {
        for (const auto& replica : tableInfo->Replicas) {
            replicaIds.push_back(replica->ReplicaId);
        }
    } else {
        THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
        THashSet<TTabletId> tabletIds;
        auto registerTablet = [&] (const TTabletInfoPtr& tabletInfo) {
            if (tabletIds.insert(tabletInfo->TabletId).second) {
                ValidateTabletMountedOrFrozen(tabletInfo);
                cellToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
            }
        };

        if (allKeys) {
            for (const auto& tabletInfo : tableInfo->Tablets) {
                registerTablet(tabletInfo);
            }
        } else {
            const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
            auto idMapping = BuildColumnIdMapping(*schema, nameTable);

            struct TGetInSyncReplicasTag
            { };
            auto rowBuffer = New<TRowBuffer>(TGetInSyncReplicasTag());

            auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
            auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

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

                registerTablet(tableInfo->GetTabletForRow(capturedKey));
            }
        }

        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
        for (const auto& [cellId, perCellTabletIds] : cellToTabletIds) {
            auto channel = GetReadCellChannelOrThrow(cellId);

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

            auto req = proxy.GetTabletInfo();
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

        for (auto [replicaId, count] : replicaIdToCount) {
            if (count == std::ssize(tabletIds)) {
                replicaIds.push_back(replicaId);
            }
        }
    }

    YT_LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %llx)",
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

TString TClient::PickRandomCluster(
    const std::vector<TString>& clusterNames)
{
    YT_VERIFY(!clusterNames.empty());
    return clusterNames[RandomNumber<size_t>() % clusterNames.size()];
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const std::vector<std::pair<NTableClient::TLegacyKey, int>>& keys)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    THashSet<TTabletId> tabletIds;
    for (const auto& [key, _] : keys) {
        auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
        auto tabletId = tabletInfo->TabletId;
        if (tabletIds.insert(tabletId).second) {
            cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
        }
    }
    return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    for (const auto& tabletInfo : tableInfo->Tablets) {
        cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
    }
    return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
}

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const THashMap<TCellId, std::vector<TTabletId>>& cellIdToTabletIds)
{
    size_t cellCount = cellIdToTabletIds.size();
    size_t tabletCount = 0;
    for (const auto& [cellId, tabletIds] : cellIdToTabletIds) {
        tabletCount += tabletIds.size();
    }

    YT_LOG_DEBUG("Looking for in-sync replicas (Path: %v, CellCount: %v, TabletCount: %v)",
        tableInfo->Path,
        cellCount,
        tabletCount);

    const auto& channelFactory = Connection_->GetChannelFactory();
    const auto& cellDirectory = Connection_->GetCellDirectory();
    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncResults;
    for (const auto& [cellId, tabletIds] : cellIdToTabletIds) {
        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
        auto channel = CreateTabletReadChannel(
            channelFactory,
            cellDescriptor,
            options,
            Connection_->GetNetworks());

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        ToProto(req->mutable_tablet_ids(), tabletIds);
        for (int index = 0; index < std::ssize(tabletIds); ++index) {
            ToProto(req->add_cell_ids(), cellId);
        }

        asyncResults.push_back(req->Invoke());
    }

    return AllSucceeded(asyncResults).Apply(
        BIND([=, this_ = MakeStrong(this)] (const std::vector<TQueryServiceProxy::TRspGetTabletInfoPtr>& rsps) {
        THashMap<TTableReplicaId, int> replicaIdToCount;
        for (const auto& rsp : rsps) {
            for (const auto& protoTabletInfo : rsp->tablets()) {
                for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                    if (IsReplicaSync(protoReplicaInfo, protoTabletInfo)) {
                        ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                    }
                }
            }
        }

        TTableReplicaInfoPtrList inSyncReplicaInfos;
        for (const auto& replicaInfo : tableInfo->Replicas) {
            auto it = replicaIdToCount.find(replicaInfo->ReplicaId);
            if (it != replicaIdToCount.end() && it->second == static_cast<ssize_t>(tabletCount)) {
                YT_LOG_DEBUG("In-sync replica found (Path: %v, ReplicaId: %v, ClusterName: %v)",
                    tableInfo->Path,
                    replicaInfo->ReplicaId,
                    replicaInfo->ClusterName);
                inSyncReplicaInfos.push_back(replicaInfo);
            }
        }

        if (inSyncReplicaInfos.empty()) {
            THROW_ERROR_EXCEPTION("No in-sync replicas found for table %v",
                tableInfo->Path);
        }

        return inSyncReplicaInfos;
    }));
}

std::optional<TString> TClient::PickInSyncClusterAndPatchQuery(
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
        if (tableInfo->IsReplicated()) {
            someReplicated = true;
        } else {
            someNotReplicated = true;
        }
    }

    if (someReplicated && someNotReplicated) {
        THROW_ERROR_EXCEPTION("Query involves both replicated and non-replicated tables");
    }

    if (!someReplicated) {
        return std::nullopt;
    }

    std::vector<TFuture<TTableReplicaInfoPtrList>> asyncCandidates;
    for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
        asyncCandidates.push_back(PickInSyncReplicas(tableInfos[tableIndex], options));
    }

    auto candidates = WaitFor(AllSucceeded(asyncCandidates))
        .ValueOrThrow();

    THashMap<TString, int> clusterNameToCount;
    for (const auto& replicaInfos : candidates) {
        std::vector<TString> clusterNames;
        for (const auto& replicaInfo : replicaInfos) {
            clusterNames.push_back(replicaInfo->ClusterName);
        }
        std::sort(clusterNames.begin(), clusterNames.end());
        clusterNames.erase(std::unique(clusterNames.begin(), clusterNames.end()), clusterNames.end());
        for (const auto& clusterName : clusterNames) {
            ++clusterNameToCount[clusterName];
        }
    }

    std::vector<TString> inSyncClusterNames;
    for (const auto& [name, count] : clusterNameToCount) {
        if (count == std::ssize(paths)) {
            inSyncClusterNames.push_back(name);
        }
    }

    if (inSyncClusterNames.empty()) {
        THROW_ERROR_EXCEPTION("No single cluster contains in-sync replicas for all involved tables %v",
            paths);
    }

    auto inSyncClusterName = PickRandomCluster(inSyncClusterNames);
    YT_LOG_DEBUG("In-sync cluster selected (Paths: %v, ClusterName: %v)",
        paths,
        inSyncClusterName);

    auto patchTableDescriptor = [&] (NAst::TTableDescriptor* descriptor, const TTableReplicaInfoPtrList& replicaInfos) {
        for (const auto& replicaInfo : replicaInfos) {
            if (replicaInfo->ClusterName == inSyncClusterName) {
                descriptor->Path = replicaInfo->ReplicaPath;
                return;
            }
        }
        YT_ABORT();
    };

    patchTableDescriptor(&query->Table, candidates[0]);
    for (size_t index = 0; index < query->Joins.size(); ++index) {
        patchTableDescriptor(&query->Joins[index].Table, candidates[index + 1]);
    }
    return inSyncClusterName;
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

NApi::IClientPtr TClient::CreateReplicaClient(const TString& clusterName)
{
    auto replicaConnection = GetReplicaConnectionOrThrow(clusterName);
    return replicaConnection->CreateClient(Options_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
