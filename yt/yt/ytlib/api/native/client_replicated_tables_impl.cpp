#include "client_impl.h"
#include "connection.h"
#include "tablet_helpers.h"
#include "config.h"

#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/table_client/row_buffer.h>

namespace NYT::NApi::NNative {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NHiveClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TClient::IsReplicaInSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo)
{
    return
        ETableReplicaMode(replicaInfo.mode()) == ETableReplicaMode::Sync &&
        replicaInfo.current_replication_row_index() >= tabletInfo.total_row_count();
}

bool TClient::IsReplicaInSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo,
    TTimestamp timestamp)
{
    return
        replicaInfo.last_replication_timestamp() >= timestamp ||
        IsReplicaInSync(replicaInfo, tabletInfo);
}

std::vector<TTableReplicaId> TClient::DoGetInSyncReplicas(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    ValidateSyncTimestamp(options.Timestamp);

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();
    tableInfo->ValidateReplicated();

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto idMapping = BuildColumnIdMapping(schema, nameTable);

    struct TGetInSyncReplicasTag
    { };
    auto rowBuffer = New<TRowBuffer>(TGetInSyncReplicasTag());

    auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
    auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

    std::vector<TTableReplicaId> replicaIds;

    if (keys.Empty()) {
        for (const auto& replica : tableInfo->Replicas) {
            replicaIds.push_back(replica->ReplicaId);
        }
    } else {
        THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
        THashSet<TTabletId> tabletIds;
        for (auto key : keys) {
            ValidateClientKey(key, schema, idMapping, nameTable);
            auto capturedKey = rowBuffer->CaptureAndPermuteRow(key, schema, idMapping, nullptr);

            if (evaluator) {
                evaluator->EvaluateKeys(capturedKey, rowBuffer);
            }
            auto tabletInfo = tableInfo->GetTabletForRow(capturedKey);
            if (tabletIds.insert(tabletInfo->TabletId).second) {
                ValidateTabletMountedOrFrozen(tabletInfo);
                cellToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
            }
        }

        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
        for (const auto& [cellId, perCellTabletIds] : cellToTabletIds) {
            auto channel = GetReadCellChannelOrThrow(cellId);

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

            auto req = proxy.GetTabletInfo();
            ToProto(req->mutable_tablet_ids(), perCellTabletIds);
            futures.push_back(req->Invoke());
        }
        auto responsesResult = WaitFor(Combine(futures));
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

        for (const auto& pair : replicaIdToCount) {
            auto replicaId = pair.first;
            auto count = pair.second;
            if (count == tabletIds.size()) {
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

TFuture<TTableReplicaInfoPtrList> TClient::PickInSyncReplicas(
    const TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const std::vector<std::pair<NTableClient::TKey, int>>& keys)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    THashSet<TTabletId> tabletIds;
    for (const auto& pair : keys) {
        auto key = pair.first;
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
    for (const auto& pair : cellIdToTabletIds) {
        tabletCount += pair.second.size();
    }

    YT_LOG_DEBUG("Looking for in-sync replicas (Path: %v, CellCount: %v, TabletCount: %v)",
        tableInfo->Path,
        cellCount,
        tabletCount);

    const auto& channelFactory = Connection_->GetChannelFactory();
    const auto& cellDirectory = Connection_->GetCellDirectory();
    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncResults;
    for (const auto& pair : cellIdToTabletIds) {
        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(pair.first);
        auto channel = CreateTabletReadChannel(
            channelFactory,
            cellDescriptor,
            options,
            Connection_->GetNetworks());

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        ToProto(req->mutable_tablet_ids(), pair.second);
        asyncResults.push_back(req->Invoke());
    }

    return Combine(asyncResults).Apply(
        BIND([=, this_ = MakeStrong(this)] (const std::vector<TQueryServiceProxy::TRspGetTabletInfoPtr>& rsps) {
        THashMap<TTableReplicaId, int> replicaIdToCount;
        for (const auto& rsp : rsps) {
            for (const auto& protoTabletInfo : rsp->tablets()) {
                for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                    if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo)) {
                        ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                    }
                }
            }
        }

        TTableReplicaInfoPtrList inSyncReplicaInfos;
        for (const auto& replicaInfo : tableInfo->Replicas) {
            auto it = replicaIdToCount.find(replicaInfo->ReplicaId);
            if (it != replicaIdToCount.end() && it->second == tabletCount) {
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

    auto tableInfos = WaitFor(Combine(asyncTableInfos))
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

    auto candidates = WaitFor(Combine(asyncCandidates))
        .ValueOrThrow();

    THashMap<TString, int> clusterNameToCount;
    for (const auto& replicaInfos : candidates) {
        SmallVector<TString, TypicalReplicaCount> clusterNames;
        for (const auto& replicaInfo : replicaInfos) {
            clusterNames.push_back(replicaInfo->ClusterName);
        }
        std::sort(clusterNames.begin(), clusterNames.end());
        clusterNames.erase(std::unique(clusterNames.begin(), clusterNames.end()), clusterNames.end());
        for (const auto& clusterName : clusterNames) {
            ++clusterNameToCount[clusterName];
        }
    }

    SmallVector<TString, TypicalReplicaCount> inSyncClusterNames;
    for (const auto& pair : clusterNameToCount) {
        if (pair.second == paths.size()) {
            inSyncClusterNames.push_back(pair.first);
        }
    }

    if (inSyncClusterNames.empty()) {
        THROW_ERROR_EXCEPTION("No single cluster contains in-sync replicas for all involved tables %v",
            paths);
    }

    // TODO(babenko): break ties in a smarter way
    const auto& inSyncClusterName = inSyncClusterNames[0];
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
