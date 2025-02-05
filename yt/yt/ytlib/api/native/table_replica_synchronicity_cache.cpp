#include "table_replica_synchronicity_cache.h"
#include "chaos_helpers.h"
#include "config.h"
#include "private.h"
#include "tablet_helpers.h"

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NHiveClient;
using namespace NLogging;
using namespace NTabletClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReplicaSynchronicity& replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ReplicaId: %v, ReplicaPath: %v, Cluster: %v, MinReplicationTimestamp: %v, IsInSync: %v}",
        replica.ReplicaInfo->ReplicaId,
        replica.ReplicaInfo->ReplicaPath,
        replica.ReplicaInfo->ClusterName,
        replica.MinReplicationTimestamp,
        replica.IsInSync);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TReplicaSynchronicityList> FetchChaosTableReplicaSynchronicities(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableMountInfo)
{
    return BIND(&GetSyncReplicationCard, connection, tableMountInfo)
        .AsyncVia(GetCurrentInvoker())
        .Run()
        .ApplyUnique(BIND([
            tableMountInfo,
            bannerReplicaTracker=connection->GetBannedReplicaTrackerCache()->GetTracker(tableMountInfo->TableId)
        ] (TReplicationCardPtr&& replicationCard) {
            bannerReplicaTracker->SyncReplicas(replicationCard);

            auto replicas = TReplicaSynchronicityList();

            for (const auto& [replicaId, replica] : replicationCard->Replicas) {
                if (tableMountInfo->IsSorted() && replica.ContentType != ETableReplicaContentType::Data) {
                    continue;
                } else if (!tableMountInfo->IsSorted() && replica.ContentType != ETableReplicaContentType::Queue) {
                    continue;
                }

                auto tableReplicaInfo = New<TTableReplicaInfo>();
                tableReplicaInfo->ReplicaId = replicaId;
                tableReplicaInfo->ClusterName = replica.ClusterName;
                tableReplicaInfo->ReplicaPath = replica.ReplicaPath;
                tableReplicaInfo->Mode = replica.Mode;

                replicas.push_back(TReplicaSynchronicity{
                    .ReplicaInfo = std::move(tableReplicaInfo),
                    .MinReplicationTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress),
                    .IsInSync = IsReplicaReallySync(replica.Mode, replica.State, replica.History),
                });
            }

            return replicas;
        }));
}

TFuture<TReplicaSynchronicityList> FetchReplicatedTableReplicaSynchronicities(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options)
{
    const auto& Logger = connection->GetLogger();

    YT_ASSERT(!tableMountInfo->ReplicationCardId);

    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    for (const auto& tabletInfo : tableMountInfo->Tablets) {
        cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
    }

    std::vector<TCellId> cellIds;
    cellIds.reserve(cellIdToTabletIds.size());
    for (const auto& [cellId, tabletIds] : cellIdToTabletIds) {
        cellIds.push_back(cellId);
    }

    YT_LOG_DEBUG("Looking for in-sync replicas (Path: %v, CellCount: %v)",
        tableMountInfo->Path,
        cellIdToTabletIds.size());

    const auto& channelFactory = connection->GetChannelFactory();
    auto cellDescriptorsByPeer = GroupCellDescriptorsByPeer(connection, cellIds);

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
    futures.reserve(cellDescriptorsByPeer.size());

    for (const auto& cellDescriptors : cellDescriptorsByPeer) {
        auto channel = CreateTabletReadChannel(
            channelFactory,
            *cellDescriptors[0],
            options,
            connection->GetNetworks());

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(connection->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        req->SetResponseHeavy(true);
        for (const auto& cellDescriptor : cellDescriptors) {
            auto cellId = cellDescriptor->CellId;
            const auto& tabletIds = cellIdToTabletIds[cellId];
            for (auto tabletId : tabletIds) {
                ToProto(req->add_tablet_ids(), tabletId);
                ToProto(req->add_cell_ids(), cellId);
            }
        }

        if (req->tablet_ids_size() == 0) {
            continue;
        }

        futures.push_back(req->Invoke());
    }

    return AllSucceeded(std::move(futures))
        .ApplyUnique(BIND([tableMountInfo] (std::vector<TQueryServiceProxy::TRspGetTabletInfoPtr>&& responses) {

            THashMap<TTableReplicaId, int> replicaIdToSyncTabletCount;
            THashMap<TTableReplicaId, TTimestamp> replicationTimestamps;

            for (const auto& response : responses) {
                for (const auto& protoTabletInfo : response->tablets()) {
                    for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                        auto replicaId = FromProto<TTableReplicaId>(protoReplicaInfo.replica_id());
                        if (IsReplicaSync(protoReplicaInfo, protoTabletInfo)) {
                            ++replicaIdToSyncTabletCount[replicaId];
                        }

                        auto timestamp = protoReplicaInfo.last_replication_timestamp();
                        auto [it, inserted] = replicationTimestamps.insert({replicaId, timestamp});
                        if (!inserted) {
                            it->second = std::min(timestamp, it->second);
                        }
                    }
                }
            }

            auto replicaSynchronicities = TReplicaSynchronicityList();
            replicaSynchronicities.reserve(tableMountInfo->Replicas.size());
            for (const auto& replicaInfo : tableMountInfo->Replicas) {
                auto it = replicaIdToSyncTabletCount.find(replicaInfo->ReplicaId);
                auto isInSync = (it == replicaIdToSyncTabletCount.end())
                    ? false
                    : (it->second == std::ssize(tableMountInfo->Tablets));

                replicaSynchronicities.push_back(TReplicaSynchronicity{
                    .ReplicaInfo = replicaInfo,
                    .MinReplicationTimestamp = GetOrCrash(replicationTimestamps, replicaInfo->ReplicaId),
                    .IsInSync = isInSync,
                });
            }

            return replicaSynchronicities;
        }));
}

TFuture<TReplicaSynchronicityList> FetchReplicaSynchronicities(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options)
{
    if (tableMountInfo->ReplicationCardId) {
        return FetchChaosTableReplicaSynchronicities(connection, tableMountInfo);
    } else {
        return FetchReplicatedTableReplicaSynchronicities(connection, tableMountInfo, options);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TReplicaSynchronicityList> TTableReplicaSynchronicityCache::GetReplicaSynchronicities(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& table,
    TInstant deadline,
    const TTabletReadOptions& options)
{
    auto guard = ReaderGuard(Lock_);
    if (auto it = TableToReplicaSynchronicities_.find(table->Path);
        it != TableToReplicaSynchronicities_.end() && it->second.CachedAt >= deadline)
    {
        return MakeFuture(it->second.ReplicaSynchronicities);
    } else {
        return FetchReplicaSynchronicities(connection, table, options)
            .Apply(BIND(&TTableReplicaSynchronicityCache::OnReplicaSynchronicitiesFetched, MakeStrong(this), table->Path));
    }
}

TReplicaSynchronicityList TTableReplicaSynchronicityCache::OnReplicaSynchronicitiesFetched(
    NYPath::TYPath path,
    TReplicaSynchronicityList replicas)
{
    auto guard = WriterGuard(Lock_);

    TableToReplicaSynchronicities_[path] = {
        TInstant::Now(),
        replicas,
    };

    return replicas;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
