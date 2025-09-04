#include "table_replica_synchronicity_cache.h"
#include "chaos_helpers.h"
#include "config.h"
#include "private.h"
#include "tablet_helpers.h"

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

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
    builder->AppendFormat(
        "{ReplicaId: %v, ReplicaPath: %v, Cluster: %v, MinReplicationTimestamp: %v, IsInSync: %v, IsDummy: %v}",
        replica.ReplicaInfo->ReplicaId,
        replica.ReplicaInfo->ReplicaPath,
        replica.ReplicaInfo->ClusterName,
        replica.MinReplicationTimestamp,
        replica.IsInSync,
        replica.IsDummy);
}

////////////////////////////////////////////////////////////////////////////////

struct TReplicaSynchronicitiesFetchResult
{
    TReplicaSynchronicityList ReplicaSynchronicities;
    bool IsDummy = false;
};

TFuture<TReplicaSynchronicitiesFetchResult> FetchChaosTableReplicaSynchronicities(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableMountInfo)
{
    return BIND(&GetSyncReplicationCard, connection, tableMountInfo->ReplicationCardId)
        .AsyncVia(GetCurrentInvoker())
        .Run()
        .ApplyUnique(BIND([
            tableMountInfo,
            bannedReplicaTracker=connection->GetBannedReplicaTrackerCache()->GetTracker(tableMountInfo->TableId)
        ] (TReplicationCardPtr&& replicationCard) {
            bannedReplicaTracker->SyncReplicas(replicationCard);

            auto result = TReplicaSynchronicitiesFetchResult();
            result.IsDummy = false;
            auto& replicas = result.ReplicaSynchronicities;

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

                auto minReplicationTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);

                replicas.push_back(TReplicaSynchronicity{
                    .ReplicaInfo = std::move(tableReplicaInfo),
                    .MinReplicationTimestamp = minReplicationTimestamp,
                    .IsInSync = IsReplicaReallySync(replica.Mode, replica.State, replica.History) &&
                        minReplicationTimestamp >= replica.History.back().Timestamp,
                });
            }

            return result;
        }));
}

TReplicaSynchronicitiesFetchResult BuildDummyTableReplicaSynchronicities(const TTableMountInfoPtr& tableMountInfo)
{
    auto result = TReplicaSynchronicitiesFetchResult();
    result.IsDummy = true;
    auto& replicaSynchronicities = result.ReplicaSynchronicities;

    replicaSynchronicities.reserve(tableMountInfo->Replicas.size());
    for (const auto& replicaInfo : tableMountInfo->Replicas) {
        replicaSynchronicities.push_back(TReplicaSynchronicity{
            .ReplicaInfo = replicaInfo,
            .MinReplicationTimestamp = MinTimestamp,
            .IsInSync = false,
            .IsDummy = true,
        });
    }

    return result;
}

TReplicaSynchronicitiesFetchResult BuildTabletReplicaSynchronicities(
    const TTableMountInfoPtr& tableMountInfo,
    bool allowDummy,
    const std::vector<TErrorOr<TQueryServiceProxy::TRspGetTabletInfoPtr>>& responses)
{
    THashMap<TTableReplicaId, int> replicaIdToSyncTabletCount;
    THashMap<TTableReplicaId, TTimestamp> replicationTimestamps;

    bool hasFails = false;
    for (const auto& responseOrError : responses) {
        if (!responseOrError.IsOK()) {
            if (allowDummy) {
                hasFails = true;
                continue;
            } else {
                responseOrError.ThrowOnError();
            }
        }

        for (const auto& protoTabletInfo : responseOrError.Value()->tablets()) {
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

    auto result = TReplicaSynchronicitiesFetchResult();
    result.IsDummy = hasFails;
    auto& replicaSynchronicities = result.ReplicaSynchronicities;

    replicaSynchronicities.reserve(tableMountInfo->Replicas.size());
    for (const auto& replicaInfo : tableMountInfo->Replicas) {
        if (auto timestampIt = replicationTimestamps.find(replicaInfo->ReplicaId);
            timestampIt != replicationTimestamps.end())
        {
            auto it = replicaIdToSyncTabletCount.find(replicaInfo->ReplicaId);
            auto isInSync = (it == replicaIdToSyncTabletCount.end())
                ? false
                : (it->second == std::ssize(tableMountInfo->Tablets));

            replicaSynchronicities.push_back(TReplicaSynchronicity{
                .ReplicaInfo = replicaInfo,
                .MinReplicationTimestamp = timestampIt->second,
                .IsInSync = isInSync,
                .IsDummy = hasFails,
            });
        } else {
            // All cells failed.
            replicaSynchronicities.push_back(TReplicaSynchronicity{
                .ReplicaInfo = replicaInfo,
                .MinReplicationTimestamp = MinTimestamp,
                .IsInSync = false,
                .IsDummy = true,
            });
        }
    }

    return result;
}

TFuture<TReplicaSynchronicitiesFetchResult> FetchReplicatedTableReplicaSynchronicities(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options,
    bool allowDummy)
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

    try {
        auto cellDescriptorsByPeer = GroupCellDescriptorsByPeer(connection, cellIds);

        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
        futures.reserve(cellDescriptorsByPeer.size());

        const auto& channelFactory = connection->GetChannelFactory();
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

        return AllSet(std::move(futures))
            .Apply(BIND(&BuildTabletReplicaSynchronicities, tableMountInfo, allowDummy));
    } catch (const TErrorException& ex) {
        auto filter = [] (TErrorCode code) {
            return code == NTabletClient::EErrorCode::CellHasNoAssignedPeers ||
                code == NHiveClient::EErrorCode::UnknownCell ||
                code == NNodeTrackerClient::EErrorCode::NoSuchNetwork;
        };

        if (!allowDummy || !ex.Error().FindMatching(filter)) {
            throw;
        }

        YT_LOG_DEBUG(ex, "Failed looking for in-sync replicas, building dummy response");
        return MakeFuture(BuildDummyTableReplicaSynchronicities(tableMountInfo));
    }
}

TFuture<TReplicaSynchronicitiesFetchResult> DoFetchReplicaSynchronicities(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options,
    bool allowDummy)
{
    if (tableMountInfo->ReplicationCardId) {
        return FetchChaosTableReplicaSynchronicities(connection, tableMountInfo);
    } else {
        return FetchReplicatedTableReplicaSynchronicities(connection, tableMountInfo, options, allowDummy);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TReplicaSynchronicityList> FetchReplicaSynchronicities(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options,
    bool allowDummy)
{
    return DoFetchReplicaSynchronicities(connection, tableMountInfo, options, allowDummy)
        .ApplyUnique(BIND([] (TReplicaSynchronicitiesFetchResult&& result) {
            return result.ReplicaSynchronicities;
        }));
}

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaSynchronicityCache
    : public ITableReplicaSynchronicityCache
{
public:
    TTableReplicaSynchronicityCache() = default;

    TFuture<TReplicaSynchronicityList> GetReplicaSynchronicities(
        const IConnectionPtr& connection,
        const NTabletClient::TTableMountInfoPtr& table,
        TInstant deadline,
        const TTabletReadOptions& options,
        bool allowDummy) override
    {
        auto guard = ReaderGuard(Lock_);
        if (auto it = TableToReplicaSynchronicities_.find(table->Path);
            it != TableToReplicaSynchronicities_.end() && it->second.CachedAt >= deadline)
        {
            return MakeFuture(it->second.ReplicaSynchronicities);
        } else {
            return DoFetchReplicaSynchronicities(connection, table, options, allowDummy)
                .Apply(BIND(&TTableReplicaSynchronicityCache::OnReplicaSynchronicitiesFetched, MakeStrong(this), table->Path));
        }
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<NYPath::TYPath, TTimestampedReplicaSynchronicities> TableToReplicaSynchronicities_;

    TReplicaSynchronicityList OnReplicaSynchronicitiesFetched(
        NYPath::TYPath path,
        TReplicaSynchronicitiesFetchResult replicasSynchronicitiesResult)
    {
        if (!replicasSynchronicitiesResult.IsDummy) {
            auto guard = WriterGuard(Lock_);

            TableToReplicaSynchronicities_[path] = {
                TInstant::Now(),
                replicasSynchronicitiesResult.ReplicaSynchronicities,
            };
        }

        return replicasSynchronicitiesResult.ReplicaSynchronicities;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITableReplicaSynchronicityCachePtr CreateTableReplicaSynchronicityCache()
{
    return New<TTableReplicaSynchronicityCache>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
